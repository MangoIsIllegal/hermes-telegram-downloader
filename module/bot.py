"""Bot for media downloader"""

import asyncio
import os
import time
from datetime import datetime
from typing import Callable, List, Union

import pyrogram
from loguru import logger
from pyrogram import types
from pyrogram.handlers import CallbackQueryHandler, MessageHandler
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from ruamel import yaml

import utils
from module.app import (
    Application,
    ChatDownloadConfig,
    ForwardStatus,
    QueryHandler,
    QueryHandlerStr,
    TaskNode,
    TaskType,
    UploadStatus,
)
from module.download_stat import (
    add_failed_download,
    delete_task as _delete_download_progress,
    set_chat_title,
)
from module.filter import Filter
from module.task_store import save_task, complete_task, get_running_tasks, update_task_progress, get_pending_tasks, update_download_state
from module.get_chat_history_v2 import get_chat_history_v2
from module.language import Language, _t
from module.pyrogram_extension import (
    check_user_permission,
    get_utf16_length,
    is_flood_wait_active,
    parse_link,
    proc_cache_forward,
    report_bot_forward_status,
    report_bot_status,
    retry,
    set_meta_data,
    upload_telegram_chat_message,
)
from utils.format import replace_date_time, validate_title
from utils.meta_data import MetaData

# pylint: disable = C0301, R0902


def _cleanup_stopped_task(node):
    """When a task is stopped by user: clear download progress and record as failed.
    
    This ensures the task:
    1. Shows up in webui's Failed tab with '手动终止' reason
    2. Does NOT show lingering progress in the active/completed list
    3. Does NOT get recovered on next restart
    
    Only incomplete entries (down_byte < total_size) are recorded as failed.
    Already-completed entries are left untouched.
    """
    try:
        from module.download_stat import get_download_result
        download_result = get_download_result()
        removed = 0
        recorded = 0
        task_id_display = getattr(node, "task_id_display", "") or str(node.task_id)
        for chat_id, messages in list(download_result.items()):
            for msg_id, value in list(messages.items()):
                tid = str(value.get("task_id", ""))
                if tid != str(node.task_id) and tid != task_id_display:
                    continue
                total = value.get("total_size", 0)
                down = value.get("down_byte", 0)
                is_complete = total > 0 and down >= total
                if not is_complete:
                    # Build source link
                    source_link = ""
                    source_chat_id = value.get("source_chat_id", 0) or getattr(node, 'source_chat_id', 0)
                    source_message_id = value.get("source_message_id", 0) or getattr(node, 'source_message_id', 0)
                    if source_chat_id and source_message_id:
                        if str(source_chat_id).startswith("-100"):
                            link_id = str(source_chat_id)[4:]
                        else:
                            link_id = str(source_chat_id)
                        source_link = f"https://t.me/c/{link_id}/{source_message_id}"
                    # Record in failed list before deleting
                    add_failed_download(
                        chat_id=chat_id,
                        msg_id=msg_id,
                        task_id=task_id_display,
                        file_name=value.get("file_name", ""),
                        error_message="手动终止",
                        total_size=total,
                        source_link=source_link,
                            from_user_id=getattr(node, "from_user_id", "") or "",
                    )
                    recorded += 1
                    # Delete only incomplete entries from download result
                    _delete_download_progress(f"{chat_id}_{msg_id}")
                    removed += 1
        if removed > 0:
            logger.info(f"Cleaned up {removed} download entries for stopped task {task_id_display}")
        else:
            # No download_result entries found — download never started or already cleaned up.
            # Don't write empty failed records (msg_id=0, file_name="") — they're useless
            # and can't be retried via WebUI.
            logger.info(f"Stopped task {task_id_display} had no active download entries to clean")
    except Exception as e:
        logger.warning(f"Failed to cleanup stopped task {getattr(node, 'task_id_display', node.task_id)}: {e}")


def _record_pending_failures(node):
    """Record failed download_status entries that weren't caught by download_task.
    
    The criteria: entry exists in _download_result, belongs to this task,
    is incomplete (down_byte < total_size), AND has NEVER been touched by
    update_download_status (start_time == end_time means Pyrogram callback 
    never ran → never reached download_media's actual download loop).
    Entries that were touched by download (start_time < end_time) have already
    been handled by download_task's add_failed_download call, so we skip them
    to avoid duplicates.
    """
    try:
        from module.download_stat import get_download_result, get_failed_downloads
        download_result = get_download_result()
        # Collect existing failed composite keys to avoid duplicates
        existing_keys = {f"{f.get('chat_id', '')}_{f.get('msg_id', '')}" for f in get_failed_downloads()}
        recorded = 0
        task_id_display = getattr(node, "task_id_display", "") or str(node.task_id)
        for chat_id, messages in list(download_result.items()):
            for msg_id, value in list(messages.items()):
                tid = str(value.get("task_id", ""))
                if not (tid == str(node.task_id) or tid == task_id_display):
                    continue
                # Skip entries already recorded by download_task
                composite_key = f"{chat_id}_{msg_id}"
                if composite_key in existing_keys:
                    continue
                # Only catch entries that never started downloading
                # (down_byte == 0 or down_byte == total_size with same timestamps
                #  means Pyrogram progress callback never ran)
                if value.get("down_byte", 0) < value.get("total_size", 1) and value.get("total_size", 0) > 0:
                    # Build source link
                    source_link = ""
                    source_chat_id = value.get("source_chat_id", 0) or getattr(node, 'source_chat_id', 0)
                    source_message_id = value.get("source_message_id", 0) or getattr(node, 'source_message_id', 0)
                    if source_chat_id and source_message_id:
                        if str(source_chat_id).startswith("-100"):
                            link_id = str(source_chat_id)[4:]
                        else:
                            link_id = str(source_chat_id)
                        source_link = f"https://t.me/c/{link_id}/{source_message_id}"
                    add_failed_download(
                        chat_id=chat_id,
                        msg_id=msg_id,
                        task_id=task_id_display,
                        file_name=value.get("file_name", ""),
                        error_message="下载未开始（下载队列中未进入实际下载流程）",
                        total_size=value.get("total_size", 0),
                        source_link=source_link,
                            from_user_id=getattr(node, "from_user_id", "") or "",
                    )
                    recorded += 1
        if recorded > 0:
            logger.warning(f"Recorded {recorded} pending failures for task {task_id_display}")
    except Exception as e:
        logger.warning(f"Failed to record pending failures for task {getattr(node, 'task_id_display', node.task_id)}: {e}")


class DownloadBot:
    """Download bot"""

    def __init__(self):
        self.bot = None
        self.client = None
        self.add_download_task: Callable = None
        self.download_chat_task: Callable = None
        self.app = None
        self.listen_forward_chat: dict = {}
        self.config: dict = {}
        self._yaml = yaml.YAML()
        self.config_path = os.path.join(os.path.abspath("."), "bot.yaml")
        self.download_command: dict = {}
        self.filter = Filter()
        self.bot_info = None
        self.task_node: dict = {}
        self.is_running = True
        self.allowed_user_ids: List[Union[int, str]] = []
        self._listen_handler_ref = None

        meta = MetaData(datetime(2022, 8, 5, 14, 35, 12), 0, "", 0, 0, 0, "", 0)
        self.filter.set_meta_data(meta)

        self.download_filter: List[str] = []
        self.task_id: int = 0
        self.reply_task = None

        # In-memory set of task_ids that are in asyncio Queue but not yet picked
        # up by a worker. Replaces the "queued" download_state — no longer written
        # to bot_tasks.json. Cleared on restart (workers re-fetch via get_messages).
        self._in_queue: set = set()
        # Cache of message objects from direct_download so consumer can skip
        # get_messages. {task_id: pyrogram.Message}. Cleared on restart.
        self._cached_messages: dict = {}

    def gen_task_id(self) -> int:
        """Gen task id"""
        self.task_id += 1
        return self.task_id

    def add_task_node(self, node: TaskNode):
        """Add task node"""
        self.task_node[node.task_id] = node

    def remove_task_node(self, task_id: int):
        """Remove task node"""
        self.task_node.pop(task_id)

    def stop_task(self, task_id: str):
        """Stop task"""
        if task_id == "all":
            for value in self.task_node.values():
                value.stop_transmission()
        else:
            try:
                task = self.task_node.get(int(task_id))
                if task:
                    task.stop_transmission()
            except Exception:
                return

    async def update_reply_message(self):
        """Update reply message"""
        _bot_reconnect_cooldown = 300  # 5 min between reconnect attempts
        _bot_last_reconnect = 0.0
        while self.is_running:
            for key, value in self.task_node.copy().items():
                if value.is_running:
                    await report_bot_status(self.bot, value)

            # Check if bot connection has degraded — reconnect if too many
            # consecutive errors and cooldown has passed.
            from module.pyrogram_extension import _bot_conn_errors, _BOT_RECONNECT_THRESHOLD
            if _bot_conn_errors["count"] >= _BOT_RECONNECT_THRESHOLD:
                now = time.time()
                if now - _bot_last_reconnect >= _bot_reconnect_cooldown:
                    logger.warning(
                        f"Bot connection errors reached {_bot_conn_errors['count']} "
                        f"(threshold {_BOT_RECONNECT_THRESHOLD}) — triggering reconnect"
                    )
                    _bot_conn_errors["count"] = 0
                    _bot_last_reconnect = now
                    success = await self._reconnect_bot()
                    if success:
                        _bot_last_reconnect = now
                    else:
                        # Reconnect failed — set shorter cooldown to retry sooner
                        _bot_last_reconnect = now - _bot_reconnect_cooldown + 60

            for key, value in self.task_node.copy().items():
                if value.is_running and value.is_finish():
                    # Send final status update before completing
                    await report_bot_status(self.bot, value, immediate_reply=True)
                    # If stopped by user, move download progress to failed list and clear data
                    if value.is_stop_transmission:
                        _cleanup_stopped_task(value)
                    # Record any failed download_status entries that weren't caught by
                    # download_task (e.g. messages that were queued but never processed)
                    if not value.is_stop_transmission and value.failed_download_task > 0:
                        _record_pending_failures(value)
                    # Safety net: if media_downloader's immediate complete_task failed,
                    # ensure task is marked complete before removal
                    from module.task_store import complete_task
                    complete_task(value.task_id)
                    self.remove_task_node(key)
                    from module.task_store import get_pending_tasks
                    if get_pending_tasks():
                        _bot.app.loop.create_task(_consume_one_pending())
                elif value.is_running and not value.is_finish() and not value.is_stop_transmission:
                    # 僵尸任务检测：任务运行中但 total_download_task 长时间不递增
                    # 说明 worker 异常退出后 task_node 未被清理
                    idle_time = time.time() - value.last_reply_time
                    if idle_time > 3600:  # 1小时无进展
                        logger.warning(
                            f"Zombie task detected: {value.task_id_display} "
                            f"idle {int(idle_time)}s, total_task={value.total_task}, "
                            f"total_download_task={value.total_download_task}, force completing"
                        )
                        complete_task(value.task_id)
                        self.remove_task_node(key)
                        from module.task_store import get_pending_tasks
                        if get_pending_tasks():
                            _bot.app.loop.create_task(_consume_one_pending())
            await asyncio.sleep(3)

    async def recover_tasks(self):
        """Recover incomplete bot tasks from previous run.
        
        Two types of tasks need recovery:
        1. 'pending' - created but never started downloading → re-queue fresh
        2. 'downloading' - was actively downloading → re-execute (resume for forward, re-download for direct)
        """
        all_tasks = []
        try:
            await asyncio.sleep(5)  # Wait for bot to fully start
            running_tasks = get_running_tasks()
            if not running_tasks:
                return

            # All tasks go through pending queue — no more direct recovery.
            # This controls download concurrency and prevents FLOOD_WAIT storms
            # on restart (previously 100+ tasks would resume simultaneously).
            all_tasks = running_tasks

            if not all_tasks:
                return

            # Reset all tasks to pending state so _consume_one_pending picks them up
            from module.task_store import update_download_state
            for task_data in all_tasks:
                tid = task_data.get("task_id")
                if tid is not None and task_data.get("download_state") != "pending":
                    update_download_state(tid, "pending")

            logger.info(f"Found {len(all_tasks)} tasks, all queued as pending (will consume one by one)")

            # Start periodic pending consumer loop (60s interval)
            if not hasattr(self, "_pending_loop_started") or not self._pending_loop_started:
                self.app.loop.create_task(_pending_consumer_loop())
                self._pending_loop_started = True

            # Kick off first consumption immediately
            self.app.loop.create_task(_consume_one_pending())

        except Exception as e:
            logger.warning(f"Task recovery failed: {e}")

        # After recovery, update persistent counter to max of all task display IDs
        try:
            from module.task_store import _set_seq, _parse_seq_from_display, _lock
            max_seq = 0
            import time
            today = time.strftime('%m%d')
            for task_data in all_tasks:
                extra = task_data.get("extra_data", {}) or {}
                display = extra.get("task_id_display", "")
                if display:
                    seq = _parse_seq_from_display(display)
                    if seq > max_seq:
                        max_seq = seq
            if max_seq > 0:
                with _lock:
                    _set_seq(today, max_seq)
                logger.info(f"Updated persistent task counter to {max_seq} (after recovering {len(all_tasks)} tasks)")
        except Exception as e:
            logger.warning(f"Failed to update task counter after recovery: {e}")

    async def _recover_forward_task(self, task_data, node, offset_id):
        """Recover a forward task from last checkpoint."""
        from module.pyrogram_extension import report_bot_status
        forward_failed = False
        try:
            async for item in get_chat_history_v2(
                self.client, node.chat_id,
                limit=node.limit, max_id=node.end_offset_id,
                offset_id=offset_id, reverse=True,
            ):
                if not node.has_protected_content:
                    await forward_normal_content(self.client, node, item)
                else:
                    await self.add_download_task(item, node)
                update_task_progress(node.task_id, item.id)
                if node.is_stop_transmission:
                    break
        except Exception as e:
            forward_failed = True
            logger.warning(f"Forward recovery failed for task {node.task_id}: {e}")
        finally:
            await report_bot_status(self.bot, node, immediate_reply=True)
            # Only mark as stopped if forward actually failed (exception thrown).
            # For non-protected content, total_task stays 0 because forward_normal_content
            # doesn't use add_download_task — that's normal, not a failure.
            if forward_failed:
                node.stop_transmission()
            complete_task(node.task_id)

    async def _recover_direct_task(self, task_data, node):
        """Recover a direct download task (single message)."""
        from module.pyrogram_extension import report_bot_status
        extra_data = task_data.get("extra_data", {})
        message_id = extra_data.get("message_id", 0)
        source_chat_id = extra_data.get("source_chat_id", 0)
        source_message_id = extra_data.get("source_message_id", 0)

        if not message_id and not source_message_id:
            logger.warning(f"Direct recovery failed: no message_id for task {node.task_id}")
            complete_task(node.task_id)
            return

        success = False
        msg = None
        try:
            # Try source channel first (more reliable for forwarded messages)
            if source_chat_id and source_message_id:
                msg = await self.client.get_messages(source_chat_id, source_message_id)
                if msg and msg.media:
                    logger.info(f"Recovery: re-downloading from source {source_chat_id}/{source_message_id} for task {node.task_id}")
                else:
                    logger.info(f"Recovery: source message has no media, trying original chat")
                    msg = None

            # Fallback: try original chat_id + message_id
            if not msg and message_id:
                msg = await self.client.get_messages(node.chat_id, message_id)

            if msg and msg.media:
                # Check if file already exists in download history
                from module.download_stat import get_download_result
                _dlr = get_download_result()
                _exists = False
                if node.chat_id in _dlr:
                    for _mid, _val in _dlr[node.chat_id].items():
                        if str(_val.get("task_id")) == str(node.task_id) and _val.get("down_byte", 0) > 0 and _val.get("down_byte") == _val.get("total_size"):
                            _exists = True
                            break
                if not _exists:
                    logger.info(f"Recovery: re-downloading message {message_id} for task {node.task_id}")
                    await self.add_download_task(msg, node)
                    node.is_running = True
                    # Wait for download to finish (no timeout — large files may take hours)
                    while node.total_task == 0 or node.total_download_task < node.total_task:
                        await asyncio.sleep(3)
                        if node.is_stop_transmission:
                            break
                else:
                    logger.info(f"Recovery: file already exists for task {node.task_id}, marking complete")
                    node.total_task = 1
                    node.total_download_task = 1
                    node.success_download_task = 1
                    success = True
                # Check if download actually succeeded
                if node.success_download_task > 0:
                    success = True
                else:
                    logger.warning(f"Recovery: task {node.task_id} completed but 0 successful downloads (total={node.total_task}, success={node.success_download_task})")
            else:
                logger.warning(f"Direct recovery: message not found or has no media (msg={msg is not None}, media={getattr(msg, 'media', None) if msg else None})")
        except Exception as e:
            logger.warning(f"Direct recovery failed for task {node.task_id}: {e}")
        finally:
            await report_bot_status(self.bot, node, immediate_reply=True)
            if not success:
                node.stop_transmission()
            if success:
                complete_task(node.task_id)
            else:
                # Cleanup temp files from failed recovery
                _cleanup_task_temp_files(node.chat_id)
                logger.info(f"Recovery task {node.task_id} not completed, will retry next restart")

    def assign_config(self, _config: dict):
        """assign config from str.

        Parameters
        ----------
        _config: dict
            application config dict

        Returns
        -------
        bool
        """

        self.download_filter = _config.get("download_filter", self.download_filter)

        return True

    def update_config(self):
        """Update config from str."""
        self.config["download_filter"] = self.download_filter

        with open(self.config_path, "w", encoding="utf-8") as yaml_file:
            self._yaml.dump(self.config, yaml_file)

    def _register_listen_handler(self):
        """在 user client 上注册 NewMessage handler，实时检测监听频道的新消息"""
        if self._listen_handler_ref:
            return  # 已注册

        async def _on_new_message(client, message):
            chat_id = message.chat.id
            if not message.media:
                logger.debug(f"NewMessage handler: msg {message.id} from chat {chat_id} has no media, skipping")
                return

            # 检查是否是 listen_forward_chat 中的频道
            if chat_id in self.listen_forward_chat:
                node = self.listen_forward_chat[chat_id]
                if not node.is_running:
                    logger.debug(f"NewMessage handler: msg {message.id} from chat {chat_id} matches listen_forward but node not running")
                    return
                try:
                    if not node.has_protected_content:
                        await forward_normal_content(client, node, message)
                        from module.pyrogram_extension import report_bot_status
                        await report_bot_status(client, node, immediate_reply=True)
                    else:
                        await self.add_download_task(message, node)
                except Exception as e:
                    logger.exception(f"Listen handler error for chat {chat_id}: {e}")
                return

            # 检查是否是 config.yaml 中配置的 chat
            if self.app and chat_id in self.app.chat_download_config:
                chat_config = self.app.chat_download_config[chat_id]
                # 只处理比 last_read_message_id 新的消息
                if message.id <= chat_config.last_read_message_id:
                    logger.debug(f"NewMessage handler: msg {message.id} from config chat {chat_id} <= last_read {chat_config.last_read_message_id}, skipping")
                    return

                # 创建临时 TaskNode 下载这条消息
                try:
                    from module.app import TaskNode
                    node = TaskNode(
                        chat_id=chat_id,
                        from_user_id=0,
                        limit=1,
                        bot=self.bot,
                        download_filter=chat_config.download_filter,
                    )
                    node.task_id_display = f"config-{chat_id}-{message.id}"

                    # 不更新 last_read_message_id — 实时监控只负责下载，
                    # 扫描位置由 download_chat_task 管理。这样下载失败时
                    # 重启扫描能重新覆盖这些消息（_is_exist 检查防止重复下载）。

                    # 加入下载队列
                    await self.add_download_task(message, node)
                    logger.info(f"Config chat {chat_id}: new message {message.id} queued for download")
                except Exception as e:
                    logger.exception(f"Config chat handler error for chat {chat_id}: {e}")
                return

            # 消息不属于任何已配置的频道
            logger.debug(f"NewMessage handler: msg {message.id} from chat {chat_id} not in any configured channel (listen_forward={list(self.listen_forward_chat.keys())}, config_chats={list(self.app.chat_download_config.keys()) if self.app else []})")

        handler = MessageHandler(
            _on_new_message,
            filters=pyrogram.filters.media  # 只处理有媒体的消息
        )
        self.client.add_handler(handler)
        self._listen_handler_ref = handler
        logger.info("Listen handler registered on user client for real-time monitoring")

    async def start(
        self,
        app: Application,
        client: pyrogram.Client,
        add_download_task: Callable,
        download_chat_task: Callable,
    ):
        """Start bot"""
        self.bot = pyrogram.Client(
            app.application_name + "_bot",
            api_hash=app.api_hash,
            api_id=app.api_id,
            bot_token=app.bot_token,
            workdir=app.session_file_path,
            proxy=app.proxy,
        )

        # Command list
        commands = [
            types.BotCommand("help", _t("Help")),
            types.BotCommand(
                "get_info", _t("Get group and user info from message link")
            ),
            types.BotCommand(
                "download",
                _t(
                    "To download the video, use the method to directly enter /download to view"
                ),
            ),
            types.BotCommand(
                "forward",
                _t("Forward video, use the method to directly enter /forward to view"),
            ),
            types.BotCommand(
                "listen_forward",
                _t(
                    "Listen forward, use the method to directly enter /listen_forward to view"
                ),
            ),
            types.BotCommand("add_filter", _t("Add download filter, use the method to directly enter /add_filter to view")),
            types.BotCommand("add_ad", "Add advertisement filter (keyword to skip on forward)"),
            types.BotCommand("remove_ad", "Remove advertisement filter"),
            types.BotCommand("add_replace_ad", "Add replace advertisement (message_link keyword)"),
            types.BotCommand("remove_replace_ad", "Remove replace advertisement (message_link keyword)"),
            types.BotCommand("set_ad", "Set group advertisement (message_link advertisement)"),
            types.BotCommand("set_language", _t("Set language")),
            types.BotCommand("stop", _t("Stop bot download or forward")),
        ]

        self.app = app
        self.client = client
        self.add_download_task = add_download_task
        self.download_chat_task = download_chat_task

        # load config
        if os.path.exists(self.config_path):
            with open(self.config_path, encoding="utf-8") as f:
                config = self._yaml.load(f.read())
                if config:
                    self.config = config
                    self.assign_config(self.config)

        await self.bot.start()

        self.bot_info = await self.bot.get_me()

        for allowed_user_id in self.app.allowed_user_ids:
            try:
                chat = await self.client.get_chat(allowed_user_id)
                self.allowed_user_ids.append(chat.id)
            except Exception as e:
                logger.warning(f"set allowed_user_ids error: {e}")

        admin = await self.client.get_me()
        self.allowed_user_ids.append(admin.id)

        await self.bot.set_bot_commands(commands)

        self._register_bot_handlers()

        try:
            await send_help_str(self.bot, admin.id)
        except Exception as e:
            logger.warning(f"Failed to send help message: {e}")

        self.reply_task = _bot.app.loop.create_task(_bot.update_reply_message())

        # Recover incomplete tasks from previous run
        _bot.app.loop.create_task(_bot.recover_tasks())

        self.bot.add_handler(
            MessageHandler(
                forward_to_comments,
                filters=pyrogram.filters.command(["forward_to_comments"])
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )

        # 自动注册频道监听 handler（支持 config.yaml 中的 chat）
        if self.app.chat_download_config:
            self._register_listen_handler()
            logger.info("Auto-registered listen handler for config chats")

    def _register_bot_handlers(self):
        """Register all bot MessageHandlers and CallbackQueryHandler.

        Called from start() and _reconnect_bot() — stop() clears handlers,
        so they must be re-registered after every reconnect.
        """
        self.bot.add_handler(
            MessageHandler(
                download_from_bot,
                filters=pyrogram.filters.command(["download"])
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )
        self.bot.add_handler(
            MessageHandler(
                forward_messages,
                filters=pyrogram.filters.command(["forward"])
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )
        self.bot.add_handler(
            MessageHandler(
                download_forward_media,
                filters=pyrogram.filters.media
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )
        self.bot.add_handler(
            MessageHandler(
                download_from_link,
                filters=pyrogram.filters.regex(r"^https://t.me.*")
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )
        self.bot.add_handler(
            MessageHandler(
                set_listen_forward_msg,
                filters=pyrogram.filters.command(["listen_forward"])
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )
        self.bot.add_handler(
            MessageHandler(
                help_command,
                filters=pyrogram.filters.command(["help"])
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )
        self.bot.add_handler(
            MessageHandler(
                get_info,
                filters=pyrogram.filters.command(["get_info"])
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )
        self.bot.add_handler(
            MessageHandler(
                help_command,
                filters=pyrogram.filters.command(["start"])
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )
        self.bot.add_handler(
            MessageHandler(
                set_language,
                filters=pyrogram.filters.command(["set_language"])
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )
        self.bot.add_handler(
            MessageHandler(
                add_filter,
                filters=pyrogram.filters.command(["add_filter"])
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )

        self.bot.add_handler(
            MessageHandler(
                add_filter_advertisement_filter,
                filters=pyrogram.filters.command(["add_ad"])
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )
        self.bot.add_handler(
            MessageHandler(
                remove_filter_advertisement_filter,
                filters=pyrogram.filters.command(["remove_ad"])
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )
        self.bot.add_handler(
            MessageHandler(
                add_replace_advertisement_filter,
                filters=pyrogram.filters.command(["add_replace_ad"])
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )
        self.bot.add_handler(
            MessageHandler(
                remove_replace_advertisement_filter,
                filters=pyrogram.filters.command(["remove_replace_ad"])
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )
        self.bot.add_handler(
            MessageHandler(
                set_add_advertisement,
                filters=pyrogram.filters.command(["set_ad"])
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )

        self.bot.add_handler(
            MessageHandler(
                stop,
                filters=pyrogram.filters.command(["stop"])
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )

        self.bot.add_handler(
            CallbackQueryHandler(
                on_query_handler, filters=pyrogram.filters.user(self.allowed_user_ids)
            )
        )

        # forward_to_comments handler (registered after reply_task in original start)
        self.bot.add_handler(
            MessageHandler(
                forward_to_comments,
                filters=pyrogram.filters.command(["forward_to_comments"])
                & pyrogram.filters.user(self.allowed_user_ids),
            )
        )

    async def _reconnect_bot(self):
        """Stop and restart the bot client to recover from persistent connection loss.

        Uses the existing session file — no re-authentication needed.
        Returns True if reconnect succeeded, False otherwise.

        所有操作加 asyncio.wait_for 超时，防止 stop()/start() 在半死 TCP 上 hang
        导致 update_reply_message 循环永久阻塞、所有进度报告停止。
        """
        try:
            logger.warning("Attempting bot reconnect: stop()...")
            try:
                await asyncio.wait_for(self.bot.stop(), timeout=30)
            except asyncio.TimeoutError:
                logger.error("bot.stop() timed out after 30s, force disconnect")
                try:
                    await self.bot.disconnect()
                except Exception:
                    pass
                self.bot.is_connected = False
            except Exception as e:
                logger.warning(f"bot.stop() during reconnect failed (continuing): {e}")
                self.bot.is_connected = False
        except Exception as e:
            logger.warning(f"bot.stop() outer error (continuing): {e}")
            self.bot.is_connected = False

        try:
            logger.warning("Attempting bot reconnect: start()...")
            try:
                await asyncio.wait_for(self.bot.start(), timeout=30)
            except asyncio.TimeoutError:
                logger.error("bot.start() timed out after 30s, reconnect failed")
                return False
            self._register_bot_handlers()
            logger.success("Bot reconnected successfully — handlers re-registered")
            return True
        except Exception as e:
            logger.error(f"bot.start() during reconnect failed: {e}")
            return False


# pending consumer 连续超时计数 — 超过3次标失败，避免无限循环
_pending_timeout_counts: dict = {}

_bot = DownloadBot()


def _cleanup_task_temp_files(chat_id: int):
    """Remove temp files for a specific chat from temp directory."""
    temp_dir = os.path.join(os.path.abspath("."), "temp")
    chat_dir = os.path.join(temp_dir, str(chat_id))
    if not os.path.isdir(chat_dir):
        return
    removed = 0
    for f in os.listdir(chat_dir):
        if f.endswith('.temp'):
            try:
                os.remove(os.path.join(chat_dir, f))
                removed += 1
            except OSError:
                pass
    # Remove directory if empty
    try:
        if not os.listdir(chat_dir):
            os.rmdir(chat_dir)
    except OSError:
        pass
    if removed:
        logger.info(f"Cleanup: removed {removed} stale temp files for chat {chat_id}")


async def start_download_bot(
    app: Application,
    client: pyrogram.Client,
    add_download_task: Callable,
    download_chat_task: Callable,
):
    """Start download bot"""
    await _bot.start(app, client, add_download_task, download_chat_task)


async def stop_download_bot():
    """Stop download bot"""
    _bot.update_config()
    _bot.is_running = False
    if _bot.reply_task:
        _bot.reply_task.cancel()
    _bot.stop_task("all")
    if _bot.bot:
        await _bot.bot.stop()


async def send_help_str(client: pyrogram.Client, chat_id):
    """
    Sends a help string to the specified chat ID using the provided client.

    Parameters:
        client (pyrogram.Client): The Pyrogram client used to send the message.
        chat_id: The ID of the chat to which the message will be sent.

    Returns:
        str: The help string that was sent.

    Note:
        The help string includes information about the Telegram Media Downloader bot,
        its version, and the available commands.
    """

    update_keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Github",
                    url="https://github.com/tangyoha/telegram_media_downloader/releases",
                ),
                InlineKeyboardButton(
                    "Join us", url="https://t.me/TeegramMediaDownload"
                ),
            ]
        ]
    )
    latest_release_str = ""
    # try:
    #     latest_release = get_latest_release(_bot.app.proxy)

    #     latest_release_str = (
    #         f"{_t('New Version')}: [{latest_release['name']}]({latest_release['html_url']})\an"
    #         if latest_release
    #         else ""
    #     )
    # except Exception:
    #     latest_release_str = ""

    msg = (
            f"`\n🤖 {_t('Telegram Media Downloader')}\n"
            f"🌐 {_t('Version')}: {utils.__version__}`\n"
            f"{latest_release_str}\n"
            f"{_t('Available commands:')}\n"
            f"/help - {_t('显示帮助信息')}\n"
            f"/start - {_t('显示帮助信息')}\n"
            f"/get_info <link> - {_t('从消息链接获取群组/频道信息')}\n"
            f"/download <link> <start_id> <end_id> [filter] - {_t('批量下载消息')}\n"
            f"/forward <src_link> <dst_link> <start_id> <end_id> [filter] - {_t('转发消息到目标群组')}\n"
            f"/forward_to_comments <src_link> <dst_link> <start_id> <end_id> - {_t('转发媒体到评论区')}\n"
            f"/listen_forward <src_link> <dst_link> [filter] - {_t('实时监听并自动转发新消息')}\n"
            f"/stop - {_t('停止下载/转发/监听转发（交互式按钮选择）')}\n"
            f"/set_language en|ru|zh|ua - {_t('设置语言')}\n"
            f"/add_filter <filter> - {_t('设置下载过滤器')}\n\n"
            f"{_t('**快捷操作（无需命令）：**')}\n"
            f"• {_t('发送 Telegram 消息链接')} - {_t('下载单条消息')}\n"
            f"• {_t('转发一条媒体消息给机器人')} - {_t('下载或重新上传该媒体')}\n\n"
            f"{_t('**说明**')}\n"
            f"• `[option]` {_t('表示可选参数，非必填')}\n"
            f"• {_t('start_id=1 表示从头开始，end_id=0 表示直到末尾')}\n"
        )

    await client.send_message(chat_id, msg, reply_markup=update_keyboard)


async def help_command(client: pyrogram.Client, message: pyrogram.types.Message):
    """
    Sends a message with the available commands and their usage.

    Parameters:
        client (pyrogram.Client): The client instance.
        message (pyrogram.types.Message): The message object.

    Returns:
        None
    """

    await send_help_str(client, message.chat.id)


async def set_language(client: pyrogram.Client, message: pyrogram.types.Message):
    """
    Set the language of the bot.

    Parameters:
        client (pyrogram.Client): The pyrogram client.
        message (pyrogram.types.Message): The message containing the command.

    Returns:
        None
    """

    if len(message.text.split()) != 2:
        await client.send_message(
            message.from_user.id,
            _t("Invalid command format. Please use /set_language en/ru/zh/ua"),
        )
        return

    language = message.text.split()[1]

    try:
        language = Language[language.upper()]
        _bot.app.set_language(language)
        await client.send_message(
            message.from_user.id, f"{_t('Language set to')} {language.name}"
        )
    except KeyError:
        await client.send_message(
            message.from_user.id,
            _t("Invalid command format. Please use /set_language en/ru/zh/ua"),
        )


async def get_info(client: pyrogram.Client, message: pyrogram.types.Message):
    """
    Async function that retrieves information from a group message link.
    """

    msg = _t("Invalid command format. Please use /get_info group_message_link")

    args = message.text.split()
    if len(args) != 2:
        await client.send_message(
            message.from_user.id,
            msg,
        )
        return

    chat_id, message_id, _ = await parse_link(_bot.client, args[1])

    entity = None
    if chat_id:
        entity = await _bot.client.get_chat(chat_id)

    if entity:
        if message_id:
            _message = await retry(_bot.client.get_messages, args=(chat_id, message_id))
            if _message:
                meta_data = MetaData()
                set_meta_data(meta_data, _message)
                msg = (
                    f"`\n"
                    f"{_t('Group/Channel')}\n"
                    f"├─ {_t('id')}: {entity.id}\n"
                    f"├─ {_t('first name')}: {entity.first_name}\n"
                    f"├─ {_t('last name')}: {entity.last_name}\n"
                    f"└─ {_t('name')}: {entity.username}\n"
                    f"{_t('Message')}\n"
                )

                for key, value in meta_data.data().items():
                    if key == "send_name":
                        msg += f"└─ {key}: {value or None}\n"
                    else:
                        msg += f"├─ {key}: {value or None}\n"

                msg += "`"
    await client.send_message(
        message.from_user.id,
        msg,
    )


async def add_filter(client: pyrogram.Client, message: pyrogram.types.Message):
    """
    Set the download filter of the bot.

    Parameters:
        client (pyrogram.Client): The pyrogram client.
        message (pyrogram.types.Message): The message containing the command.

    Returns:
        None
    """

    args = message.text.split(maxsplit=1)
    if len(args) != 2:
        await client.send_message(
            message.from_user.id,
            _t("Invalid command format. Please use /add_filter your filter"),
        )
        return

    filter_str = replace_date_time(args[1])
    res, err = _bot.filter.check_filter(filter_str)
    if res:
        _bot.app.down = args[1]
        await client.send_message(
            message.from_user.id, f"{_t('Add download filter')} : {args[1]}"
        )
    else:
        await client.send_message(
            message.from_user.id, f"{err}\n{_t('Check error, please add again!')}"
        )
    return


async def add_filter_advertisement_filter(
    client: pyrogram.Client, message: pyrogram.types.Message
):
    """
    Set the download filter of the bot.

    Parameters:
        client (pyrogram.Client): The pyrogram client.
        message (pyrogram.types.Message): The message containing the command.

    Returns:
        None
    """

    args = message.text.split(maxsplit=1)
    if len(args) != 2:
        await client.send_message(
            message.from_user.id,
            _t("Invalid command format. Please use /add_ad filter"),
        )
        return

    filter_str = args[1]

    if filter_str in _bot.app.filter_advertisement_list:
        await client.send_message(message.from_user.id, f"Filter already exists: {filter_str}")
        return

    _bot.app.filter_advertisement_list.append(filter_str)
    await client.send_message(message.from_user.id, f"Add filter: {filter_str}")
    _bot.app.update_config(True)


async def remove_filter_advertisement_filter(
    client: pyrogram.Client, message: pyrogram.types.Message
):
    """
    Add or remove advertisement filter
    """

    args = message.text.split(maxsplit=1)
    if len(args) != 2:
        await client.send_message(
            message.from_user.id,
            _t("Invalid command format. Please use /remove_ad filter"),
        )
        return

    filter_str = args[1]
    if filter_str in _bot.app.filter_advertisement_list:
        _bot.app.filter_advertisement_list.remove(filter_str)
        await client.send_message(
            message.from_user.id, f"{_t('Remove filter')} : {args[1]}"
        )

        _bot.app.update_config(True)
    else:
        await client.send_message(
            message.from_user.id, f"{_t('Filter')} : {args[1]} {_t('not exist')}"
        )


async def set_add_advertisement(
    client: pyrogram.Client, message: pyrogram.types.Message
):
    """
    Add or remove advertisement filter
    """

    args = message.text.split(maxsplit=2)
    if len(args) < 2:
        await client.send_message(
            message.from_user.id,
            _t("Invalid command format. Please use /set_ad mesage_link advertisement"),
        )
        return

    mesage_link = args[1]
    advertisement_str = None if len(args) < 3 else args[2]

    try:
        chat_id, _, _ = await parse_link(_bot.client, mesage_link)
        if advertisement_str:
            _bot.app.group_add_advertisement[chat_id] = advertisement_str
            _bot.app.update_config(True)
            await client.send_message(
                message.from_user.id, f"Set advertisement : {advertisement_str}"
            )
        else:
            if chat_id in _bot.app.group_add_advertisement:
                del _bot.app.group_add_advertisement[chat_id]
                _bot.app.update_config(True)
                await client.send_message(
                    message.from_user.id, f"Removed advertisement for chat {chat_id}"
                )
            else:
                await client.send_message(
                    message.from_user.id, f"No advertisement set for chat {chat_id}"
                )
    except Exception as e:
        await client.send_message(
            message.from_user.id, f"{_t('Parse link error')}: {e}"
        )
        return


class MessageProcessor:
    """Helper class for processing message captions and entities."""

    def __init__(self, raw_message, filter_str):
        self.raw_message = raw_message
        self.raw_caption = raw_message.caption
        self.filter_str = filter_str
        self.raw_filter_str = pyrogram.parser.utils.add_surrogates(filter_str)
        self.raw_caption_str = pyrogram.parser.utils.add_surrogates(raw_message.caption)
        self.idx = self.raw_caption_str.find(self.raw_filter_str)
        self.start_offset = self.idx
        self.end_offset = self.idx + get_utf16_length(filter_str)
        self.filtered_entities = []

    # pylint: disable = R0916
    def process_entities(self):
        """Process and filter message entities."""
        for entity in self.raw_message.caption_entities:
            cur_start_offset = entity.offset
            cur_end_offset = entity.offset + entity.length

            # Check if entity should be included
            if (
                (
                    cur_start_offset >= self.start_offset
                    and cur_end_offset <= self.end_offset
                )
                or (
                    cur_start_offset < self.start_offset
                    and cur_end_offset > self.start_offset
                )
                or (
                    cur_start_offset < self.end_offset
                    and cur_end_offset > self.end_offset
                )
            ):
                self.filtered_entities.append(entity)

        self.filtered_entities.sort(key=lambda x: x.offset)

    def get_total_span(self):
        """Calculate the total span for text extraction."""
        if self.filtered_entities:
            first_entity = self.filtered_entities[0]
            last_entity = self.filtered_entities[-1]
            return (
                min(self.start_offset, first_entity.offset),
                max(self.end_offset, last_entity.offset + last_entity.length),
            )
        return (self.start_offset, self.end_offset)

    def extract_text(self, total_span):
        """Extract and process text with adjusted entity offsets."""
        text = self.raw_caption[total_span[0] : total_span[1]]
        for entity in self.filtered_entities:
            entity.offset -= total_span[0]
        return pyrogram.parser.Parser.unparse(text, self.filtered_entities, True)


async def proc_replace_advertisement(mesage_link: str, filter_str: str):
    """
    Process and replace advertisement content in a message.

    This function takes a message link and a filter string, retrieves the message,
    and processes its caption by handling entities and filtering advertisement content.
    It preserves the formatting and entities while replacing the specified filter text.

    Args:
        mesage_link (str): The link to the Telegram message
        filter_str (str): The string to filter/replace in the message caption

    Returns:
        str: The processed caption with preserved formatting and entities

    Raises:
        Exception: If there are issues parsing the message link or accessing the message
    """
    chat_id, message_id, _ = await parse_link(_bot.client, mesage_link)
    raw_message = await retry(_bot.client.get_messages, args=(chat_id, message_id))

    processor = MessageProcessor(raw_message, filter_str)
    processor.process_entities()
    total_span = processor.get_total_span()
    return processor.extract_text(total_span)


async def add_replace_advertisement_filter(
    client: pyrogram.Client, message: pyrogram.types.Message
):
    """
    Set the download filter of the bot.

    Parameters:
        client (pyrogram.Client): The pyrogram client.
        message (pyrogram.types.Message): The message containing the command.

    Returns:
        None
    """

    args = message.text.split(maxsplit=2)
    if len(args) != 3:
        await client.send_message(
            message.from_user.id,
            _t("Invalid command format. Please use /add_replace_ad your filter"),
        )
        return

    mesage_link = args[1]
    filter_str = args[2]

    try:
        filter_str = await proc_replace_advertisement(mesage_link, filter_str)
        _bot.app.replace_advertisement_list.append(filter_str)
        _bot.app.update_config(True)
        await client.send_message(
            message.from_user.id, f"{_t('Add filter')} : {filter_str}"
        )
    except Exception as e:
        await client.send_message(
            message.from_user.id, f"{_t('Add filter')} : {filter_str}\n{e}"
        )
        return


async def remove_replace_advertisement_filter(
    client: pyrogram.Client, message: pyrogram.types.Message
):
    """
    Set the download filter of the bot.

    Parameters:
        client (pyrogram.Client): The pyrogram client.
        message (pyrogram.types.Message): The message containing the command.

    Returns:
        None
    """

    args = message.text.split(maxsplit=2)
    if len(args) != 3:
        await client.send_message(
            message.from_user.id,
            _t(
                "Invalid command format. Please use /remove_replace_ad mesage_link advertisement_filter"
            ),
        )
        return

    mesage_link = args[1]
    filter_str = args[2]

    try:
        filter_str = await proc_replace_advertisement(mesage_link, filter_str)

        if filter_str in _bot.app.replace_advertisement_list:
            _bot.app.replace_advertisement_list.remove(filter_str)
            await client.send_message(
                message.from_user.id, f"{_t('Remove filter')} : {filter_str}"
            )
        else:
            await client.send_message(
                message.from_user.id,
                f"{_t('Filter not found')}: {filter_str}",
            )
        _bot.app.update_config(True)
    except Exception as e:
        await client.send_message(
            message.from_user.id, f"{_t('Add filter')} : {filter_str}\n{e}"
        )
        return


async def direct_download(
    download_bot: DownloadBot,
    chat_id: Union[str, int],
    message: pyrogram.types.Message,
    download_message: pyrogram.types.Message,
    client: pyrogram.Client = None,
    source_chat_id: int = 0,
    source_message_id: int = 0,
    source_chat_title: str = "",
):
    """Direct Download

    Args:
        source_chat_id: Original source channel ID (for forwarded messages recovery)
        source_message_id: Original source message ID (for forwarded messages recovery)
        source_chat_title: Source channel title (for web display)
    """

    replay_message = "Direct download..."
    last_reply_message = await download_bot.bot.send_message(
        message.from_user.id, replay_message, reply_to_message_id=message.id
    )

    node = TaskNode(
        chat_id=chat_id,
        from_user_id=message.from_user.id,
        reply_message_id=last_reply_message.id,
        replay_message=replay_message,
        limit=1,
        bot=download_bot.bot,
        task_id=_bot.gen_task_id(),
    )

    # node.client = client
    # ↑ 注释掉以统一使用 HookClient（设置了 max_concurrent_transmissions=25 的 client），
    #   让 worker 走 media_downloader.py:466 的 download_task(client, ...) 分支。
    #   否则 bot handler 的 client 默认 max_concurrent_transmissions=1，导致单线程下载。
    #   如需恢复旧行为，取消注释即可。
    node.source_chat_title = source_chat_title
    node.source_chat_id = source_chat_id
    node.source_message_id = source_message_id

    _bot.add_task_node(node)

    extra_data = {
        "message_id": download_message.id,
        "task_id_display": node.task_id_display,
    }
    if source_chat_id and source_message_id:
        extra_data["source_chat_id"] = source_chat_id
        extra_data["source_message_id"] = source_message_id
    if source_chat_title:
        extra_data["source_chat_title"] = source_chat_title

    save_task(
        task_id=node.task_id,
        chat_id=chat_id,
        url="",
        start_offset_id=0,
        end_offset_id=0,
        limit=1,
        download_filter=None,
        from_user_id=message.from_user.id,
        task_type="direct",
        extra_data=extra_data,
    )

    # Cache the message so consumer can skip get_messages.
    # Do NOT queue directly — consumer controls when to feed workers.
    _bot._cached_messages[node.task_id] = download_message
    node.is_running = True

    # Do NOT trigger _consume_one_pending directly — let the periodic
    # _pending_consumer_loop (every 2s) pick it up. This ensures the
    # concurrency guard is always respected.


async def download_forward_media(
    client: pyrogram.Client, message: pyrogram.types.Message
):
    """
    Downloads the media from a forwarded message.

    Parameters:
        client (pyrogram.Client): The client instance.
        message (pyrogram.types.Message): The message object.

    Returns:
        None
    """

    if message.media and getattr(message, message.media.value):
        # If forwarded from a channel/group, download from source
        if message.forward_from_chat:
            source_chat_id = message.forward_from_chat.id
            source_message_id = message.forward_from_message_id or 0
            source_chat_title = message.forward_from_chat.title or ""

            download_message = None
            if source_message_id:
                source_msg = await retry(
                    _bot.client.get_messages,
                    args=(source_chat_id, source_message_id),
                )
                if source_msg and source_msg.media:
                    await direct_download(
                        _bot, source_chat_id, message, source_msg, client,
                        source_chat_id=source_chat_id,
                        source_message_id=source_message_id,
                        source_chat_title=source_chat_title,
                    )
                    return
                # Source message deleted or no media — try link-based fetch
                # The forwarded message's file reference may be stale. Construct
                # a link and use parse_link → get_messages to get a fresh copy.
                logger.info(
                    f"Forward: source msg {source_message_id} has no media, "
                    f"trying link-based fetch"
                )
                try:
                    from module.pyrogram_extension import parse_link
                    # Build source link: private channels use /c/, public use /username/
                    fwd_chat = message.forward_from_chat
                    if fwd_chat.username:
                        link_str = f"https://t.me/{fwd_chat.username}/{source_message_id}"
                    else:
                        link_id = str(source_chat_id)
                        if link_id.startswith("-100"):
                            link_id = link_id[4:]
                        link_str = f"https://t.me/c/{link_id}/{source_message_id}"
                    link_chat_id, link_msg_id, _ = await parse_link(_bot.client, link_str)
                    if link_chat_id and link_msg_id:
                        download_message = await retry(
                            _bot.client.get_messages,
                            args=(link_chat_id, link_msg_id),
                        )
                        if download_message and download_message.media:
                            logger.info(
                                f"Forward: link-based fetch succeeded for "
                                f"{link_chat_id}/{link_msg_id}"
                            )
                            await direct_download(
                                _bot, link_chat_id, message, download_message, client,
                                source_chat_id=source_chat_id,
                                source_message_id=source_message_id,
                                source_chat_title=source_chat_title,
                            )
                            return
                except Exception as e:
                    logger.warning(f"Forward: link-based fetch failed: {e}")

            # All fallbacks failed — fall through to direct download with the
            # original forwarded message (last resort)
            await direct_download(_bot, message.from_user.id, message, message, client)
            return

        # Direct upload or forward from user (no source channel info)
        await direct_download(_bot, message.from_user.id, message, message, client)
        return

    await client.send_message(
        message.from_user.id,
        f"1. {_t('Direct download, directly forward the message to your robot')}\n\n",
        parse_mode=pyrogram.enums.ParseMode.HTML,
    )


async def download_from_link(client: pyrogram.Client, message: pyrogram.types.Message):
    """
    Downloads a single message from a Telegram link.

    Parameters:
        client (pyrogram.Client): The pyrogram client.
        message (pyrogram.types.Message): The message containing the Telegram link.

    Returns:
        None
    """

    if not message.text or not message.text.startswith("https://t.me"):
        return

    msg = (
        f"1. {_t('Directly download a single message')}\n"
        "<i>https://t.me/12000000/1</i>\n\n"
    )

    text = message.text.split()
    if len(text) != 1:
        await client.send_message(
            message.from_user.id, msg, parse_mode=pyrogram.enums.ParseMode.HTML
        )

    chat_id, message_id, _ = await parse_link(_bot.client, text[0])

    entity = None
    if chat_id:
        entity = await _bot.client.get_chat(chat_id)
    if entity:
        title = entity.title or entity.first_name or str(entity.id)
        set_chat_title(entity.id, title)
        if message_id:
            download_message = await retry(
                _bot.client.get_messages, args=(chat_id, message_id)
            )
            if download_message:
                await direct_download(_bot, entity.id, message, download_message)
            else:
                await client.send_message(
                    message.from_user.id,
                    f"{_t('From')} {entity.title} {_t('download')} {message_id} {_t('error')}!",
                    reply_to_message_id=message.id,
                )
        return

    await client.send_message(
        message.from_user.id, msg, parse_mode=pyrogram.enums.ParseMode.HTML
    )


# pylint: disable = R0912, R0915,R0914


async def download_from_bot(client: pyrogram.Client, message: pyrogram.types.Message):
    """Download from bot"""

    msg = (
        f"{_t('Parameter error, please enter according to the reference format')}:\n\n"
        f"1. {_t('Download all messages of common group')}\n"
        "<i>/download https://t.me/fkdhlg 1 0</i>\n\n"
        f"{_t('The private group (channel) link is a random group message link')}\n\n"
        f"2. {_t('The download starts from the N message to the end of the M message')}. "
        f"{_t('When M is 0, it means the last message. The filter is optional')}\n"
        f"<i>/download https://t.me/12000000 N M [filter]</i>\n\n"
    )

    args = message.text.split(maxsplit=4)
    if not message.text or len(args) < 4:
        await client.send_message(
            message.from_user.id, msg, parse_mode=pyrogram.enums.ParseMode.HTML
        )
        return

    url = args[1]
    try:
        start_offset_id = int(args[2])
        end_offset_id = int(args[3])
    except Exception:
        await client.send_message(
            message.from_user.id, msg, parse_mode=pyrogram.enums.ParseMode.HTML
        )
        return

    limit = 0
    if end_offset_id:
        if end_offset_id < start_offset_id:
            raise ValueError(
                f"end_offset_id < start_offset_id, {end_offset_id} < {start_offset_id}"
            )

        limit = end_offset_id - start_offset_id + 1

    download_filter = args[4] if len(args) > 4 else None

    if download_filter:
        download_filter = replace_date_time(download_filter)
        res, err = _bot.filter.check_filter(download_filter)
        if not res:
            await client.send_message(
                message.from_user.id, err, reply_to_message_id=message.id
            )
            return
    entity = None
    try:
        chat_id, _, _ = await parse_link(_bot.client, url)
        if chat_id:
            entity = await _bot.client.get_chat(chat_id)
        if entity:
            chat_title = entity.title or entity.first_name or str(entity.id)
            set_chat_title(entity.id, chat_title)
            reply_message = f"from {chat_title} "
            chat_download_config = ChatDownloadConfig()
            chat_download_config.last_read_message_id = start_offset_id
            chat_download_config.download_filter = download_filter
            reply_message += (
                f"download message id = {start_offset_id} - {end_offset_id} !"
            )
            last_reply_message = await client.send_message(
                message.from_user.id, reply_message, reply_to_message_id=message.id
            )
            node = TaskNode(
                chat_id=entity.id,
                from_user_id=message.from_user.id,
                reply_message_id=last_reply_message.id,
                replay_message=reply_message,
                limit=limit,
                start_offset_id=start_offset_id,
                end_offset_id=end_offset_id,
                bot=_bot.bot,
                task_id=_bot.gen_task_id(),
            )
            _bot.add_task_node(node)
            save_task(
                task_id=node.task_id,
                chat_id=entity.id,
                url=url,
                start_offset_id=start_offset_id,
                end_offset_id=end_offset_id,
                limit=limit,
                download_filter=download_filter,
                from_user_id=message.from_user.id,
                task_type="download",
                extra_data={"task_id_display": node.task_id_display},
            )
            _bot.app.loop.create_task(
                _bot.download_chat_task(_bot.client, chat_download_config, node)
            )
    except Exception as e:
        await client.send_message(
            message.from_user.id,
            f"{_t('chat input error, please enter the channel or group link')}\n\n"
            f"{_t('Error type')}: {e.__class__}"
            f"{_t('Exception message')}: {e}",
        )
        return


async def get_forward_task_node(
    client: pyrogram.Client,
    message: pyrogram.types.Message,
    task_type: TaskType,
    src_chat_link: str,
    dst_chat_link: str,
    offset_id: int = 0,
    end_offset_id: int = 0,
    download_filter: str = None,
    reply_comment: bool = False,
):
    """Get task node"""
    limit: int = 0

    if end_offset_id:
        if end_offset_id < offset_id:
            await client.send_message(
                message.from_user.id,
                f" end_offset_id({end_offset_id}) < start_offset_id({offset_id}),"
                f" end_offset_id{_t('must be greater than')} offset_id",
            )
            return None

        limit = end_offset_id - offset_id + 1

    src_chat_id, _, _ = await parse_link(_bot.client, src_chat_link)
    dst_chat_id, target_msg_id, topic_id = await parse_link(_bot.client, dst_chat_link)

    if not src_chat_id or not dst_chat_id:
        logger.info(f"{src_chat_id} {dst_chat_id}")
        await client.send_message(
            message.from_user.id,
            _t("Invalid chat link") + f"{src_chat_id} {dst_chat_id}",
            reply_to_message_id=message.id,
        )
        return None

    try:
        src_chat = await _bot.client.get_chat(src_chat_id)
        dst_chat = await _bot.client.get_chat(dst_chat_id)
    except Exception as e:
        await client.send_message(
            message.from_user.id,
            f"{_t('Invalid chat link')} {e}",
            reply_to_message_id=message.id,
        )
        logger.exception(f"get chat error: {e}")
        return None

    me = await client.get_me()
    if dst_chat.id == me.id:
        # TODO: when bot receive message judge if download
        await client.send_message(
            message.from_user.id,
            _t("Cannot be forwarded to this bot, will cause an infinite loop"),
            reply_to_message_id=message.id,
        )
        return None

    if download_filter:
        download_filter = replace_date_time(download_filter)
        res, err = _bot.filter.check_filter(download_filter)
        if not res:
            await client.send_message(
                message.from_user.id, err, reply_to_message_id=message.id
            )

    last_reply_message = await client.send_message(
        message.from_user.id,
        "Forwarding message, please wait...",
        reply_to_message_id=message.id,
    )

    node = TaskNode(
        chat_id=src_chat.id,
        from_user_id=message.from_user.id,
        upload_telegram_chat_id=dst_chat_id,
        reply_message_id=last_reply_message.id,
        replay_message=last_reply_message.text,
        has_protected_content=src_chat.has_protected_content,
        download_filter=download_filter,
        limit=limit,
        start_offset_id=offset_id,
        end_offset_id=end_offset_id,
        bot=_bot.bot,
        task_id=_bot.gen_task_id(),
        task_type=task_type,
        topic_id=topic_id,
    )

    if target_msg_id and reply_comment:
        node.reply_to_message = await _bot.client.get_discussion_message(
            dst_chat_id, target_msg_id
        )

    _bot.add_task_node(node)

    node.upload_user = _bot.client
    if not dst_chat.type is pyrogram.enums.ChatType.BOT:
        has_permission = await check_user_permission(_bot.client, me.id, dst_chat.id)
        if has_permission:
            node.upload_user = _bot.bot

    if node.upload_user is _bot.client:
        await client.edit_message_text(
            message.from_user.id,
            last_reply_message.id,
            "Note that the robot may not be in the target group,"
            " use the user account to forward",
        )

    return node


# pylint: disable = R0914
async def forward_message_impl(
    client: pyrogram.Client, message: pyrogram.types.Message, reply_comment: bool
):
    """
    Forward message
    """

    async def report_error(client: pyrogram.Client, message: pyrogram.types.Message):
        """Report error"""

        await client.send_message(
            message.from_user.id,
            f"{_t('Invalid command format')}."
            f"{_t('Please use')} "
            "/forward https://t.me/c/src_chat https://t.me/c/dst_chat "
            f"1 400 `[`{_t('Filter')}`]`\n",
        )

    args = message.text.split(maxsplit=5)
    if len(args) < 5:
        await report_error(client, message)
        return

    src_chat_link = args[1]
    dst_chat_link = args[2]

    try:
        offset_id = int(args[3])
        end_offset_id = int(args[4])
    except Exception:
        await report_error(client, message)
        return

    download_filter = args[5] if len(args) > 5 else None

    node = await get_forward_task_node(
        client,
        message,
        TaskType.Forward,
        src_chat_link,
        dst_chat_link,
        offset_id,
        end_offset_id,
        download_filter,
        reply_comment,
    )

    if not node:
        return

    # Persist forward task for crash recovery
    save_task(
        task_id=node.task_id,
        chat_id=node.chat_id,
        url=src_chat_link,
        start_offset_id=offset_id,
        end_offset_id=end_offset_id,
        limit=node.limit,
        download_filter=download_filter,
        from_user_id=message.from_user.id,
        task_type="forward",
        extra_data={"dst_chat_id": node.upload_telegram_chat_id, "dst_chat_link": dst_chat_link, "task_id_display": node.task_id_display},
    )

    if not node.has_protected_content:
        forward_failed = False
        try:
            async for item in get_chat_history_v2(  # type: ignore
                _bot.client,
                node.chat_id,
                limit=node.limit,
                max_id=node.end_offset_id,
                offset_id=offset_id,
                reverse=True,
            ):
                await forward_normal_content(client, node, item)
                # Update progress for crash recovery
                update_task_progress(node.task_id, item.id)
                if node.is_stop_transmission:
                    await client.edit_message_text(
                        message.from_user.id,
                        node.reply_message_id,
                        f"{_t('Stop Forward')}",
                    )
                    break
        except Exception as e:
            forward_failed = True
            await client.edit_message_text(
                message.from_user.id,
                node.reply_message_id,
                f"{_t('Error forwarding message')} {e}",
            )
        finally:
            await report_bot_status(client, node, immediate_reply=True)
            # Only mark as stopped if forward threw an exception
            if forward_failed:
                node.stop_transmission()
            complete_task(node.task_id)
    else:
        await forward_msg(node, offset_id)
        complete_task(node.task_id)


async def forward_messages(client: pyrogram.Client, message: pyrogram.types.Message):
    """
    Forwards messages from one chat to another.

    Parameters:
        client (pyrogram.Client): The pyrogram client.
        message (pyrogram.types.Message): The message containing the command.

    Returns:
        None
    """
    return await forward_message_impl(client, message, False)


async def forward_normal_content(
    client: pyrogram.Client, node: TaskNode, message: pyrogram.types.Message
):
    """Forward normal content"""
    forward_ret = ForwardStatus.FailedForward
    caption = message.caption
    if caption:
        caption = validate_title(caption)
        _bot.app.set_caption_name(node.chat_id, message.media_group_id, caption)
    else:
        caption = _bot.app.get_caption_name(node.chat_id, message.media_group_id)

    if caption and _bot.app.is_match_advertisement(caption):
        forward_ret = ForwardStatus.SkipForward
        if message.media_group_id:
            # TODO
            node.upload_status[message.id] = UploadStatus.SkipUpload
        return

    if node.download_filter:
        meta_data = MetaData()
        set_meta_data(meta_data, message, caption)
        _bot.filter.set_meta_data(meta_data)
        if not _bot.filter.exec(node.download_filter):
            forward_ret = ForwardStatus.SkipForward
            if message.media_group_id:
                node.upload_status[message.id] = UploadStatus.SkipUpload
                await proc_cache_forward(_bot.client, node, message, False, _bot.app)
            await report_bot_forward_status(client, node, forward_ret)
            return

    await upload_telegram_chat_message(
        _bot.client, node.upload_user, _bot.app, node, message
    )


async def forward_msg(node: TaskNode, message_id: int):
    """Forward normal message"""

    chat_download_config = ChatDownloadConfig()
    chat_download_config.last_read_message_id = message_id
    chat_download_config.download_filter = node.download_filter  # type: ignore

    await _bot.download_chat_task(_bot.client, chat_download_config, node)


# Global flood wait cooldown is now unified in pyrogram_extension._unified_flood_wait

async def _consume_one_pending():
    """Consume exactly one pending task from bot_tasks.json.
    Single-shot, no loop. Sends TG notification on consumption.
    """
    import logging
    logger = logging.getLogger("bot.pending")
    try:
        from module.task_store import get_pending_tasks, update_download_state, remove_task
        add_download_task = _bot.add_download_task
        pending = get_pending_tasks()
        if not pending:
            return
        # 并发守卫：用内存计数器替代 JSON 文件读写，消除 TOCTOU 竞态
        # _active_downloads 在 __main__ 模块（python media_downloader.py 启动时）
        # worker 的 global _active_downloads 改的是 __main__ 的全局变量，
        # 而不是 import media_downloader 的。必须用 sys.modules['__main__']
        import sys
        _main = sys.modules.get('__main__')
        in_queue_count = len(getattr(_bot, '_in_queue', set()))
        consuming_count = len(getattr(_bot, '_consuming', set()))
        max_tasks = getattr(_bot.app, 'max_download_task', 5)
        _active = getattr(_main, '_active_downloads', 0) if _main else 0
        _total_active = _active + in_queue_count + consuming_count
        if _total_active >= max_tasks:
            logger.info(f"Pending consumer: concurrency guard blocked (active={_active} in_queue={in_queue_count} consuming={consuming_count} max={max_tasks})")
            return

        # Find first truly pending task (not already in asyncio Queue)
        task_data = None
        for t in pending:
            if t.get("download_state") == "pending" and t.get("task_id") not in _bot._in_queue:
                task_data = t
                break
        if not task_data:
            return
        task_id = task_data.get("task_id")
        if not task_id:
            return
        
        chat_id = task_data.get("chat_id")
        extra = task_data.get("extra_data", {}) or {}
        msg_id = extra.get("message_id") or extra.get("source_message_id")
        from_user_id = task_data.get("from_user_id", "")
        task_type = task_data.get("task_type", "")
        
        # Config tasks are handled by download_all_chat, skip them
        if task_type == "config":
            remove_task(task_id)
            logger.info(f"Skipped config task {task_id} (handled by download_all_chat)")
            return
        
        if not chat_id or not msg_id:
            return
        client = _bot.client
        if not client:
            return

        # Check unified flood wait cooldown (shared with edit_message, download_media)
        if is_flood_wait_active():
            from module.pyrogram_extension import get_flood_wait_remaining
            remaining = int(get_flood_wait_remaining())
            logger.debug(f"Pending consumer: unified FLOOD_WAIT cooldown, {remaining}s remaining")
            return

        try:
            cid = int(chat_id)
        except (ValueError, TypeError):
            cid = chat_id

        # Mark as consuming (in get_messages phase, counts towards concurrency guard)
        if not hasattr(_bot, '_consuming'):
            _bot._consuming = set()
        _bot._consuming.add(task_id)
        try:
            # Use cached message from direct_download if available (skip get_messages)
            cached_msg = _bot._cached_messages.pop(task_id, None)
            if cached_msg:
                msg = cached_msg
            else:
                try:
                    msg = await asyncio.wait_for(client.get_messages(cid, int(msg_id)), timeout=300)
                except asyncio.TimeoutError:
                    # TG 静默限速：get_messages 300s 超时
                    # 连续超时计数 — 超过3次标失败，不再无限循环
                    _pending_timeout_counts[task_id] = _pending_timeout_counts.get(task_id, 0) + 1
                    consecutive = _pending_timeout_counts[task_id]
                    from module.pyrogram_extension import _unified_flood_wait
                    if consecutive >= 3:
                        # 连续3次超时（共~15分钟），连接已死，标失败
                        logger.error(
                            f"Pending consumer: get_messages TIMEOUT (300s) x{consecutive} for "
                            f"chat {cid} msg {msg_id}, marking as FAILED"
                        )
                        _pending_timeout_counts.pop(task_id, None)
                        try:
                            from module.download_stat import add_failed_download
                            add_failed_download(
                                chat_id=cid, msg_id=msg_id,
                                task_id=extra.get('task_id_display', str(task_id)),
                                file_name="", error_message="获取消息超时（连续3次300s超时，连接已死）",
                                total_size=0, source_link="",
                                from_user_id=str(from_user_id) if from_user_id else "",
                            )
                        except Exception:
                            pass
                        remove_task(task_id)
                        # 触发 client 重连 — 连续超时说明 TCP 已死
                        from media_downloader import _maybe_reconnect_client
                        asyncio.create_task(_maybe_reconnect_client())
                        return
                    # 未达3次，保持 pending，设递增 cooldown
                    backoff = 60 * consecutive  # 60s, 120s, 180s
                    _unified_flood_wait["until"] = time.time() + backoff + 5
                    _unified_flood_wait["reason"] = f"get_messages 超时疑似限速 chat {cid} msg {msg_id} (第{consecutive}次)"
                    logger.warning(
                        f"Pending consumer: get_messages TIMEOUT (300s) for chat {cid} msg {msg_id}, "
                        f"setting {backoff}s cooldown. Task stays pending (consecutive timeout #{consecutive})."
                    )
                    if from_user_id and _bot and _bot.bot:
                        try:
                            notify_text = (
                                "⏸️ TG 连接超时，疑似限速\n"
                                f"任务: {extra.get('task_id_display', str(task_id))} 保持待执行\n"
                                f"暂停 {backoff} 秒后自动重试（第{consecutive}次超时）\n"
                                f"原因: get_messages 300s 超时"
                            )
                            await _bot.bot.send_message(int(from_user_id), notify_text)
                        except Exception:
                            pass
                    return
                except pyrogram.errors.exceptions.flood_420.FloodWait as e:
                    # Don't move to failed list — keep pending, set unified cooldown
                    wait_val = getattr(e, "value", 60)
                    from module.pyrogram_extension import _unified_flood_wait
                    _unified_flood_wait["until"] = time.time() + wait_val + 5
                    _unified_flood_wait["reason"] = f"FloodWait {wait_val}s on get_messages chat {cid} msg {msg_id}"
                    logger.warning(
                        f"Pending consumer: FLOOD_WAIT {wait_val}s on get_messages "
                        f"(chat {cid} msg {msg_id}), pausing consumer. Task stays pending."
                    )
                    # Notify user about rate limit pause
                    if from_user_id and _bot and _bot.bot:
                        try:
                            notify_text = (
                                f"⏸️ TG 限速暂停\n"
                                f"需要等待 {wait_val} 秒\n"
                                f"任务: {extra.get('task_id_display', str(task_id))} 保持待执行\n"
                                f"原因: FloodWait (get_messages)"
                            )
                            await _bot.bot.send_message(int(from_user_id), notify_text)
                        except Exception:
                            pass
                    return
                except Exception as e:
                    # 防御性检测：如果异常是 TimeoutError 类型，按限速处理
                    if isinstance(e, TimeoutError):
                        _pending_timeout_counts[task_id] = _pending_timeout_counts.get(task_id, 0) + 1
                        consecutive = _pending_timeout_counts[task_id]
                        backoff = 60 * consecutive
                        from module.pyrogram_extension import _unified_flood_wait
                        _unified_flood_wait["until"] = time.time() + backoff + 5
                        _unified_flood_wait["reason"] = f"get_messages 连接超时疑似限速 chat {cid} msg {msg_id} (第{consecutive}次)"
                        logger.warning(
                            f"Pending consumer: TimeoutError for chat {cid} msg {msg_id}: {e}, "
                            f"setting {backoff}s cooldown. Task stays pending (consecutive #{consecutive})."
                        )
                        if consecutive >= 3:
                            # 连续3次超时，标失败并触发重连
                            logger.error(f"Pending consumer: TimeoutError x{consecutive}, marking FAILED")
                            _pending_timeout_counts.pop(task_id, None)
                            try:
                                from module.download_stat import add_failed_download
                                add_failed_download(
                                    chat_id=cid, msg_id=msg_id,
                                    task_id=extra.get('task_id_display', str(task_id)),
                                    file_name="", error_message=f"获取消息超时（连续{consecutive}次超时）",
                                    total_size=0, source_link="",
                                    from_user_id=str(from_user_id) if from_user_id else "",
                                )
                            except Exception:
                                pass
                            remove_task(task_id)
                            from media_downloader import _maybe_reconnect_client
                            asyncio.create_task(_maybe_reconnect_client())
                        return  # 保持 pending，不 remove_task
                    logger.warning(f"Pending consumer: get_messages failed for chat {cid} msg {msg_id}: {e}, moving to failed")
                    try:
                        from module.download_stat import add_failed_download
                        task_id_display = extra.get("task_id_display", str(task_id))
                        add_failed_download(
                            chat_id=cid,
                            msg_id=msg_id,
                            task_id=task_id_display,
                            file_name="",
                            error_message=f"获取消息失败: {e}",
                            total_size=0,
                            source_link="",
                            from_user_id=str(from_user_id) if from_user_id else "",
                        )
                    except Exception:
                        pass
                    remove_task(task_id)
                    return
            if not msg or msg.empty:
                logger.warning(f"Pending consumer: msg {msg_id} not found in chat {cid}, moving to failed")
                try:
                    from module.download_stat import add_failed_download
                    task_id_display = extra.get("task_id_display", str(task_id))
                    source_link = ""
                    if extra.get("source_chat_id") and extra.get("source_message_id"):
                        sid = extra["source_chat_id"]
                        link_id = str(sid)[4:] if str(sid).startswith("-100") else str(sid)
                        source_link = f"https://t.me/c/{link_id}/{extra['source_message_id']}"
                    add_failed_download(
                        chat_id=cid,
                        msg_id=msg_id,
                        task_id=task_id_display,
                        file_name="",
                        error_message="消息不存在或已被删除",
                        total_size=0,
                        source_link=source_link,
                        from_user_id=str(from_user_id) if from_user_id else "",
                    )
                except Exception:
                    pass
                remove_task(task_id)
                return
            # get_messages 成功 — 清除连续超时计数
            _pending_timeout_counts.pop(task_id, None)
        finally:
            _bot._consuming.discard(task_id)
        node = _bot.task_node.get(int(task_id)) if str(task_id).isdigit() else _bot.task_node.get(task_id)
        if not node:
            from module.app import TaskNode
            # Preserve original task_id_display and source info from extra_data
            # so recovered tasks keep their original display ID (e.g. 0626-784)
            # instead of generating a new one (0628-1241)
            node = TaskNode(
                chat_id=cid,
                from_user_id=from_user_id or cid,
                reply_message_id=0,
                limit=1,
                bot=_bot.bot,
                task_id=task_id,
                task_id_display=extra.get("task_id_display", ""),
            )
            node.source_chat_id = extra.get("source_chat_id", 0)
            node.source_message_id = extra.get("source_message_id", 0)
            node.source_chat_title = extra.get("source_chat_title", "")
            _bot.add_task_node(node)
        # Mark as in-queue (in-memory, not persisted). Worker will remove on pickup.
        _bot._in_queue.add(task_id)
        node.is_running = True

        # Create placeholder in _download_result so WebUI shows the task immediately.
        # Pyrogram's update_download_status will overwrite total_size/file_name/down_byte
        # when the actual download starts.
        # Set total_size=1 (not 0) so web.py doesn't filter it out as a "placeholder"
        # entry. WebUI will show "获取文件信息中..." until real size arrives.
        from module.download_stat import _download_result
        if cid not in _download_result:
            _download_result[cid] = {}
        if int(msg_id) not in _download_result[cid]:
            _download_result[cid][int(msg_id)] = {
                "down_byte": 0,
                "total_size": 1,  # Non-zero so web.py doesn't hide this entry
                "progress": 0,
                "file_name": "获取文件信息中...",
                "start_time": time.time(),
                "end_time": 0,
                "download_speed": 0,
                "each_second_total_download": 0,
                "task_id": task_id,
                "task_id_display": getattr(node, "task_id_display", str(task_id)),
                "source_chat_title": getattr(msg.chat, "title", "") or extra.get("source_chat_title", ""),
                "source_chat_id": cid,
                "source_message_id": int(msg_id),
            }
            logger.info(f"Placeholder created in _download_result for task {task_id} (chat {cid} msg {msg_id})")

        # Send notification BEFORE add_download_task to avoid race condition:
        # if we queue first, worker may finish a small file before send_message
        # returns, causing reply_message_id=0 and missing final status notification
        # Skip if reply_message_id already set (e.g. recovery task already sent B6)
        if from_user_id and _bot and _bot.bot and not node.reply_message_id:
            try:
                chat_name = getattr(msg.chat, "title", str(cid))
                notify_text = (
                    "📥 待下载任务开始下载\n"
                    f"消息: {msg_id}\n"
                    f"任务: {node.task_id_display}\n"
                    f"群组: {chat_name}"
                )
                recovery_msg = await _bot.bot.send_message(int(from_user_id), notify_text)
                node.reply_message_id = recovery_msg.id
                node.last_edit_msg = ""       # Force first progress update to trigger
                node.last_progress_pct = -1   # Force first 20% bucket to trigger
            except Exception as e:
                logger.warning(f"Pending consumer notification failed: {e}")

        await add_download_task(msg, node)
        # Mark as downloading so concurrency guard counts this task.
        # Without this, get_downloading_tasks() can't see it, and the
        # guard keeps feeding new tasks beyond max_download_task.
        update_download_state(task_id, "downloading")
        logger.info(f"Pending consumer: queued task {task_id} (msg {msg_id}) to download queue")
    except Exception as e:
        logger.warning(f"Pending consumer error: {e}")


async def _pending_consumer_loop():
    """Periodically fill the worker queue with pending tasks.

    Runs every 2s. Each tick consumes ONE pending task — the concurrency
    guard inside _consume_one_pending checks downloading + in_queue + consuming
    >= max_download_task and returns early if all slots are full.
    This prevents over-filling when get_messages is fast (cached messages).
    """
    import logging
    logger = logging.getLogger("bot.pending_loop")
    while True:
        try:
            await asyncio.sleep(2)
            from module.task_store import get_pending_tasks
            pending = get_pending_tasks()
            if pending:
                await _consume_one_pending()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"Pending consumer loop error: {e}")


async def set_listen_forward_msg(
    client: pyrogram.Client, message: pyrogram.types.Message
):
    """
    Set the chat to listen for forwarded messages.
    """
    args = message.text.split(maxsplit=3)

    if len(args) < 3:
        await client.send_message(
            message.from_user.id,
            f"{_t('Invalid command format')}. {_t('Please use')} /listen_forward "
            f"https://t.me/c/src_chat https://t.me/c/dst_chat [{_t('Filter')}]\n",
        )
        return

    src_chat_link = args[1]
    dst_chat_link = args[2]
    download_filter = args[3] if len(args) > 3 else None

    node = await get_forward_task_node(
        client,
        message,
        TaskType.ListenForward,
        src_chat_link,
        dst_chat_link,
        download_filter=download_filter,
    )

    if not node:
        return

    if node.chat_id in _bot.listen_forward_chat:
        _bot.remove_task_node(_bot.listen_forward_chat[node.chat_id].task_id)

    node.is_running = True
    _bot.listen_forward_chat[node.chat_id] = node

    # 用事件驱动替代轮询：注册 NewMessage handler 实时监听
    _bot._register_listen_handler()
    if not hasattr(_bot, "pending_consumer_task") or _bot.pending_consumer_task is None:
        _bot.pending_consumer_task = _bot.app.loop.create_task(_consume_one_pending())


async def stop(client: pyrogram.Client, message: pyrogram.types.Message):
    """Stops listening for forwarded messages."""

    await client.send_message(
        message.chat.id,
        _t("Please select:"),
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        _t("Stop Download"), callback_data="stop_download"
                    ),
                    InlineKeyboardButton(
                        _t("Stop Forward"), callback_data="stop_forward"
                    ),
                ],
                [  # Second row
                    InlineKeyboardButton(
                        _t("Stop Listen Forward"), callback_data="stop_listen_forward"
                    )
                ],
            ]
        ),
    )


async def stop_task(
    client: pyrogram.Client,
    query: pyrogram.types.CallbackQuery,
    queryHandler: str,
    task_type: TaskType,
):
    """Stop task"""
    if query.data == queryHandler:
        buttons: List[InlineKeyboardButton] = []
        temp_buttons: List[InlineKeyboardButton] = []
        for key, value in _bot.task_node.copy().items():
            if not value.is_finish() and value.task_type is task_type:
                if len(temp_buttons) == 3:
                    buttons.append(temp_buttons)
                    temp_buttons = []
                temp_buttons.append(
                    InlineKeyboardButton(
                        f"{key}", callback_data=f"{queryHandler} task {key}"
                    )
                )
        if temp_buttons:
            buttons.append(temp_buttons)

        if buttons:
            buttons.insert(
                0,
                [
                    InlineKeyboardButton(
                        _t("all"), callback_data=f"{queryHandler} task all"
                    )
                ],
            )
            await client.edit_message_text(
                query.message.from_user.id,
                query.message.id,
                f"{_t('Stop')} {_t(task_type.name)}...",
                reply_markup=InlineKeyboardMarkup(buttons),
            )
        else:
            await client.edit_message_text(
                query.message.from_user.id,
                query.message.id,
                f"{_t('No Task')}",
            )
    else:
        task_id = query.data.split(" ")[2]
        await client.edit_message_text(
            query.message.from_user.id,
            query.message.id,
            f"{_t('Stop')} {_t(task_type.name)}...",
        )
        _bot.stop_task(task_id)


async def on_query_handler(
    client: pyrogram.Client, query: pyrogram.types.CallbackQuery
):
    """
    Asynchronous function that handles query callbacks.

    Parameters:
        client (pyrogram.Client): The Pyrogram client object.
        query (pyrogram.types.CallbackQuery): The callback query object.

    Returns:
        None
    """

    for it in QueryHandler:
        queryHandler = QueryHandlerStr.get_str(it.value)
        if queryHandler in query.data:
            await stop_task(client, query, queryHandler, TaskType(it.value))


async def forward_to_comments(client: pyrogram.Client, message: pyrogram.types.Message):
    """
    Forwards specified media to a designated comment section.

    Usage: /forward_to_comments <source_chat_link> <destination_chat_link> <msg_start_id> <msg_end_id>

    Parameters:
        client (pyrogram.Client): The pyrogram client.
        message (pyrogram.types.Message): The message containing the command.
    """
    return await forward_message_impl(client, message, True)
