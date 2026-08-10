"""Downloads media from telegram."""
import asyncio
import logging
import os
import shutil
import time
from typing import List, Optional, Tuple, Union

import pyrogram
from loguru import logger
from pyrogram.types import Audio, Document, Photo, Video, VideoNote, Voice
from rich.logging import RichHandler

from module.app import Application, ChatDownloadConfig, DownloadStatus, TaskNode
from module.bot import start_download_bot, stop_download_bot
from module.download_stat import load_downloads, save_downloads, set_chat_title, update_download_status
from module.download_stat import add_failed_download as _add_failed_download
from module.task_store import update_task_progress, update_download_state
from module.get_chat_history_v2 import get_chat_history_v2
from module.language import _t
from module.pyrogram_extension import (
    HookClient,
    fetch_message,
    get_extension,
    record_download_status,
    report_bot_download_status,
    set_max_concurrent_transmissions,
    set_meta_data,
    update_cloud_upload_stat,
    upload_telegram_chat,
)
from module.web import init_web
from utils.format import truncate_filename, validate_title
from utils.log import LogFilter
from utils.meta import print_meta
from utils.meta_data import MetaData

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler()],
)

CONFIG_NAME = "config.yaml"
DATA_FILE_NAME = "data.yaml"
APPLICATION_NAME = "media_downloader"
app = Application(CONFIG_NAME, DATA_FILE_NAME, APPLICATION_NAME)

queue: asyncio.Queue = None  # Created in main() after event loop starts
RETRY_TIME_OUT = 3

# ── Client connection error tracking & auto-reconnect ──
# When download_media hits connection-level errors (TimeoutError, OSError,
# FILE_REFERENCE_EXPIRED) consecutively, the Pyrogram session's underlying
# TCP connection is likely in a "half-dead" state — socket is alive but
# no data flows. Pyrogram's internal retry (3x) uses the same dead socket,
# so all retries fail. Auto-reconnect forces client.stop()+start() to build
# a fresh TCP connection + MTProto session, mirroring the manual fix of
# toggling the v2rayA node.
_client_conn_errors = {"count": 0}
_CLIENT_RECONNECT_THRESHOLD = 10
_client_reconnecting = {"active": False}
_client_last_reconnect = {"time": 0.0}
_CLIENT_RECONNECT_COOLDOWN = 300  # 5 min between reconnect attempts
_main_client_ref = {"client": None}  # set in start_server()

logging.getLogger("pyrogram.session.session").addFilter(LogFilter())
logging.getLogger("pyrogram.client").addFilter(LogFilter())

logging.getLogger("pyrogram").setLevel(logging.WARNING)


def _check_download_finish(media_size: int, download_path: str, ui_file_name: str):
    """Check download task if finish"""
    download_size = os.path.getsize(download_path)
    if media_size == download_size:
        logger.success(f"{_t('Successfully downloaded')} - {ui_file_name}")
    else:
        logger.warning(
            f"{_t('Media downloaded with wrong size')}: "
            f"{download_size}, {_t('actual')}: "
            f"{media_size}, {_t('file name')}: {ui_file_name}"
        )
        os.remove(download_path)
        raise pyrogram.errors.exceptions.bad_request_400.BadRequest()


def _move_to_download_path(temp_download_path: str, download_path: str):
    """Move file to download path"""
    directory, _ = os.path.split(download_path)
    os.makedirs(directory, exist_ok=True)
    shutil.move(temp_download_path, download_path)


def _check_timeout(retry: int, _: int):
    """Check if message download timeout"""
    if retry == 2:
        return True
    return False


def _can_download(_type: str, file_formats: dict, file_format: Optional[str]) -> bool:
    """Check if the given file format can be downloaded."""
    if _type in ["audio", "document", "video"]:
        allowed_formats: list = file_formats[_type]
        if not file_format in allowed_formats and allowed_formats[0] != "all":
            return False
    return True


def _is_exist(file_path: str) -> bool:
    """Check if a file exists and it is not a directory."""
    return not os.path.isdir(file_path) and os.path.exists(file_path)


def _cleanup_temp_file(temp_file_name: str):
    """Remove temp file if it exists."""
    if temp_file_name and os.path.exists(temp_file_name):
        try:
            os.remove(temp_file_name)
        except OSError:
            pass


def _cleanup_stale_temp_files():
    """Remove stale temp files on startup.

    Rules:
    - 0-byte .temp files: always delete (empty shells from failed downloads)
    - Non-zero .temp files: delete if corresponding target file exists in downloads/
    - Empty directories in temp/: delete
    """
    temp_dir = app.temp_save_path
    if not os.path.isdir(temp_dir):
        return

    removed = 0
    for root, dirs, files in os.walk(temp_dir, topdown=False):
        for f in files:
            if not f.endswith('.temp'):
                continue
            temp_path = os.path.join(root, f)
            try:
                file_size = os.path.getsize(temp_path)
            except OSError:
                continue

            if file_size == 0:
                # Empty temp file — always remove
                try:
                    os.remove(temp_path)
                    removed += 1
                except OSError:
                    pass
            else:
                # Non-zero: check if target already exists in downloads/
                # temp path: temp/chat_id/filename.ext.temp
                # target path: downloads/chat_id/filename.ext
                rel_path = os.path.relpath(temp_path, temp_dir)
                # Strip .temp suffix to get the target filename
                target_name = f[:-5] if f.endswith('.temp') else f
                target_path = os.path.join(
                    os.path.abspath("."), "downloads",
                    os.path.dirname(rel_path), target_name
                )
                if os.path.exists(target_path):
                    try:
                        target_size = os.path.getsize(target_path)
                        if target_size >= file_size:
                            os.remove(temp_path)
                            removed += 1
                    except OSError:
                        pass

        # Remove empty directories
        for d in dirs:
            dir_path = os.path.join(root, d)
            try:
                if not os.listdir(dir_path):
                    os.rmdir(dir_path)
            except OSError:
                pass

    if removed:
        logger.info(f"Startup cleanup: removed {removed} stale temp files")


async def _get_media_meta(
    chat_id: Union[int, str],
    message: pyrogram.types.Message,
    media_obj: Union[Audio, Document, Photo, Video, VideoNote, Voice],
    _type: str,
) -> Tuple[str, str, Optional[str]]:
    """Extract file name and file id from media object."""
    if _type in ["audio", "document", "video"]:
        file_format: Optional[str] = media_obj.mime_type.split("/")[-1]
    else:
        file_format = None

    file_name = None
    temp_file_name = None
    dirname = validate_title(f"{chat_id}")
    if message.chat and message.chat.title:
        dirname = validate_title(f"{message.chat.title}")

    if message.date:
        datetime_dir_name = message.date.strftime(app.date_format)
    else:
        datetime_dir_name = "0"

    if _type in ["voice", "video_note"]:
        file_format = media_obj.mime_type.split("/")[-1]
        file_save_path = app.get_file_save_path(_type, dirname, datetime_dir_name)
        file_name = "{} - {}_{}.{}".format(
            message.id, _type, media_obj.date.isoformat(), file_format,
        )
        file_name = validate_title(file_name)
        temp_file_name = os.path.join(app.temp_save_path, dirname, file_name)
        file_name = os.path.join(file_save_path, file_name)
    else:
        file_name = getattr(media_obj, "file_name", None)
        caption = getattr(message, "caption", None)

        file_name_suffix = ".unknown"
        if not file_name:
            file_name_suffix = get_extension(
                media_obj.file_id, getattr(media_obj, "mime_type", "")
            )
        else:
            _, file_name_without_suffix = os.path.split(os.path.normpath(file_name))
            file_name, file_name_suffix = os.path.splitext(file_name_without_suffix)
            if not file_name_suffix:
                file_name_suffix = get_extension(
                    media_obj.file_id, getattr(media_obj, "mime_type", "")
                )

        if caption:
            caption = validate_title(caption)
            app.set_caption_name(chat_id, message.media_group_id, caption)
            app.set_caption_entities(
                chat_id, message.media_group_id, message.caption_entities
            )
        else:
            caption = app.get_caption_name(chat_id, message.media_group_id)

        if not file_name and message.photo:
            file_name = f"{message.photo.file_unique_id}"

        gen_file_name = (
            app.get_file_name(message.id, file_name, caption) + file_name_suffix
        )
        file_save_path = app.get_file_save_path(_type, dirname, datetime_dir_name)
        temp_file_name = os.path.join(app.temp_save_path, dirname, gen_file_name)
        file_name = os.path.join(file_save_path, gen_file_name)
    return truncate_filename(file_name), truncate_filename(temp_file_name), file_format


async def add_download_task(message: pyrogram.types.Message, node: TaskNode):
    """Add Download task"""
    if message.empty:
        return False
    if queue is None:
        logger.error(f"add_download_task: queue is None! msg {message.id} cannot be queued")
        return False
    node.download_status[message.id] = DownloadStatus.Downloading
    await queue.put((message, node))
    node.total_task += 1
    logger.info(f"add_download_task: put msg {message.id} into queue (size now {queue.qsize()}), task {getattr(node, 'task_id_display', node.task_id)}")
    return True


async def save_msg_to_file(app, chat_id: Union[int, str], message: pyrogram.types.Message):
    """Write message text into file"""
    dirname = validate_title(
        message.chat.title if message.chat and message.chat.title else str(chat_id)
    )
    datetime_dir_name = message.date.strftime(app.date_format) if message.date else "0"
    file_save_path = app.get_file_save_path("msg", dirname, datetime_dir_name)
    file_name = os.path.join(
        app.temp_save_path, file_save_path,
        f"{app.get_file_name(message.id, None, None)}.txt",
    )
    os.makedirs(os.path.dirname(file_name), exist_ok=True)
    if _is_exist(file_name):
        return DownloadStatus.SkipDownload, None
    with open(file_name, "w", encoding="utf-8") as f:
        f.write(message.text or "")
    return DownloadStatus.SuccessDownload, file_name


async def download_task(client: pyrogram.Client, message: pyrogram.types.Message, node: TaskNode):
    """Download and Forward media"""
    download_status, file_name, error_message = await download_media(
        client, message, app.media_types, app.file_formats, node
    )
    # Backfill source_chat_title from cache (populated during download_media)
    if not node.source_chat_title and getattr(node, 'source_chat_id', 0):
        from module.download_stat import get_chat_title as _gct
        cached = _gct(node.source_chat_id)
        if cached:
            node.source_chat_title = cached
    if app.enable_download_txt and message.text and not message.media:
        download_status, file_name = await save_msg_to_file(app, node.chat_id, message)
    if not node.bot:
        app.set_download_id(node, message.id, download_status)
    node.download_status[message.id] = download_status
    file_size = os.path.getsize(file_name) if file_name else 0
    # Record failed downloads to the failed list for webui display
    if download_status is DownloadStatus.FailedDownload:
        # Get task_id_display (format: MMDD-N)
        task_id_display = getattr(node, "task_id_display", "") or str(node.task_id)
        # Build source link from node or message
        source_link = ""
        if getattr(node, 'source_chat_id', 0) and getattr(node, 'source_message_id', 0):
            # For forwarded messages, use source channel link
            source_id = node.source_chat_id
            if str(source_id).startswith("-100"):
                link_id = str(source_id)[4:]
            else:
                link_id = str(source_id)
            source_link = f"https://t.me/c/{link_id}/{node.source_message_id}"
        elif message and message.chat:
            # For direct messages, use current message link
            chat_id_for_link = message.chat.id
            if hasattr(message.chat, 'username') and message.chat.username:
                source_link = f"https://t.me/{message.chat.username}/{message.id}"
            else:
                if str(chat_id_for_link).startswith("-100"):
                    link_id = str(chat_id_for_link)[4:]
                else:
                    link_id = str(chat_id_for_link)
                source_link = f"https://t.me/c/{link_id}/{message.id}"
        _add_failed_download(
            chat_id=node.chat_id,
            msg_id=message.id if message else message_id,
            task_id=task_id_display,
            file_name=file_name or "",
            error_message=error_message or "下载失败",
            total_size=file_size,
            source_link=source_link,
            from_user_id=getattr(node, "from_user_id", "") or "",
        )
        # Remove from active download list so it doesn't stay in WebUI forever
        from module.download_stat import delete_download_result_entry as _ddre
        _ddre(node.chat_id, message.id if message else message_id)
    elif download_status is DownloadStatus.SkipDownload:
        # Remove placeholder from active download list
        from module.download_stat import delete_download_result_entry as _ddre
        _ddre(node.chat_id, message.id if message else message_id)
    await upload_telegram_chat(
        client, node.upload_user if node.upload_user else client,
        app, node, message, download_status, file_name,
    )
    if not node.upload_telegram_chat_id and download_status is DownloadStatus.SuccessDownload:
        ui_file_name = file_name
        if app.hide_file_name:
            ui_file_name = f"****{os.path.splitext(file_name)[-1]}"
        if await app.upload_file(file_name, update_cloud_upload_stat, (node, message.id, ui_file_name)):
            node.upload_success_count += 1
    await report_bot_download_status(node.bot, node, download_status, file_size)
    # Send final status with full stats immediately for single downloads
    if node.bot and node.is_finish() and not node.is_stop_transmission:
        from module.pyrogram_extension import report_bot_status
        try:
            await report_bot_status(node.bot, node, immediate_reply=True)
        except Exception as e:
            logger.warning(f"Failed to send final bot status for task {node.task_id}: {e}")
        try:
            from module.task_store import complete_task as _ct
            _ct(node.task_id)
        except Exception as e:
            logger.warning(f"Failed to complete task {node.task_id}: {e}")


@record_download_status
async def download_media(
    client: pyrogram.client.Client,
    message: pyrogram.types.Message,
    media_types: List[str],
    file_formats: dict,
    node: TaskNode,
):
    """Download media from Telegram. Each file retried 3 times with 5s delay.
    Returns: (DownloadStatus, file_name, error_message)
    """
    file_name: str = ""
    ui_file_name: str = ""
    task_start_time: float = time.time()
    media_size = 0
    _media = None
    error_message = ""  # Track specific error reason
    # Skip the initial fetch_message — the pending consumer already called
    # get_messages seconds ago, so the file reference is fresh. The extra
    # get_messages call here was causing indefinite blocks when TG rate-limits
    # the same chat. If the file reference does expire during download, the
    # retry loop below will call fetch_message to refresh it.

    # Cache chat title from message object
    if message and message.chat:
        chat_title = getattr(message.chat, 'title', None) or getattr(message.chat, 'first_name', None)
        if chat_title:
            set_chat_title(message.chat.id, chat_title)
    try:
        for _type in media_types:
            _media = getattr(message, _type, None)
            if _media is None:
                continue
            file_name, temp_file_name, file_format = await _get_media_meta(
                node.chat_id, message, _media, _type
            )
            media_size = getattr(_media, "file_size", 0)
            ui_file_name = file_name
            if app.hide_file_name:
                ui_file_name = f"****{os.path.splitext(file_name)[-1]}"

            if _can_download(_type, file_formats, file_format):
                if _is_exist(file_name):
                    file_size = os.path.getsize(file_name)
                    if media_size > 0 and file_size >= media_size:
                        logger.info(
                            f"id={message.id} {ui_file_name} "
                            f"{_t('already download,download skipped')}."
                        )
                        return DownloadStatus.SkipDownload, None, ""
                    elif file_size > 0:
                        # 先重命名原文件为 .bak，下载成功后再删除备份
                        backup_path = file_name + ".bak"
                        os.replace(file_name, backup_path)
                        logger.info(
                            f"id={message.id} {ui_file_name} "
                            f"{_t('File exists but size mismatch')}: "
                            f"{file_size} != {media_size}, {_t('re-downloading')}."
                        )
            else:
                return DownloadStatus.SkipDownload, None, ""
            break
    except Exception as e:
        logger.error(
            f"Message[{message.id}]: "
            f"{_t('could not be downloaded due to following exception')}:\n[{e}].",
            exc_info=True,
        )
        return DownloadStatus.SkipDownload, None, ""
    if _media is None:
        logger.warning(f"Message[{message.id}]: no media found in message, skipping download")
        return DownloadStatus.SkipDownload, None, ""
    # Build source link from message for failed downloads
    source_link = ""
    if message and message.chat:
        chat_id_for_link = message.chat.id
        # For private chats (user bot), use username if available
        if hasattr(message.chat, 'username') and message.chat.username:
            source_link = f"https://t.me/{message.chat.username}/{message.id}"
        else:
            # For channels/supergroups, use c/ prefix
            # Remove -100 prefix for channels
            if str(chat_id_for_link).startswith("-100"):
                link_id = str(chat_id_for_link)[4:]
            else:
                link_id = str(chat_id_for_link)
            source_link = f"https://t.me/c/{link_id}/{message.id}"

    message_id = message.id
    total_wait = 0
    for retry in range(3):
        try:
            temp_download_path = await client.download_media(
                message, file_name=temp_file_name,
                progress=update_download_status,
                progress_args=(message_id, ui_file_name, task_start_time, node, client),
            )
            if temp_download_path and isinstance(temp_download_path, str):
                _check_download_finish(media_size, temp_download_path, ui_file_name)
                await asyncio.sleep(0.5)
                _move_to_download_path(temp_download_path, file_name)
                # 清除 .bak 备份（下载成功）
                bak_path = file_name + ".bak"
                if os.path.exists(bak_path):
                    try:
                        os.remove(bak_path)
                    except OSError:
                        pass
                _client_conn_errors["count"] = 0  # Reset on success
                return DownloadStatus.SuccessDownload, file_name, ""
            else:
                # download_media returned None or non-str — Pyrogram couldn't fetch
                # without raising. Log details and set error_message for user.
                reason = "下载返回为空" if temp_download_path is None else f"下载返回类型异常: {type(temp_download_path).__name__}"
                logger.warning(
                    f"Message[{message.id}] {ui_file_name}: "
                    f"client.download_media returned {repr(temp_download_path)}, "
                    f"retry {retry + 1}/3"
                )
                error_message = reason
                await asyncio.sleep(RETRY_TIME_OUT)
                message = await fetch_message(client, message)
                if message is None:
                    logger.error(f"Message[{message_id}] {ui_file_name}: fetch_message returned None, message may be deleted")
                    error_message = "消息不存在或已被删除"
                    break
                if _check_timeout(retry, message.id):
                    logger.error(
                        f"Message[{message.id}] {ui_file_name}: "
                        f"download_media returned None/empty after 3 retries."
                    )
                    if error_message:
                        error_message = f"{error_message}（重试3次后失败）"
        except pyrogram.errors.exceptions.bad_request_400.BadRequest:
            _cleanup_temp_file(temp_file_name)
            logger.warning(
                f"Message[{message.id}]: {_t('file reference expired, refetching')}..."
            )
            error_message = "文件引用过期"
            _client_conn_errors["count"] += 1
            if await _maybe_reconnect_client():
                error_message = "文件引用过期（触发客户端重连）"
            await asyncio.sleep(RETRY_TIME_OUT)
            message = await fetch_message(client, message)
            if message is None:
                logger.error(f"Message[{message_id}] {ui_file_name}: fetch_message returned None (file ref expired), message may be deleted")
                error_message = "消息不存在或已被删除（文件引用过期）"
                break
            if _check_timeout(retry, message.id):
                logger.error(
                    f"Message[{message.id}]: {_t('file reference expired for 3 retries, download skipped.')}"
                )
                error_message = "文件引用过期（重试3次后失败）"
        except pyrogram.errors.exceptions.flood_420.FloodWait as wait_err:
            _cleanup_temp_file(temp_file_name)
            # 累计 FloodWait 超过600 秒则不再等待
            total_wait += wait_err.value
            if total_wait > 600:
                logger.error(
                    f"Message[{message.id}]: {_t('FloodWait total timeout exceeded, download skipped.')}"
                )
                error_message = f"频率限制总超时，累计等待{total_wait}秒"
                break
            # Set unified cooldown so edit_message and pending consumer pause too
            from module.pyrogram_extension import _unified_flood_wait
            _unified_flood_wait["until"] = time.time() + wait_err.value + 5
            _unified_flood_wait["reason"] = f"download_media FloodWait {wait_err.value}s (msg {message.id})"
            # First FloodWait for this file: notify user so they know progress is paused
            if total_wait == wait_err.value and node and node.bot and getattr(node, "from_user_id", ""):
                try:
                    notify_text = (
                        "⏳ 下载遇到 TG 限速\n"
                        f"任务: {getattr(node, 'task_id_display', str(node.task_id))}\n"
                        f"文件: {ui_file_name}\n"
                        f"需等待 {wait_err.value} 秒后自动重试"
                    )
                    await node.bot.send_message(int(node.from_user_id), notify_text)
                except Exception:
                    pass
            await asyncio.sleep(wait_err.value)
            logger.info("Message[{}]: FlowWait {}s, waiting (total={}s)", message.id, wait_err.value, total_wait)
            error_message = f"频率限制，等待{wait_err.value}秒"
            _check_timeout(retry, message.id)
            # Notify user that download has resumed after FloodWait
            if node and node.bot and getattr(node, "from_user_id", ""):
                try:
                    resume_text = (
                        "✅ 限速恢复，继续下载\n"
                        f"任务: {getattr(node, 'task_id_display', str(node.task_id))}\n"
                        f"文件: {ui_file_name}"
                    )
                    await node.bot.send_message(int(node.from_user_id), resume_text)
                except Exception:
                    pass
        except TypeError:
            _cleanup_temp_file(temp_file_name)
            logger.warning(
                f"{_t('Timeout Error occurred when downloading Message')}[{message.id}], "
                f"{_t('retrying after')} {RETRY_TIME_OUT} {_t('seconds')}"
            )
            error_message = "下载超时"
            await asyncio.sleep(RETRY_TIME_OUT)
            if _check_timeout(retry, message.id):
                logger.error(
                    f"Message[{message.id}]: {_t('Timing out after 3 reties, download skipped.')}"
                )
                error_message = "下载超时（重试3次后失败）"
        except TimeoutError as e:
            # TG 静默限速：连接超时，没有显式 FloodWait 错误码
            # 注意：TimeoutError 是 OSError 子类，必须放在 except OSError 之前
            _cleanup_temp_file(temp_file_name)
            # 递增退避：第1次60s，第2次120s，第3次300s
            backoff = [60, 120, 300][min(retry, 2)]
            from module.pyrogram_extension import _unified_flood_wait
            _unified_flood_wait["until"] = time.time() + backoff + 5
            _unified_flood_wait["reason"] = f"连接超时疑似限速 (msg {message.id})"
            _client_conn_errors["count"] += 1
            if await _maybe_reconnect_client():
                backoff = 5  # Short backoff after reconnect
            # 第一次超时就通知用户
            if retry == 0 and node and node.bot and getattr(node, "from_user_id", ""):
                try:
                    notify_text = (
                        "⏸️ TG 连接超时，疑似限速\n"
                        f"任务: {getattr(node, 'task_id_display', str(node.task_id))}\n"
                        f"文件: {ui_file_name}\n"
                        f"暂停 {backoff} 秒后自动重试\n"
                        f"原因: Request timed out (非FloodWait)"
                    )
                    await node.bot.send_message(int(node.from_user_id), notify_text)
                except Exception:
                    pass
            await asyncio.sleep(backoff)
            # 刷新消息引用（可能已过期），用 try-except 防止二次超时
            try:
                message = await fetch_message(client, message)
            except Exception as fetch_err:
                logger.warning(f"Message[{message.id}]: fetch_message 也超时: {fetch_err}")
            error_message = f"连接超时疑似限速（等待{backoff}秒后重试）"
        except OSError as e:
            # 连接级错误（网络断连、代理断开等）
            # TimeoutError 已被上面的 handler 拦截，这里处理非超时类连接错误
            # 但如果异常信息包含 timeout 关键字，也按限速处理（防御性）
            _cleanup_temp_file(temp_file_name)
            err_str = str(e).lower()
            if "timed out" in err_str or "timeout" in err_str:
                # 看起来像超时，用长退避 + 设 cooldown
                backoff = [60, 120, 300][min(retry, 2)]
                _unified_flood_wait["until"] = time.time() + backoff + 5
                _unified_flood_wait["reason"] = f"连接超时疑似限速 (msg {message.id})"
            else:
                # 普通连接错误，保持原有指数退避：10s, 20s, 40s
                backoff = 10 * (2 ** retry)
            _client_conn_errors["count"] += 1
            if await _maybe_reconnect_client():
                backoff = 5  # Short backoff after reconnect
            logger.warning(
                f"Message[{message.id}] {ui_file_name}: connection error ({type(e).__name__}), "
                f"retry {retry + 1}/3 after {backoff}s backoff"
            )
            error_message = f"连接错误: {str(e)[:80]}"
            await asyncio.sleep(backoff)
            message = await fetch_message(client, message)
            if message is None:
                logger.error(f"Message[{message_id}] {ui_file_name}: fetch_message returned None after connection error")
                error_message = "连接错误后消息不可用"
                break
            if _check_timeout(retry, message.id):
                logger.error(
                    f"Message[{message.id}] {ui_file_name}: connection failed after 3 retries"
                )
                error_message = f"连接错误（重试3次后失败）"
        except Exception as e:
            _cleanup_temp_file(temp_file_name)
            error_str = str(e)
            # "This message doesn't contain any downloadable media" happens when
            # the file reference is stale. Refresh the message and retry instead
            # of breaking immediately.
            if "doesn't contain any downloadable media" in error_str:
                logger.warning(
                    f"Message[{message.id}] {ui_file_name}: stale file reference "
                    f"(no downloadable media), refreshing message (retry {retry + 1}/3)"
                )
                error_message = "文件引用过期，正在刷新消息"
                await asyncio.sleep(RETRY_TIME_OUT)
                message = await fetch_message(client, message)
                if message is None:
                    logger.error(f"Message[{message_id}] {ui_file_name}: fetch_message returned None (stale file ref)")
                    error_message = "文件引用过期且消息不可用"
                    break
                if _check_timeout(retry, message.id):
                    logger.error(
                        f"Message[{message.id}] {ui_file_name}: "
                        f"still no downloadable media after 3 retries with message refresh"
                    )
                    error_message = "文件引用过期（刷新消息后重试3次仍失败）"
                continue
            logger.error(
                f"Message[{message.id}]: "
                f"{_t('could not be downloaded due to following exception')}:\n[{e}].",
                exc_info=True,
            )
            error_message = f"下载异常: {error_str[:100]}"
            break
    # 修复：失败前检查文件是否已落盘
    # 场景1: pyrogram 已将文件写入 temp 但在返回前抛了异常
    if temp_file_name and os.path.exists(temp_file_name):
        temp_size = os.path.getsize(temp_file_name)
        if media_size > 0 and temp_size >= media_size:
            try:
                _move_to_download_path(temp_file_name, file_name)
                logger.info(f"Message[{message.id}] {ui_file_name}: 下载实际已完成(temp {temp_size}字节)")
                return DownloadStatus.SkipDownload, file_name, ""
            except Exception as e:
                logger.warning(f"Message[{message.id}]: 移动已完成文件失败: {e}")
    _cleanup_temp_file(temp_file_name)
    # 场景2: 目标文件已存在（可能被并发任务或之前的成功下载写入）
    if file_name and _is_exist(file_name):
        file_size = os.path.getsize(file_name)
        if media_size > 0 and file_size >= media_size:
            logger.info(f"Message[{message.id}] {ui_file_name}: 文件已存在({file_size}字节)，标记为跳过")
            return DownloadStatus.SkipDownload, None, ""
    # Log the specific failure reason before returning
    final_reason = error_message or "下载失败（未知原因）"
    logger.warning(f"Message[{message.id}] {ui_file_name}: download failed after 3 retries, reason: {final_reason}")
    return DownloadStatus.FailedDownload, None, final_reason


def _load_config():
    """Load config"""
    app.load_config()


def _check_config() -> bool:
    """Check config"""
    print_meta(logger)
    try:
        _load_config()

        logger.add(
            os.path.join(app.log_file_path, "tdl.log"),
            rotation="10 MB",
            retention="30 days",
            level=app.log_level,
        )

        logger.add(
            os.path.join(app.log_file_path, "download.log"),
            rotation="10 MB",
            retention="30 days",
            level=app.log_level,
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {name}:{function}:{line} - {message}",
        )

        load_downloads()
    except Exception as e:
        logger.exception(f"load config error: {e}")
        return False
    return True


async def worker(client: pyrogram.client.Client):
    """Work for download task

    进度心跳机制：download_task 在独立 Task 中执行，watchdog 每 30s 检查
    _task_heartbeat。如果某任务超过 _TASK_HEARTBEAT_TIMEOUT(300s) 没有任何
    Pyrogram 进度回调，说明连接已死（不是慢），cancel 该任务释放 worker。
    慢下载（有进度回调）不受影响。
    """
    from module.download_stat import (
        _TASK_HEARTBEAT_TIMEOUT, get_task_heartbeat_age, clear_task_heartbeat,
    )
    while app.is_running:
        try:
            logger.info(f"Worker waiting for queue item...")
            item = await queue.get()
            message = item[0]
            node: TaskNode = item[1]
            logger.info(f"Worker picked up message {message.id} from chat {node.chat_id} for task {node.task_id_display}")
            # Mark task as actively downloading (no longer pending/in-queue)
            if node.task_id:
                from module.bot import _bot
                _bot._in_queue.discard(node.task_id)
                update_download_state(node.task_id, "downloading")
            if node.is_stop_transmission:
                continue

            target_client = node.client if node.client else client
            composite_key = f"{node.chat_id}_{message.id}"

            # 用 Task 包裹 download_task，配合心跳 watchdog 检测死连接
            dl_task = asyncio.create_task(
                download_task(target_client, message, node)
            )
            dl_task_start = time.time()
            _MAX_TASK_RUNTIME = 1800  # 30分钟最大运行时间（心跳从未设置时的后备超时）
            watchdog_triggered = False
            try:
                while not dl_task.done():
                    await asyncio.sleep(30)  # 每 30s 检查一次心跳
                    if dl_task.done():
                        break
                    age = get_task_heartbeat_age(composite_key)
                    runtime = time.time() - dl_task_start
                    if age > _TASK_HEARTBEAT_TIMEOUT or (age < 0 and runtime > _MAX_TASK_RUNTIME):
                        logger.error(
                            f"Worker: task {node.task_id_display} (msg {message.id}) "
                            f"no progress for {int(age)}s (>{_TASK_HEARTBEAT_TIMEOUT}s), "
                            f"cancelling — likely dead TCP connection"
                        )
                        dl_task.cancel()
                        watchdog_triggered = True
                        break
                # 等待 dl_task 完成（正常结束或 cancel）
                await dl_task
            except asyncio.CancelledError:
                # dl_task 被 cancel 时 await 会抛 CancelledError
                if watchdog_triggered:
                    logger.warning(
                        f"Worker: task {node.task_id_display} cancelled by heartbeat watchdog"
                    )
                    # 心跳超时说明 TCP 连接已死，递增错误计数并触发重连
                    # 不走 download_media 的 except handler（cancel 是外部杀的），
                    # 所以必须在这里手动触发，否则重连永远不会被激活
                    _client_conn_errors["count"] += 3
                    asyncio.create_task(_maybe_reconnect_client())
                    # watchdog cancel 跳过了 download_media 的 except handler，
                    # 必须在这里清理任务，否则永久卡在 downloading
                    try:
                        from module.task_store import complete_task as _wct
                        if node and node.task_id:
                            _wct(node.task_id)
                            logger.info(f"Worker: force-completed task {node.task_id_display} after watchdog cancel")
                    except Exception:
                        pass
                else:
                    raise
            finally:
                clear_task_heartbeat(composite_key)
        except Exception as e:
            logger.exception(f"Worker exception for task {getattr(node, 'task_id_display', '?')}: {e}")
            # 防止幽灵任务：任何异常退出都必须清理任务状态，
            # 否则 download_state 永远 "downloading"，并发守卫永久阻塞
            try:
                from module.task_store import complete_task as _ct
                if node and node.task_id:
                    _ct(node.task_id)
                    logger.info(f"Worker: force-completed task {node.task_id_display} after exception")
            except Exception:
                pass


async def download_chat_task(client: pyrogram.Client, chat_download_config: ChatDownloadConfig, node: TaskNode):
    """Download all task"""
    messages_iter = get_chat_history_v2(
        client, node.chat_id, limit=node.limit,
        max_id=node.end_offset_id, offset_id=chat_download_config.last_read_message_id, reverse=True,
    )
    chat_download_config.node = node
    if chat_download_config.ids_to_retry:
        logger.info(f"{_t('Downloading files failed during last run')}...")
        try:
            skipped_messages: list = await asyncio.wait_for(
                client.get_messages(
                    chat_id=node.chat_id, message_ids=chat_download_config.ids_to_retry
                ),
                timeout=120,  # 批量获取加 120s 超时，防止半死 TCP 上 hang 15分钟
            )
        except asyncio.TimeoutError:
            logger.error(
                f"download_chat_task: get_messages timeout (120s) for {len(chat_download_config.ids_to_retry)} "
                f"retry messages in chat {node.chat_id}, skipping retry this run"
            )
            skipped_messages = []
        for message in skipped_messages:
            await add_download_task(message, node)
    async for message in messages_iter:
        # 让出控制权，避免阻塞 handler
        await asyncio.sleep(0)
        
        # Cache chat title from message
        if message and message.chat:
            chat_title = getattr(message.chat, 'title', None) or getattr(message.chat, 'first_name', None)
            if chat_title:
                set_chat_title(message.chat.id, chat_title)
        meta_data = MetaData()
        caption = message.caption
        if caption:
            caption = validate_title(caption)
            app.set_caption_name(node.chat_id, message.media_group_id, caption)
            app.set_caption_entities(node.chat_id, message.media_group_id, message.caption_entities)
        else:
            caption = app.get_caption_name(node.chat_id, message.media_group_id)
        set_meta_data(meta_data, message, caption)
        if app.need_skip_message(chat_download_config, message.id):
            continue
        if app.exec_filter(chat_download_config, meta_data):
            if message.media:  # Only add to download queue if message has media
                await add_download_task(message, node)
        else:
            node.download_status[message.id] = DownloadStatus.SkipDownload
            if message.media_group_id:
                await upload_telegram_chat(client, node.upload_user, app, node, message, DownloadStatus.SkipDownload)
        # Update task progress for crash recovery
        update_task_progress(node.task_id, message.id)
        # 降低 last_read_message_id 更新频率：每 200 条消息持久化一次
        # 这样崩溃时最多重复扫描 200 条，而不是每条都更新
        chat_download_config.last_read_message_id = max(
            chat_download_config.last_read_message_id, message.id
        )
        if message.id % 200 == 0:
            app.update_config(immediate=True)
    # 扫描结束后保存最终位置，确保重启时从正确位置继续
    chat_download_config.need_check = True
    chat_download_config.total_task = node.total_task
    node.is_running = True
    app.update_config(immediate=True)


async def download_all_chat(client: pyrogram.Client):
    """Download All chat"""
    from module.task_store import save_task as _save_task
    for key, value in app.chat_download_config.items():
        value.node = TaskNode(chat_id=key)
        _save_task(
            task_id=value.node.task_id,
            chat_id=key,
            url="",
            start_offset_id=value.last_read_message_id,
            end_offset_id=0,
            limit=0,
            download_filter=value.download_filter,
            from_user_id=0,
            task_type="config",
        )
        try:
            await download_chat_task(client, value, value.node)
        except Exception as e:
            logger.warning(f"Download {key} error: {e}")
        finally:
                    value.need_check = True
                    from module.task_store import complete_task
                    complete_task(value.node.task_id)


async def run_until_all_task_finish():
    """Normal download"""
    while True:
        finish = all(value.need_check and value.total_task == value.finish_task for _, value in app.chat_download_config.items())
        if (not app.bot_token and finish) or app.restart_program:
            break
        await asyncio.sleep(1)


def _exec_loop():
    """Exec loop"""
    app.loop.run_until_complete(run_until_all_task_finish())


async def start_server(client: pyrogram.Client):
    """Start the server"""
    _main_client_ref["client"] = client
    await client.start()


async def _reconnect_client():
    """Force-reconnect the main Pyrogram client to recover from half-dead TCP sessions.

    Mirrors the manual fix of toggling the v2rayA node: client.stop() kills
    the stale TCP connection, client.start() builds a fresh one using the
    existing session file (no re-auth needed).

    所有操作都加 asyncio.wait_for 超时，防止 stop()/start() 在半死 TCP 上 hang
    15分钟（TCP.TIMEOUT=900s）导致 _client_reconnecting["active"] 永久锁死。
    """
    client = _main_client_ref["client"]
    if client is None:
        logger.error("Cannot reconnect: no client reference")
        return False

    _client_reconnecting["active"] = True
    try:
        # stop() 加 30s 超时 — 半死 TCP 上 stop() 会等 TCP.TIMEOUT(900s)
        logger.warning("Client auto-reconnect: stop()...")
        try:
            await asyncio.wait_for(client.stop(), timeout=30)
        except asyncio.TimeoutError:
            logger.error("client.stop() timed out after 30s, force disconnect")
            # stop() = terminate() + disconnect()，超时说明某一步 hang 了
            # 手动清理 Pyrogram 内部状态，否则 start() 会报 "already connected"
            try:
                await client.disconnect()
            except Exception:
                pass
            # 强制重置连接标志 — disconnect() 可能因 is_initialized=True 而失败，
            # 但我们需要让 start()→connect() 能重新建立连接
            client.is_connected = False
        except Exception as e:
            logger.warning(f"client.stop() during reconnect failed (continuing): {e}")
            client.is_connected = False

        # start() 加 30s 超时 — 防止 start() 在未清理干净的 session 上 hang
        logger.warning("Client auto-reconnect: start()...")
        try:
            await asyncio.wait_for(client.start(), timeout=30)
            logger.success("Client reconnected successfully — fresh TCP session established")
            _client_conn_errors["count"] = 0
            return True
        except asyncio.TimeoutError:
            logger.error("client.start() timed out after 30s, reconnect failed")
            return False
        except Exception as e:
            logger.error(f"client.start() during reconnect failed: {e}")
            return False
    finally:
        _client_reconnecting["active"] = False


async def _maybe_reconnect_client():
    """Check if consecutive connection errors warrant a client reconnect.

    Called from download_media error handlers. Returns True if reconnect was
    triggered (caller should abort current download), False otherwise.
    """
    if _client_conn_errors["count"] < _CLIENT_RECONNECT_THRESHOLD:
        return False

    if _client_reconnecting["active"]:
        logger.debug("Reconnect already in progress, skipping")
        return True

    now = time.time()
    if now - _client_last_reconnect["time"] < _CLIENT_RECONNECT_COOLDOWN:
        logger.debug(
            f"Reconnect cooldown active ({int(_CLIENT_RECONNECT_COOLDOWN - (now - _client_last_reconnect['time']))}s remaining), skipping"
        )
        return True

    logger.warning(
        f"Client connection errors reached {_client_conn_errors['count']} "
        f"(threshold {_CLIENT_RECONNECT_THRESHOLD}) — triggering auto-reconnect"
    )
    _client_last_reconnect["time"] = now
    success = await _reconnect_client()
    if not success:
        # Reconnect failed — retry sooner (60s instead of full cooldown)
        _client_last_reconnect["time"] = now - _CLIENT_RECONNECT_COOLDOWN + 60
    return True


async def stop_server(client: pyrogram.Client):
    """Stop the server"""
    await client.stop()


def main():
    """Main function"""
    # ── Patch A: Increase TCP timeout to 900s (15 min) ──
    # Pyrogram default TCP.TIMEOUT=10s causes reconnect storms when TG throttles
    # downloads — each chunk request waits >10s for a response, triggering
    # connection teardown + MTProto re-handshake, producing massive upload traffic.
    # Xray proxy has no idle timeout, so 900s is safe.
    from pyrogram.connection.transport.tcp import TCP as _TCP
    _TCP.TIMEOUT = 900
    logger.info(f"Patched TCP.TIMEOUT: 10s -> {_TCP.TIMEOUT}s")

    # ── Patch B: Swallow ChannelInvalid in Message._parse ──
    # When a monitored channel has messages that reply to messages in another
    # channel the user account can't access, Pyrogram's Message._parse tries
    # to fetch reply_to_message via client.get_messages → resolve_peer →
    # channels.GetChannels, which throws ChannelInvalid. This kills the entire
    # message_parser call in the dispatcher, so the update is silently dropped
    # and our NewMessage handler never fires — the download never starts.
    # Fix: wrap Message._parse so that on ChannelInvalid, we retry with a
    # client wrapper that returns None for inaccessible reply targets.
    from pyrogram.types import Message as _PMsg
    try:
        from pyrogram.errors import ChannelInvalid as _ChannelInvalidErr
    except ImportError:
        # Fallback for forks where ChannelInvalid is under subpackage
        from pyrogram.errors.exceptions.bad_request_400 import ChannelInvalid as _ChannelInvalidErr

    _orig_msg_parse = _PMsg._parse

    class _SafeReplyClient:
        """Pass-through client that swallows ChannelInvalid in reply fetches."""
        def __init__(self, client):
            self._client = client

        async def get_messages(self, *a, **kw):
            try:
                return await self._client.get_messages(*a, **kw)
            except _ChannelInvalidErr:
                return None

        async def get_stories(self, *a, **kw):
            try:
                return await self._client.get_stories(*a, **kw)
            except _ChannelInvalidErr:
                return None

        async def get_forum_topics_by_id(self, *a, **kw):
            try:
                return await self._client.get_forum_topics_by_id(*a, **kw)
            except _ChannelInvalidErr:
                return None

        def __getattr__(self, name):
            return getattr(self._client, name)

    async def _safe_msg_parse(client, *args, **kwargs):
        try:
            return await _orig_msg_parse(client, *args, **kwargs)
        except _ChannelInvalidErr:
            logger.warning(
                "ChannelInvalid in Message._parse — "
                "retrying without reply_to_message"
            )
            return await _orig_msg_parse(
                _SafeReplyClient(client), *args, **kwargs
            )

    _PMsg._parse = _safe_msg_parse
    logger.info("Patched Message._parse: ChannelInvalid no longer drops updates")

    tasks = []
    client = HookClient(
        "media_downloader", api_id=app.api_id, api_hash=app.api_hash,
        proxy=app.proxy, workdir=app.session_file_path,
        start_timeout=app.start_timeout, no_updates=False,
    )
    try:
        app.pre_run()
        _cleanup_stale_temp_files()
        init_web(app)
        set_max_concurrent_transmissions(client, app.max_concurrent_transmissions)
        app.loop.run_until_complete(start_server(client))
        # Create queue AFTER event loop is running to ensure it binds to the
        # correct loop. Creating asyncio.Queue at module import time (before
        # loop starts) can cause put/get to use different loop references,
        # resulting in workers never receiving items.
        global queue
        queue = asyncio.Queue()
        logger.success(_t("Successfully started (Press Ctrl+C to stop)"))
        app.loop.create_task(download_all_chat(client))
        # Always start 6 workers (max). Consumer guards actual concurrency
        # via app.max_download_task. Idle workers block on queue.get() — zero cost.
        _MAX_WORKERS = 6
        for _ in range(_MAX_WORKERS):
            tasks.append(app.loop.create_task(worker(client)))
        if app.bot_token:
            app.loop.run_until_complete(start_download_bot(app, client, add_download_task, download_chat_task))
        _exec_loop()
    except KeyboardInterrupt:
        logger.info(_t("KeyboardInterrupt"))
    except Exception as e:
        logger.exception("{}", e)
    finally:
        app.is_running = False
        save_downloads()
        if app.bot_token:
            app.loop.run_until_complete(stop_download_bot())
        app.loop.run_until_complete(stop_server(client))
        for task in tasks:
            task.cancel()
        logger.info(_t("Stopped!"))
        logger.info(f"{_t('update config')}......")
        app.update_config()
        logger.success(
            f"{_t('Updated last read message_id to config file')},"
            f"{_t('total download')} {app.total_download_task}, "
            f"{_t('total upload file')} {app.cloud_drive_config.total_upload_success_file_count}"
        )


if __name__ == "__main__":
    if _check_config():
        main()