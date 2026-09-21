"""web ui for media download"""

import datetime
import logging
import os
import threading

from flask import Flask, jsonify, render_template, request

import utils
from module.app import Application
from module.task_store import get_pending_tasks, remove_task
from module.download_stat import (
    DownloadState,
    batch_delete_failed,
    batch_delete_tasks,
    delete_task,
    get_chat_title,
    get_download_result,
    get_download_state,
    get_failed_downloads,
    get_total_download_speed,
    is_task_paused,
    pause_task,
    remove_failed_download,
    resume_task,
    set_download_state,
)
from utils.format import format_byte
import asyncio

log = logging.getLogger("werkzeug")
log.setLevel(logging.ERROR)

_flask_app = Flask(__name__, static_folder='static', static_url_path='/static')
_flask_app.secret_key = "tdl"
# Always reload templates from disk on each request — needed for NAS deployments
# where index.html is updated by the deploy script without restarting the container.
_flask_app.config["TEMPLATES_AUTO_RELOAD"] = True
# Disable response caching for index.html so browser/proxy don't serve stale versions
# after a deploy.
_flask_app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0
# Force Jinja to not cache (belt-and-suspenders: ensure auto-reload really works)
_flask_app.jinja_env.auto_reload = True
_flask_app.jinja_env.cache = {}


def get_flask_app() -> Flask:
    """get flask app instance"""
    return _flask_app


def run_web_server(app: Application):
    """
    Runs a web server using the Flask framework.
    """
    get_flask_app().run(
        app.web_host, app.web_port, debug=app.debug_web, use_reloader=False
    )


# pylint: disable = W0603
_app: Application = None


def init_web(app: Application):
    """
    Initialize and start the web server.

    Args:
        app: The Application instance.

    Returns:
        None.
    """
    global _app
    _app = app
    # Load download history into memory so WebUI shows completed tasks
    from module.download_stat import load_downloads
    load_downloads()
    logger = logging.getLogger("web.init")
    logger.info("download_history loaded into memory")
    if app.debug_web:
        threading.Thread(target=run_web_server, args=(app,)).start()
    else:
        threading.Thread(
            target=get_flask_app().run, daemon=True, args=(app.web_host, app.web_port)
        ).start()


@_flask_app.route("/")
def index():
    """Index page - no login required"""
    return render_template("index.html")


@_flask_app.route("/get_completed_count")
def get_completed_count():
    """Lightweight endpoint: return only the total count of completed downloads.
    Used for polling to detect new completions without fetching full data."""
    count = 0
    for messages in get_download_result().values():
        for value in messages.values():
            if value["down_byte"] == value["total_size"] and value["total_size"] > 0:
                count += 1
    return jsonify(total=count)


@_flask_app.route("/get_download_status")
def get_download_speed():
    """Get download speed"""
    return jsonify(
        download_speed=format_byte(get_total_download_speed()) + "/s",
        upload_speed="0.00 B/s"
    )


@_flask_app.route("/get_flood_wait")
def get_flood_wait():
    """Get unified FloodWait cooldown status for WebUI display."""
    from module.pyrogram_extension import is_flood_wait_active, get_flood_wait_remaining, _unified_flood_wait
    from module.download_stat import _throttle_state
    import time as _now
    now = _now.time()
    # throttle active 直接看 notified — 只要没解除限速就一直显示
    # docker 重启后 _throttle_state 重置为默认值，自动清除
    throttle_active = _throttle_state["notified"]
    throttle_elapsed = int(now - _throttle_state["since"]) if _throttle_state["since"] > 0 else 0
    return jsonify(
        active=is_flood_wait_active(),
        remaining=int(get_flood_wait_remaining()),
        reason=_unified_flood_wait.get("reason", ""),
        throttle={
            "active": throttle_active,
            "elapsed": throttle_elapsed,
        }
    )


@_flask_app.route("/get_download_state")
def web_get_download_state():
    """Get current download state"""
    state = get_download_state()
    if state is DownloadState.Downloading:
        return "downloading"
    return "paused"


@_flask_app.route("/set_download_state", methods=["POST"])
def web_set_download_state():
    """Set download state"""
    state = request.args.get("state")

    if state == "continue" and get_download_state() is DownloadState.StopDownload:
        set_download_state(DownloadState.Downloading)

    if state == "pause" and get_download_state() is DownloadState.Downloading:
        set_download_state(DownloadState.StopDownload)

    # Always return action based on actual current state
    return "pause" if get_download_state() is DownloadState.Downloading else "continue"


@_flask_app.route("/get_app_version")
def get_app_version():
    """Get telegram_media_downloader version"""
    return utils.__version__


@_flask_app.route("/get_max_workers")
def get_max_workers():
    """Get current max download task count"""
    return jsonify(max_workers=_app.max_download_task)


@_flask_app.route("/set_max_workers", methods=["POST"])
def set_max_workers():
    """Set max download task count (1-6, hot-applies to the live worker pool)"""
    n = request.args.get("n", type=int)
    if n is None or n < 1 or n > 6:
        return jsonify(error="n must be 1-6"), 400
    _app.max_download_task = n
    # max_concurrent_transmissions follows max_download_task * 5
    _app.max_concurrent_transmissions = n * 5
    # Apply to pyrogram client if available
    from module.pyrogram_extension import set_max_concurrent_transmissions
    try:
        from module.bot import _bot
        if _bot and _bot.client:
            set_max_concurrent_transmissions(_bot.client, n * 5)
    except Exception:
        pass
    # 热生效：动态伸缩 worker 池（9-21）
    try:
        import media_downloader as _md
        _md._worker_pool_set_target(n)
    except Exception as e:
        logger.warning(f"set_max_workers: worker pool adjust failed: {e}")
    # Persist to config file
    _app.update_config(True)
    return jsonify(max_workers=n)


@_flask_app.route("/get_download_list")
def get_download_list():
    """Get download list with task_id and status"""
    from module.download_stat import get_download_result
    # Removed: load_downloads() on empty result — it overwrites runtime data
    # with stale disk data when all downloads happen to be between states.
    # load_downloads() is already called at startup; trust the in-memory state.
    if request.args.get("already_down") is None:
        return "[]"

    already_down = request.args.get("already_down") == "true"

    # Pagination support for completed downloads
    offset = request.args.get("offset", 0, type=int)
    limit = request.args.get("limit", 0, type=int)
    # Server-side search for completed downloads
    search = request.args.get("search", "").strip().lower()

    download_result = get_download_result()
    result = []
    for chat_id, messages in download_result.items():
        for idx, value in messages.items():
            is_already_down = value["down_byte"] == value["total_size"] and value["total_size"] > 0

            if already_down and not is_already_down:
                continue
            if not already_down:
                # Hide placeholder entries (total_size==0, down_byte==0) — these
                # are created by consumer for pending tasks but aren't actively
                # downloading yet. Only show tasks with real progress data.
                if value.get("total_size", 0) == 0 and value.get("down_byte", 0) == 0:
                    continue

            progress = value.get("progress", 0)
            if not progress and value["total_size"] > 0:
                progress = round(value["down_byte"] / value["total_size"] * 100, 1)

            # Staleness check: if speed hasn't been updated in 3s (no Pyrogram callback), show 0
            import time as _now
            raw_speed = value["download_speed"]
            if raw_speed > 0 and not is_already_down:
                last_update = value.get("end_time", 0)
                if (_now.time() - last_update) > 3.0:
                    raw_speed = 0
            download_speed = format_byte(raw_speed) + "/s"

            # ETA calculation (use stale-checked speed)
            eta = ""
            speed = raw_speed
            if speed > 0 and not is_already_down:
                remaining = value["total_size"] - value["down_byte"]
                eta_seconds = int(remaining / speed)
                if eta_seconds >= 3600:
                    eta = f"{eta_seconds // 3600}h{(eta_seconds % 3600) // 60:02d}m"
                elif eta_seconds >= 60:
                    eta = f"{eta_seconds // 60}m{eta_seconds % 60:02d}s"
                else:
                    eta = f"{eta_seconds}s"

            # Internal key for operations (stable across restarts)
            task_id = f"{chat_id}_{idx}"

            # Determine status: completed, paused, waiting, or active
            if is_already_down:
                status = "completed"
            elif is_task_paused(task_id) or is_task_paused(value.get("task_id", "")):
                status = "paused"
            elif value.get("total_size", 0) == 0 and value.get("down_byte", 0) == 0:
                status = "waiting"
            else:
                status = "active"

            # Phase: 区分 placeholder（消息数据获取中）和真正在下载
            # total_size<=1 且 down_byte==0 = consumer 刚创建的占位条目
            # Pyrogram 第一次回调就会覆盖为真实 total_size
            if status == "active" and value.get("total_size", 0) <= 1 and value.get("down_byte", 0) == 0:
                phase = "fetching"
            else:
                phase = "downloading"

            # Get chat title from cache
            source_title = value.get("source_chat_title", "")
            if not source_title and value.get("source_chat_id"):
                source_title = get_chat_title(value["source_chat_id"]) or ""
            chat_title = source_title or get_chat_title(chat_id)

            # Format times
            end_time = value.get("end_time", 0)
            start_time = value.get("start_time", 0)
            completed_time = ""
            created_at = ""
            if start_time:
                created_at = datetime.datetime.fromtimestamp(start_time).strftime("%m-%d %H:%M:%S")
            if is_already_down:
                ts = end_time if end_time else start_time
                if ts:
                    completed_time = datetime.datetime.fromtimestamp(ts).strftime("%m-%d %H:%M:%S")

            # Server-side search filter (by task_id or filename)
            if search:
                task_id_display = value.get("task_id_display", "") or task_id
                searchable = f"{task_id} {task_id_display} {os.path.basename(value['file_name'])}".lower()
                if search not in searchable:
                    continue

            result.append({
                "task_id": str(task_id),
                "chat": str(chat_id),
                "chat_title": chat_title,
                "id": str(idx),
                "filename": os.path.basename(value["file_name"]) if value.get("file_name") else f"msg_{idx} (等待下载)",
                "total_size": format_byte(value["total_size"]) if value["total_size"] > 0 else "未知",
                "total_size_bytes": value["total_size"],
                "download_progress": str(progress),
                "download_progress_raw": progress,
                "download_speed": download_speed,
                "eta": eta,
                "save_path": value["file_name"].replace("\\", "/"),
                "status": status,
                "phase": phase,
                "start_time": start_time,
                "end_time": end_time,
                "created_at": created_at,
                "completed_time": completed_time,
                "task_id_display": value.get("task_id_display", "") or task_id,
            })

    # Sort by time: active by start_time (newest first), completed by end_time (newest first)
    if already_down:
        result.sort(key=lambda x: x.get("end_time", 0) or x.get("start_time", 0), reverse=True)
    else:
        result.sort(key=lambda x: x.get("start_time", 0), reverse=True)

    total = len(result)

    # Apply pagination slice (for completed downloads with limit > 0)
    if already_down and limit > 0:
        result = result[offset:offset + limit]

    return jsonify(result) if not already_down else jsonify({
        "tasks": result,
        "total": total,
        "offset": offset,
        "limit": limit,
    })


@_flask_app.route("/get_failed_downloads")
def web_get_failed_downloads():
    """Get list of failed downloads"""
    failed = get_failed_downloads()
    # Sort by failure timestamp, newest first
    failed = sorted(failed, key=lambda f: f.get("timestamp", 0), reverse=True)
    result = []
    for f in failed:
        chat_id = f.get("chat_id", "")
        chat_title = get_chat_title(chat_id)
        failed_time = ""
        ts = f.get("timestamp", 0)
        if ts:
            failed_time = datetime.datetime.fromtimestamp(ts).strftime("%m-%d %H:%M:%S")
        result.append({
            "task_id": str(f["task_id"]),
            "chat": str(chat_id),
            "chat_title": chat_title,
            "id": str(f.get("msg_id", "")),
            "filename": os.path.basename(f.get("file_name", "")),
            "error_message": f.get("error_message", "Unknown error"),
            "total_size": format_byte(f.get("total_size", 0)),
            "failed_time": failed_time,
            "source_link": f.get("source_link", ""),
            "from_user_id": f.get("from_user_id", "") or "",
        })
    return jsonify(result)


@_flask_app.route("/pause_task", methods=["POST"])
def web_pause_task():
    """Pause a specific download task"""
    task_id = request.args.get("task_id")
    if not task_id:
        return jsonify({"code": "0", "message": "task_id required"})
    pause_task(task_id)
    return jsonify({"code": "1", "message": "paused"})


@_flask_app.route("/resume_task", methods=["POST"])
def web_resume_task():
    """Resume a specific download task"""
    task_id = request.args.get("task_id")
    if not task_id:
        return jsonify({"code": "0", "message": "task_id required"})
    success = resume_task(task_id)
    if success:
        return jsonify({"code": "1", "message": "resumed"})
    return jsonify({"code": "0", "message": "task not paused"})


@_flask_app.route("/check_file_exists", methods=["POST"])
def web_check_file_exists():
    """Check if the local file for a completed task exists.
    Returns: {code, exists, path, filename}"""
    task_id = request.args.get("task_id")
    if not task_id:
        return jsonify({"code": "0", "message": "task_id required"})

    file_path = None
    download_result = get_download_result()
    for chat_id, messages in download_result.items():
        for msg_id, value in messages.items():
            composite_key = f"{chat_id}_{msg_id}"
            if composite_key == str(task_id) or str(value.get("task_id", "")) == str(task_id):
                file_path = value.get("file_name", "")
                break
        if file_path:
            break

    if not file_path:
        return jsonify({"code": "1", "exists": False, "path": "", "filename": ""})

    exists = os.path.isfile(file_path)
    return jsonify({
        "code": "1",
        "exists": exists,
        "path": file_path.replace("\\", "/"),
        "filename": os.path.basename(file_path),
    })


@_flask_app.route("/delete_task", methods=["POST"])
def web_delete_task():
    """Delete a specific download task. If delete_file=true, also remove the local file."""
    from module.pyrogram_extension import remove_download_cache
    task_id = request.args.get("task_id")
    if not task_id:
        return jsonify({"code": "0", "message": "task_id required"})
    delete_file = request.args.get("delete_file", "false").lower() == "true"

    # Find file path before deleting (needed if delete_file=true)
    file_path = None
    download_result = get_download_result()
    for chat_id, messages in download_result.items():
        for msg_id, value in messages.items():
            composite_key = f"{chat_id}_{msg_id}"
            if composite_key == str(task_id) or str(value.get("task_id", "")) == str(task_id):
                file_path = value.get("file_name", "")
                remove_download_cache(chat_id, msg_id)

    # Delete local file if requested
    file_deleted = False
    file_error = ""
    if delete_file and file_path:
        if os.path.isfile(file_path):
            try:
                os.remove(file_path)
                file_deleted = True
            except Exception as e:
                file_error = str(e)
        else:
            file_error = "file not found"

    # Try active downloads first, then failed
    success = delete_task(task_id)
    if not success:
        success = remove_failed_download(task_id)
    if success:
        return jsonify({
            "code": "1",
            "message": "deleted",
            "file_deleted": file_deleted,
            "file_error": file_error,
        })
    return jsonify({"code": "0", "message": "task not found"})


@_flask_app.route("/batch_delete", methods=["POST"])
def web_batch_delete():
    """Batch delete tasks by task_ids. If delete_file=true in JSON body, also remove local files."""
    data = request.get_json(silent=True)
    if not data or "task_ids" not in data:
        return jsonify({"code": "0", "message": "task_ids required"})

    task_ids = data["task_ids"]
    if not isinstance(task_ids, list):
        return jsonify({"code": "0", "message": "task_ids must be a list"})

    delete_file = data.get("delete_file", False)

    # Collect file paths before deletion if delete_file is requested
    files_deleted = 0
    files_not_found = 0
    if delete_file:
        download_result = get_download_result()
        for tid in task_ids:
            file_path = None
            for chat_id, messages in download_result.items():
                for msg_id, value in messages.items():
                    composite_key = f"{chat_id}_{msg_id}"
                    if composite_key == str(tid) or str(value.get("task_id", "")) == str(tid):
                        file_path = value.get("file_name", "")
                        break
                if file_path:
                    break
            if file_path:
                if os.path.isfile(file_path):
                    try:
                        os.remove(file_path)
                        files_deleted += 1
                    except Exception:
                        files_not_found += 1
                else:
                    files_not_found += 1

    deleted_active = batch_delete_tasks(task_ids)
    deleted_failed = batch_delete_failed(task_ids)
    total = deleted_active + deleted_failed

    return jsonify({
        "code": "1",
        "message": f"deleted {total} tasks",
        "deleted": total,
        "files_deleted": files_deleted,
        "files_not_found": files_not_found,
    })


@_flask_app.route("/retry_task", methods=["POST"])
def web_retry_task():
    """Retry a failed download task - re-download the file"""
    task_id = request.args.get("task_id")
    if not task_id:
        return jsonify({"code": "0", "message": "task_id required"})

    # Find the failed entry to get chat_id + msg_id
    failed_list = get_failed_downloads()
    target = None
    for f in failed_list:
        if str(f.get("task_id", "")) == str(task_id):
            target = f
            break

    if not target:
        return jsonify({"code": "0", "message": "task not found in failed list"})

    chat_id = target.get("chat_id")
    msg_id = target.get("msg_id")
    from_user_id = target.get("from_user_id", "") or ""
    source_link = target.get("source_link", "") or ""
    if not chat_id or not msg_id:
        return jsonify({"code": "0", "message": "incomplete task data (missing chat_id/msg_id)"})

    # Remove from failed list first
    remove_failed_download(task_id)

    # Submit async retry to the main event loop
    try:
        if _app and _app.loop:
            asyncio.run_coroutine_threadsafe(
                _async_retry_download(chat_id, msg_id, from_user_id, source_link=source_link, original_task_id=task_id),
                _app.loop,
            )
            return jsonify({"code": "1", "message": "已加入重试队列"})
        return jsonify({"code": "0", "message": "应用尚未初始化完成"})
    except Exception as e:
        return jsonify({"code": "0", "message": f"重试失败: {str(e)}"})


@_flask_app.route("/batch_retry", methods=["POST"])
def web_batch_retry():
    """Batch retry multiple failed downloads"""
    data = request.get_json(silent=True)
    if not data or "task_ids" not in data:
        return jsonify({"code": "0", "message": "task_ids required"})

    task_ids = data["task_ids"]
    if not isinstance(task_ids, list):
        return jsonify({"code": "0", "message": "task_ids must be a list"})

    failed_list = get_failed_downloads()
    queued = 0
    errors = []

    for task_id in task_ids:
        target = None
        for f in failed_list:
            if str(f.get("task_id", "")) == str(task_id):
                target = f
                break

        if not target:
            errors.append(f"{task_id}: not found")
            continue

        chat_id = target.get("chat_id")
        msg_id = target.get("msg_id")
        from_user_id = target.get("from_user_id", "")
        source_link = target.get("source_link", "") or ""
        if not chat_id or not msg_id:
            errors.append(f"{task_id}: incomplete data")
            continue

        # Remove from failed list first
        remove_failed_download(task_id)

        # Immediately insert a placeholder into bot_tasks.json so WebUI sees it
        from module.task_store import save_task as _save_placeholder
        _save_placeholder(
            task_id=f"retry_{task_id}",
            chat_id=int(chat_id) if str(chat_id).lstrip("-").isdigit() else chat_id,
            url="",
            start_offset_id=0,
            end_offset_id=0,
            limit=1,
            download_filter=None,
            from_user_id=from_user_id or (int(chat_id) if str(chat_id).lstrip("-").isdigit() else chat_id),
            task_type="retry",
            extra_data={"source_task_id": task_id, "message_id": msg_id, "pending": True},
        )

        # Submit async retry
        try:
            if _app and _app.loop:
                asyncio.run_coroutine_threadsafe(
                    _async_retry_download(chat_id, msg_id, from_user_id, placeholder_task_id=f"retry_{task_id}", source_link=source_link, original_task_id=task_id),
                    _app.loop,
                )
                queued += 1
            else:
                errors.append(f"{task_id}: app not initialized")
        except Exception as e:
            errors.append(f"{task_id}: {str(e)}")

    return jsonify({
        "code": "1",
        "message": f"已加入重试队列 {queued} 个任务" + (f"，{len(errors)} 个失败" if errors else ""),
        "queued": queued,
        "errors": errors,
    })


@_flask_app.route("/get_pending_list")
def web_get_pending_list():
    """Get list of pending tasks (received but not started downloading)"""
    import time as _time
    pending = get_pending_tasks()
    # Filter out tasks already in _download_result (consumer created placeholder).
    # These will show up in the active download list — no need to duplicate in pending.
    from module.download_stat import get_download_result as _get_dr
    _dr = _get_dr()

    result = []
    for task in pending:
        # 如果该任务已在 _download_result（consumer 创建了 placeholder），
        # 说明即将出现在 Active 列表，从 Pending 排除，避免撞车
        _cid = task.get("chat_id")
        _mid = task.get("extra_data", {}).get("message_id")
        if _cid is not None and _mid is not None:
            try:
                _cid_int = int(_cid)
                _mid_int = int(_mid)
                if _cid_int in _dr and _mid_int in _dr[_cid_int]:
                    continue
            except (ValueError, TypeError):
                pass  # 类型不对就不过滤，保守起见让任务继续显示在 pending

        task_id = task.get("task_id", "")
        created_at = task.get("created_at", 0)
        created_at_str = ""
        wait_time = ""
        if created_at:
            created_at_str = datetime.datetime.fromtimestamp(created_at).strftime("%m-%d %H:%M:%S")
            elapsed = int(_time.time() - created_at)
            h, m = divmod(elapsed // 60, 60)
            wait_time = f"{h}h{m:02d}m" if h > 0 else f"{m}m"

        # Try to get chat title from task data or URL
        chat_title = task.get("extra_data", {}).get("chat_title", "")
        url = task.get("url", "")

        # Determine source type
        source_type = task.get("task_type", "download")
        if source_type == "download":
            source_type = "频道消息"
        elif source_type == "forward":
            source_type = "转发消息"
        else:
            source_type = "消息"

        # Build filename from URL or extra_data
        filename = task.get("extra_data", {}).get("filename", "")
        if not filename and url:
            filename = url.split("/")[-1] if "/" in url else url
        if not filename:
            filename = f"task_{task_id}"

        total_size = task.get("extra_data", {}).get("total_size", "")
        if isinstance(total_size, (int, float)) and total_size > 0:
            total_size = format_byte(total_size)
        elif not total_size:
            total_size = "未知"

        task_id_display = task.get("extra_data", {}).get("task_id_display", str(task_id))

        # All pending tasks show as "等待中" — no more "排队中" state
        queue_label = "等待中"

        result.append({
            "task_id": str(task_id),
            "task_id_display": task_id_display,
            "chat": str(task.get("chat_id", "")),
            "chat_title": chat_title,
            "filename": filename,
            "total_size": total_size,
            "url": url,
            "source_type": source_type,
            "created_at": created_at_str,
            "created_ts": created_at,
            "wait_time": wait_time,
            "queue_label": queue_label,
        })

    # Sort by created_at ascending (earliest first)
    result.sort(key=lambda x: x.get("created_ts", 0))
    return jsonify(result)


@_flask_app.route("/remove_pending", methods=["POST"])
def web_remove_pending():
    """Remove a pending task from the queue"""
    task_id = request.args.get("task_id")
    if not task_id:
        return jsonify({"code": "0", "message": "task_id required"})
    # Try int conversion since task_store uses int task_ids
    try:
        task_id_int = int(task_id)
    except (ValueError, TypeError):
        task_id_int = task_id
    remove_task(task_id_int)
    return jsonify({"code": "1", "message": "removed"})


async def _async_retry_download(chat_id, msg_id, from_user_id="", placeholder_task_id="", source_link="", original_task_id=""):
    """Async helper: fetch the message and re-add to download queue

    Args:
        source_link: If provided, use parse_link → get_messages to get a fresh
                     message copy. This is more reliable than direct get_messages
                     because it resolves the link the same way download_from_link does.
        original_task_id: The original failed task_id, used to restore the failed
                          entry if retry fails. Without this, the task "disappears"
                          from the failed list.
    """
    logger = logging.getLogger("web.retry")

    def _restore_failed(reason):
        """Re-add to failed list if retry can't even start downloading."""
        try:
            from module.download_stat import add_failed_download
            add_failed_download(
                chat_id=chat_id,
                msg_id=msg_id,
                task_id=original_task_id or placeholder_task_id or "",
                file_name="",
                error_message=reason,
                total_size=0,
                source_link=source_link or "",
                from_user_id=str(from_user_id) if from_user_id else "",
            )
        except Exception:
            pass
        # 清理 placeholder 和 task node，避免残留在 bot_tasks.json
        try:
            if placeholder_task_id:
                from module.task_store import remove_task
                remove_task(placeholder_task_id)
        except Exception:
            pass

    try:
        try:
            cid = int(chat_id)
        except (ValueError, TypeError):
            cid = chat_id

        from module.bot import _bot

        client = _bot.client
        if not client:
            logger.error("Retry failed: _bot.client is not available")
            _restore_failed("重试失败: client不可用")
            return

        msg = None

        # Strategy 1: If we have a source_link, use parse_link to get a fresh
        # message. This mirrors the download_from_link path and is more reliable
        # for forwarded messages with stale file references.
        if source_link:
            try:
                from module.pyrogram_extension import parse_link
                link_chat_id, link_msg_id, _ = await parse_link(client, source_link)
                if link_chat_id and link_msg_id:
                    link_msg = await client.get_messages(link_chat_id, link_msg_id)
                    if link_msg and not link_msg.empty:
                        msg = link_msg
                        logger.info(
                            f"Retry: source_link parse succeeded, "
                            f"got msg {msg.id} from chat {link_chat_id}"
                        )
                    else:
                        logger.warning(
                            f"Retry: source_link got empty message for "
                            f"{link_chat_id}/{link_msg_id}, falling back to direct get_messages"
                        )
            except Exception as e:
                logger.warning(
                    f"Retry: source_link parse failed ({source_link}): {e}, "
                    f"falling back to direct get_messages"
                )

        # Strategy 2: Fallback to direct get_messages with original chat_id/msg_id
        if not msg:
            msg = await client.get_messages(cid, int(msg_id))

        if not msg or msg.empty:
            logger.error(f"Retry failed: message {msg_id} not found in chat {cid}")
            _restore_failed("重试失败: 消息不存在或已删除")
            return

        from module.app import TaskNode, TaskType

        node = TaskNode(
            chat_id=cid,
            from_user_id=from_user_id or cid,
            reply_message_id=0,
            replay_message="WebUI retry",
            limit=1,
            bot=_bot.bot,
            task_id=_bot.gen_task_id(),
        )
        _bot.add_task_node(node)

        # Persist to bot_tasks.json so the task survives container restarts
        from module.task_store import save_task as _save_task
        saved_ok = _save_task(
            task_id=node.task_id,
            chat_id=cid,
            url="",
            start_offset_id=0,
            end_offset_id=0,
            limit=1,
            download_filter=None,
            from_user_id=from_user_id or cid,
            task_type="download",
            extra_data={"task_id_display": node.task_id_display, "message_id": msg_id},
        )
        if not saved_ok:
            logger.warning(f"Retry skipped: save_task refused (duplicate) chat={cid} msg={msg_id}")
            return

        # Clean up the placeholder entry created by batch_retry
        if placeholder_task_id:
            try:
                from module.task_store import remove_task as _remove_placeholder
                _remove_placeholder(placeholder_task_id)
            except Exception:
                pass

        # Cache message and trigger consumer — same flow as direct_download
        _bot._cached_messages[node.task_id] = msg
        node.is_running = True

        # Send retry notification to user via bot and record message ID for later editing
        if from_user_id and _bot and _bot.bot:
            try:
                chat_name = getattr(msg.chat, "title", str(cid))
                msg_text = "🔄 重试任务已加入下载队列\n"
                msg_text += f"消息: {msg_id}\n"
                msg_text += f"任务: {node.task_id_display}\n"
                msg_text += f"群组: {chat_name}"
                sent_msg = await _bot.bot.send_message(int(from_user_id), msg_text)
                # 记录消息ID，让 report_bot_status 能编辑这条消息更新进度
                node.reply_message_id = sent_msg.id
            except Exception as e:
                logger.warning(f"Retry notification failed for user {from_user_id}: {e}")

        logger.info(f"Retry: queued message {msg_id} from chat {cid} as task {node.task_id_display}")

        # Do NOT trigger _consume_one_pending directly — let the periodic
        # _pending_consumer_loop (every 2s) pick it up. This ensures the
        # concurrency guard is always respected, preventing over-fill when
        # many retries are submitted simultaneously.
    except Exception as e:
        logger.error(f"Retry failed for chat={chat_id} msg={msg_id}: {e}", exc_info=True)
        _restore_failed(f"重试失败: {e}")
