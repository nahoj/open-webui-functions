"""
title: Auto-Export Chats
author: Johan Grande, agents
repository: https://github.com/nahoj/open-webui-functions
version: 3.0
license: MIT
description: Automatically export chats to Markdown files.

This function creates a background job that exports new and changed chats every 5 minutes by default.
Chats are exported to folders mirroring OWUI's.

The export is opt-in per user with a UserValve (in a chat, Controls > Valves > Functions).

This function also provides 2 action buttons (enabled with Global/model toggle) to
- run the export once;
- stop the current run.

You can leave the buttons disabled if you only want the export to run in the background,
as long as the main toggle for this function is on.
"""

import asyncio
import datetime
import json
import logging
import os
import re
import threading
import uuid
from typing import Any, Dict, List, Optional, Set
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, Field

from open_webui.internal.db import get_db_context
from open_webui.models.chats import Chat, Chats
from open_webui.models.folders import Folder
from open_webui.models.functions import Functions
from open_webui.models.users import Users


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sanitize_filename(text: str) -> str:
    """Removes characters that are invalid for file names."""
    text = text.strip()
    text = _truncate_utf8(text, 100)
    text = "".join(ch if ch.isprintable() else "-" for ch in text)
    text = re.sub(r'[<>:"/\\|?*]', "-", text)
    text = text.replace(" ", "_")
    if not text:
        return "Untitled"
    return text


def _truncate_utf8(string: str, max_bytes: int) -> str:
    """Truncates a UTF-8 string to a maximum number of bytes without creating invalid characters."""
    return string.encode("utf-8")[:max_bytes].decode("utf-8", "ignore")


# ---------------------------------------------------------------------------
# Per-user change-detection snapshots
# ---------------------------------------------------------------------------


class _FolderSnapshot:
    __slots__ = ("updated_at", "name", "parent_id")

    def __init__(self, updated_at: int, name: str, parent_id: Optional[str]):
        self.updated_at = updated_at
        self.name = name
        self.parent_id = parent_id

    def differs(self, other: "_FolderSnapshot") -> bool:
        return (
            self.updated_at != other.updated_at
            or self.name != other.name
            or self.parent_id != other.parent_id
        )


# ---------------------------------------------------------------------------
# Action
# ---------------------------------------------------------------------------


class Action:
    actions = [
        {"id": "run_now", "name": "Run Auto-Export Now"},
        {"id": "stop_current_job", "name": "Stop Current Auto-Export Job"},
    ]
    STATE_FILE_NAME = "_auto_export_state.json"
    FOLDER_ID_FILE_PREFIX = ".open_webui_id="

    class Valves(BaseModel):
        POLL_INTERVAL_SECONDS: int = Field(
            default=300,
            description="How often to check for changes (seconds). Set to 0 to disable background auto-run. Default: 5 minutes.",
        )
        OPEN_WEBUI_BASE_URL: str = Field(
            default="",
            description="Base URL of the Open WebUI instance (e.g., https://your.domain), for Markdown frontmatter link.",
        )
        SAVE_FOLDER: str = Field(
            default="/app/backend/data/Chats",
            description="Root folder where chat Markdown files will be saved.",
        )

    class UserValves(BaseModel):
        ENABLED: bool = Field(
            default=False,
            description="Enable chat auto-export for this user.",
        )

    def __init__(self):
        self.valves = self.Valves()

        # Logging
        self.logger = logging.getLogger("auto_export_chats")
        self.logger.setLevel(logging.DEBUG)

        self._function_id = os.path.basename(os.path.dirname(__file__))
        self._run_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._job_stop_requested = threading.Event()
        self._job_running = threading.Event()
        self._poll_thread = None
        self._start_poll_thread_if_needed()

        self.logger.info(
            f"Auto-Export Chats initialized (polling every {self.valves.POLL_INTERVAL_SECONDS}s). "
            f"Saving to: {os.path.abspath(self.valves.SAVE_FOLDER)}"
        )

    # ------------------------------------------------------------------
    # Polling loop
    # ------------------------------------------------------------------

    def _poll_loop(self):
        while not self._stop_event.is_set():
            self._sync_runtime_valves()
            if not self._is_function_active():
                self.logger.info("Auto-export action disabled; stopping background poller.")
                self._stop_event.set()
                break
            now = datetime.datetime.now(datetime.timezone.utc)
            next_run_at = self._get_next_poll_run_at(now)
            if next_run_at is None:
                self.logger.info("Auto-export background polling disabled; stopping background poller.")
                break
            wait_seconds = max(0.0, (next_run_at - now).total_seconds())
            if self._stop_event.wait(wait_seconds):
                break
            try:
                self._run_export_job()
            except Exception:
                self.logger.error("Polling error", exc_info=True)

    def _make_poll_thread(self) -> threading.Thread:
        return threading.Thread(
            target=self._poll_loop,
            name="auto-export-poller",
            daemon=True,
        )

    def _start_poll_thread_if_needed(self):
        if int(self.valves.POLL_INTERVAL_SECONDS) <= 0:
            return
        if self._poll_thread is not None and self._poll_thread.is_alive():
            return
        self._stop_event = threading.Event()
        self._poll_thread = self._make_poll_thread()
        self._poll_thread.start()

    def _stop_poll_thread_if_running(self):
        if self._poll_thread is not None and self._poll_thread.is_alive():
            self._stop_event.set()

    def _get_next_poll_run_at(self, now: datetime.datetime) -> Optional[datetime.datetime]:
        interval_seconds = max(1, int(self.valves.POLL_INTERVAL_SECONDS))
        if int(self.valves.POLL_INTERVAL_SECONDS) <= 0:
            return None
        if now.tzinfo is None:
            now = now.replace(tzinfo=datetime.timezone.utc)
        else:
            now = now.astimezone(datetime.timezone.utc)
        now_ts = now.timestamp()
        next_run_ts = ((int(now_ts) // interval_seconds) + 1) * interval_seconds
        return datetime.datetime.fromtimestamp(next_run_ts, tz=datetime.timezone.utc)

    def _is_function_active(self) -> bool:
        function = Functions.get_function_by_id(self._function_id)
        return bool(function and function.is_active)

    def _run_export_job(self) -> str:
        if not self._run_lock.acquire(blocking=False):
            return "busy"
        try:
            self._job_stop_requested.clear()
            self._job_running.set()
            job_started_at = datetime.datetime.now(datetime.timezone.utc)
            return self._poll_all_users(job_started_at)
        finally:
            self._job_running.clear()
            self._run_lock.release()

    def _poll_all_users(self, job_started_at: datetime.datetime) -> str:
        users = self._get_enabled_user_ids()
        success = True
        for user_id in users:
            if self._job_stop_requested.is_set():
                self.logger.info("Auto-export job stop requested; stopping before next user.")
                return "stopped"
            user_root_dir = self._get_user_root_dir(user_id)
            last_successful_export_at = self._read_last_successful_export_at(user_root_dir)
            try:
                self._poll_user(user_id, last_successful_export_at, job_started_at, user_root_dir)
            except Exception:
                success = False
                self.logger.error(f"Error polling user {user_id}", exc_info=True)
                continue
            if self._job_stop_requested.is_set():
                self.logger.info(f"Auto-export job stop requested while processing user {user_id}.")
                return "stopped"
            self._write_state(user_root_dir, job_started_at)
        return "success" if success else "failed"

    def _request_stop_current_job(self) -> str:
        if not self._job_running.is_set():
            return "idle"
        self._job_stop_requested.set()
        return "stopping"

    def _get_enabled_user_ids(self) -> Set[str]:
        if not self._function_id:
            return set()

        try:
            result = Users.get_users(limit=None)
            # get_users returns {"users": [...]} dict or possibly a list directly
            if isinstance(result, dict):
                users = result.get("users", [])
            elif isinstance(result, list):
                users = result
            else:
                self.logger.warning("Unexpected return type from Users.get_users: %s", type(result).__name__)
                return set()
        except Exception:
            self.logger.error("Error loading users for auto-export polling", exc_info=True)
            return set()

        enabled_user_ids: Set[str] = set()
        for user in users:
            settings = getattr(user, "settings", None)
            if hasattr(settings, "model_dump"):
                settings = settings.model_dump()
            settings = settings or {}
            if (
                settings.get("functions", {})
                .get("valves", {})
                .get(self._function_id, {})
                .get("ENABLED")
            ):
                enabled_user_ids.add(user.id)
        return enabled_user_ids

    @staticmethod
    def _resolve_function_id(request: Optional[Any]) -> Optional[str]:
        if request is None:
            return None
        action_id = getattr(request, "path_params", {}).get("action_id")
        if not action_id:
            return None
        return action_id.split(".", 1)[0]

    def _sync_runtime_valves(self):
        module_valves = globals().get("valves")
        if isinstance(module_valves, self.Valves):
            self.valves = module_valves
        if int(self.valves.POLL_INTERVAL_SECONDS) <= 0:
            self._stop_poll_thread_if_running()
        else:
            self._start_poll_thread_if_needed()

    def _state_file_path(self, user_root_dir: str) -> str:
        return os.path.join(user_root_dir, self.STATE_FILE_NAME)

    def _read_state(self, user_root_dir: str) -> Dict[str, Any]:
        try:
            with open(self._state_file_path(user_root_dir), "r", encoding="utf-8") as f:
                data = json.load(f)
        except FileNotFoundError:
            return {}
        except Exception:
            self.logger.error("Error reading auto-export state", exc_info=True)
            return {}

        return data if isinstance(data, dict) else {}

    def _read_last_successful_export_at(self, user_root_dir: str) -> Optional[datetime.datetime]:
        data = self._read_state(user_root_dir)

        value = data.get("last_successful_export_at") if isinstance(data, dict) else None
        if not value:
            return None
        try:
            parsed = datetime.datetime.fromisoformat(value)
        except ValueError:
            self.logger.warning("Invalid last_successful_export_at in auto-export state file")
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=datetime.timezone.utc)
        return parsed

    def _write_state(self, user_root_dir: str, job_started_at: datetime.datetime):
        os.makedirs(user_root_dir, exist_ok=True)
        with open(self._state_file_path(user_root_dir), "w", encoding="utf-8") as f:
            json.dump(
                {
                    "last_successful_export_at": job_started_at.isoformat(),
                },
                f,
                indent=2,
            )
            f.write("\n")

    @staticmethod
    async def _emit_status(__event_emitter__, description: str, done: bool = True):
        if __event_emitter__ is not None:
            await __event_emitter__(
                {
                    "type": "status",
                    "data": {
                        "description": description,
                        "done": done,
                    },
                }
            )

    def _poll_user(
        self,
        user_id: str,
        last_successful_export_at: Optional[datetime.datetime],
        job_started_at: datetime.datetime,
        user_root_dir: str,
    ):
        cur_folders = self._query_folders(user_id)
        folder_path_map = self._build_folder_path_map(cur_folders)
        self._cleanup_orphaned_folder_markers(user_root_dir, set(cur_folders.keys()))
        self._reconcile_user_folders(user_root_dir, cur_folders, folder_path_map)

        chats_to_export_ids = self._query_chat_ids_to_export(user_id, last_successful_export_at)
        user_tz = self._get_user_timezone(user_id)
        for chat_id in chats_to_export_ids:
            if self._job_stop_requested.is_set():
                self.logger.info(f"Auto-export job stop requested; stopping user {user_id} before chat {chat_id}.")
                return
            try:
                chat = Chats.get_chat_by_id(chat_id)
                if chat:
                    self._export_chat(chat, folder_path_map, user_root_dir, user_tz)
            except Exception:
                self.logger.error(f"Error exporting chat {chat_id}", exc_info=True)
                raise

        if chats_to_export_ids:
            self.logger.info(
                f"User {user_id}: exported {len(chats_to_export_ids)} since {last_successful_export_at.isoformat() if last_successful_export_at else 'beginning'} "
                f"up to job start {job_started_at.isoformat()}"
            )

    # ------------------------------------------------------------------
    # Lightweight DB queries (no chat JSON blob)
    # ------------------------------------------------------------------

    @staticmethod
    def _query_folders(user_id: str) -> Dict[str, _FolderSnapshot]:
        """Returns {folder_id: _FolderSnapshot}."""
        with get_db_context() as db:
            rows = (
                db.query(Folder.id, Folder.updated_at, Folder.name, Folder.parent_id)
                .filter_by(user_id=user_id)
                .all()
            )
            return {
                fid: _FolderSnapshot(updated_at, name, parent_id)
                for fid, updated_at, name, parent_id in rows
            }

    @staticmethod
    def _query_chat_ids_to_export(
        user_id: str,
        last_successful_export_at: Optional[datetime.datetime],
    ) -> List[str]:
        with get_db_context() as db:
            query = db.query(Chat.id).filter_by(user_id=user_id)
            if last_successful_export_at is not None:
                since_ts = int(last_successful_export_at.timestamp())
                query = query.filter(Chat.updated_at > since_ts)
            return [row[0] for row in query.all()]

    def _cleanup_orphaned_folder_markers(self, user_root_dir: str, valid_folder_ids: Set[str]) -> None:
        """Remove .open_webui_id=* marker files whose folder ID is no longer in the DB."""
        marker_prefix = self.FOLDER_ID_FILE_PREFIX
        for root, _dirs, files in os.walk(user_root_dir):
            for fname in files:
                if fname.startswith(marker_prefix):
                    folder_id = self._parse_folder_id_file_name(fname)
                    if folder_id and folder_id not in valid_folder_ids:
                        marker_path = os.path.join(root, fname)
                        try:
                            os.remove(marker_path)
                            self.logger.info(f"Removed orphaned folder marker: {marker_path}")
                        except OSError:
                            pass

    # ------------------------------------------------------------------
    # Folder path resolution
    # ------------------------------------------------------------------

    @staticmethod
    def _build_folder_path_map(folders: Dict[str, _FolderSnapshot]) -> Dict[str, str]:
        """Returns {folder_id: "Root/Sub/Leaf"} filesystem-safe relative path."""
        cache: Dict[str, str] = {}

        def _resolve(fid: str, depth: int = 0) -> str:
            if fid in cache:
                return cache[fid]
            if depth > 50 or fid not in folders:
                return ""
            snap = folders[fid]
            safe = _sanitize_filename(snap.name)
            if snap.parent_id and snap.parent_id in folders:
                parent_path = _resolve(snap.parent_id, depth + 1)
                path = os.path.join(parent_path, safe) if parent_path else safe
            else:
                path = safe
            cache[fid] = path
            return path

        for fid in folders:
            _resolve(fid)
        return cache

    def _get_user_root_dir(self, user_id: str) -> str:
        user = Users.get_user_by_id(user_id)
        return os.path.join(self.valves.SAVE_FOLDER, _sanitize_filename(user.name))

    @classmethod
    def _folder_id_file_name(cls, folder_id: str) -> str:
        return f"{cls.FOLDER_ID_FILE_PREFIX}{folder_id}"

    @classmethod
    def _parse_folder_id_file_name(cls, file_name: str) -> Optional[str]:
        if not file_name.startswith(cls.FOLDER_ID_FILE_PREFIX):
            return None
        folder_id = file_name[len(cls.FOLDER_ID_FILE_PREFIX) :]
        return folder_id or None

    def _write_folder_id_marker(self, folder_dir: str, folder_id: str):
        os.makedirs(folder_dir, exist_ok=True)
        expected_marker_name = self._folder_id_file_name(folder_id)
        for entry in os.listdir(folder_dir):
            marker_id = self._parse_folder_id_file_name(entry)
            if marker_id is not None and entry != expected_marker_name:
                os.remove(os.path.join(folder_dir, entry))
        marker_path = os.path.join(folder_dir, expected_marker_name)
        if not os.path.exists(marker_path):
            with open(marker_path, "w", encoding="utf-8"):
                pass

    def _read_folder_id_marker(self, folder_dir: str) -> Optional[str]:
        try:
            for entry in os.listdir(folder_dir):
                folder_id = self._parse_folder_id_file_name(entry)
                if folder_id is not None:
                    return folder_id
        except FileNotFoundError:
            return None
        return None

    def _find_folder_dir_by_id(self, user_root_dir: str, folder_id: str) -> Optional[str]:
        if not os.path.isdir(user_root_dir):
            return None
        marker_name = self._folder_id_file_name(folder_id)
        for root, dirs, files in os.walk(user_root_dir):
            if marker_name in files:
                return root
            dirs[:] = [d for d in dirs if not d.startswith(".")]
        return None

    def _iter_managed_folder_ids_under_path(self, path: str) -> Set[str]:
        managed_ids: Set[str] = set()
        if not os.path.exists(path):
            return managed_ids
        for root, dirs, files in os.walk(path):
            for file_name in files:
                folder_id = self._parse_folder_id_file_name(file_name)
                if folder_id is not None:
                    managed_ids.add(folder_id)
            dirs[:] = [d for d in dirs if not d.startswith(".")]
        return managed_ids

    @staticmethod
    def _is_relative_to(path: str, parent: str) -> bool:
        try:
            return os.path.commonpath([os.path.abspath(path), os.path.abspath(parent)]) == os.path.abspath(parent)
        except ValueError:
            return False

    @staticmethod
    def _collect_folder_subtree_ids(folder_id: str, folders: Dict[str, _FolderSnapshot]) -> Set[str]:
        subtree_ids: Set[str] = set()

        def _visit(current_id: str):
            subtree_ids.add(current_id)
            for child_id, snap in folders.items():
                if snap.parent_id == current_id:
                    _visit(child_id)

        _visit(folder_id)
        return subtree_ids

    def _is_folder_processable(
        self,
        folder_id: str,
        actual_dir: str,
        expected_dir: str,
        folders: Dict[str, _FolderSnapshot],
    ) -> bool:
        if self._is_relative_to(expected_dir, actual_dir):
            return False
        subtree_ids = self._collect_folder_subtree_ids(folder_id, folders)
        target_ids = self._iter_managed_folder_ids_under_path(expected_dir)
        return target_ids.issubset(subtree_ids)

    def _merge_directories(self, source_dir: str, target_dir: str):
        os.makedirs(target_dir, exist_ok=True)
        for entry in os.listdir(source_dir):
            source_path = os.path.join(source_dir, entry)
            target_path = os.path.join(target_dir, entry)
            if os.path.isdir(source_path):
                if os.path.isdir(target_path):
                    self._merge_directories(source_path, target_path)
                    if not os.listdir(source_path):
                        os.rmdir(source_path)
                else:
                    os.replace(source_path, target_path)
            else:
                if not os.path.exists(target_path):
                    os.replace(source_path, target_path)
                elif os.path.getmtime(source_path) > os.path.getmtime(target_path):
                    os.replace(source_path, target_path)
                else:
                    os.remove(source_path)
        if os.path.isdir(source_dir) and not os.listdir(source_dir):
            os.rmdir(source_dir)

    def _process_folder_relocation(
        self,
        folder_id: str,
        actual_dir: Optional[str],
        expected_dir: str,
    ):
        if actual_dir is None:
            os.makedirs(expected_dir, exist_ok=True)
            self._write_folder_id_marker(expected_dir, folder_id)
            return
        os.makedirs(os.path.dirname(expected_dir), exist_ok=True)
        if not os.path.exists(expected_dir):
            os.replace(actual_dir, expected_dir)
        elif os.path.abspath(actual_dir) != os.path.abspath(expected_dir):
            self._merge_directories(actual_dir, expected_dir)
        self._write_folder_id_marker(expected_dir, folder_id)

    def _reconcile_user_folders(
        self,
        user_root_dir: str,
        folders: Dict[str, _FolderSnapshot],
        folder_path_map: Dict[str, str],
    ):
        os.makedirs(user_root_dir, exist_ok=True)
        pending_folder_ids = sorted(folder_path_map, key=lambda folder_id: (folder_path_map[folder_id].count(os.sep), folder_path_map[folder_id]))
        while pending_folder_ids:
            processed_any = False
            for folder_id in list(pending_folder_ids):
                expected_dir = os.path.join(user_root_dir, folder_path_map[folder_id])
                marker_id = self._read_folder_id_marker(expected_dir)
                if marker_id == folder_id:
                    self._write_folder_id_marker(expected_dir, folder_id)
                    pending_folder_ids.remove(folder_id)
                    processed_any = True
                    continue
                actual_dir = self._find_folder_dir_by_id(user_root_dir, folder_id)
                if actual_dir is not None and not self._is_folder_processable(folder_id, actual_dir, expected_dir, folders):
                    continue
                self._process_folder_relocation(folder_id, actual_dir, expected_dir)
                pending_folder_ids.remove(folder_id)
                processed_any = True
                break
            if not processed_any:
                raise RuntimeError(f"Could not reconcile folder layout for user root {user_root_dir}")

    # ------------------------------------------------------------------
    # Message extraction from chat history tree
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_current_branch(chat_data: dict) -> List[Dict[str, str]]:
        """Walk from currentId back through parentId to get the active conversation branch."""
        history = chat_data.get("history", {})
        messages = history.get("messages", {})
        current_id = history.get("currentId")
        if not messages or not current_id:
            return []

        branch: list = []
        visited: set = set()
        msg_id = current_id
        while msg_id and msg_id not in visited:
            visited.add(msg_id)
            msg = messages.get(msg_id)
            if not msg:
                break
            branch.append(msg)
            msg_id = msg.get("parentId")

        branch.reverse()
        return branch

    # ------------------------------------------------------------------
    # Export / delete
    # ------------------------------------------------------------------

    def _export_chat(
        self,
        chat,
        folder_path_map: Dict[str, str],
        user_root_dir: str,
        user_tz: datetime.tzinfo,
    ):
        short_id = chat.id[-12:]
        display_title = chat.title or "Untitled Conversation"
        safe_title = _sanitize_filename(display_title)
        change_dt_local = self._get_chat_change_datetime_for_user(chat, user_tz)

        # Resolve destination directory
        rel_path = folder_path_map.get(chat.folder_id, "") if chat.folder_id else ""
        dest_dir = os.path.join(user_root_dir, rel_path) if rel_path else user_root_dir
        os.makedirs(dest_dir, exist_ok=True)

        # Remove previous export for this chat (may be in a different directory)
        self._remove_chat_files(user_root_dir, chat.id)

        # Build filename
        date_str = change_dt_local.strftime("%Y-%m-%d_%Hh%M")
        file_name = f"{date_str}_{safe_title}_{short_id}.md"
        file_path = os.path.join(dest_dir, file_name)

        # Extract messages from the active branch
        messages = self._extract_current_branch(chat.chat)

        # Tags from meta
        tags = sorted((chat.meta or {}).get("tags", []))

        # Build Markdown
        base_url = self.valves.OPEN_WEBUI_BASE_URL.rstrip("/")
        tag_lines = "".join(f"  - {t}\n" for t in tags)
        markdown_content = (
            f"---\n"
            f"link: {base_url}/c/{chat.id}\n"
            f"last_updated: {change_dt_local.isoformat()}\n"
            f"tags:\n"
            f"  - ai_chat\n"
            f"{tag_lines}"
            f"---\n\n"
            f"# {display_title}\n\n"
        )
        for message in messages:
            role = message.get("role", "unknown").capitalize()
            content = message.get("content", "")
            markdown_content += f"## {role}\n\n{content}\n\n---\n\n"

        # Atomic write
        tmp_path = f"{file_path}.tmp-{uuid.uuid4().hex[:6]}"
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(markdown_content)
        os.replace(tmp_path, file_path)

        self.logger.debug(f"Exported chat to {file_path}")

    def _get_chat_change_datetime_for_user(self, chat, user_tz: datetime.tzinfo) -> datetime.datetime:
        change_dt = self._get_chat_change_datetime(chat)
        return change_dt.astimezone(user_tz)

    def _get_user_timezone(self, user_id: Optional[str]) -> datetime.tzinfo:
        if user_id:
            try:
                user = Users.get_user_by_id(user_id)
                timezone_name = getattr(user, "timezone", None) if user else None
                if timezone_name:
                    return ZoneInfo(timezone_name)
            except ZoneInfoNotFoundError:
                self.logger.warning(f"Unknown timezone for user {user_id}; falling back to UTC")
            except Exception:
                self.logger.warning(f"Error resolving timezone for user {user_id}; falling back to UTC", exc_info=True)
        return datetime.timezone.utc

    @staticmethod
    def _get_chat_change_datetime(chat) -> datetime.datetime:
        try:
            return datetime.datetime.fromtimestamp(chat.updated_at, tz=datetime.timezone.utc)
        except (AttributeError, TypeError, ValueError, OSError, OverflowError):
            return datetime.datetime.now(datetime.timezone.utc)

    def _remove_chat_files(self, root_dir: str, chat_id: str):
        """Remove all previously exported files for a chat (matched by short_id suffix)."""
        short_id = chat_id[-12:]
        try:
            for root, _dirs, files in os.walk(root_dir):
                for fname in files:
                    if fname.endswith(".md") and short_id in fname:
                        fpath = os.path.join(root, fname)
                        try:
                            os.remove(fpath)
                            self.logger.debug(f"Removed {fpath}")
                        except Exception as e:
                            self.logger.warning(f"Could not remove {fpath}: {e}")
        except Exception as e:
            self.logger.warning(f"Error scanning for files to remove (chat {chat_id}): {e}")

    async def action(
        self,
        body: dict,
        __user__: Optional[dict] = None,
        __id__: Optional[str] = None,
        __request__=None,
        __event_emitter__=None,
    ):
        self._sync_runtime_valves()
        user_id = str((__user__ or {}).get("id", ""))
        if not user_id:
            await self._emit_status(__event_emitter__, "Auto-export requires a logged-in user.")
            return body

        function_id = self._resolve_function_id(__request__)
        if function_id:
            self._function_id = function_id

        if __id__ == "run_now":
            result = self._run_export_job()
            if result == "success":
                await self._emit_status(__event_emitter__, "Auto-export run completed.")
            elif result == "busy":
                await self._emit_status(__event_emitter__, "Auto-export already running; skipped.")
            elif result == "stopped":
                await self._emit_status(__event_emitter__, "Auto-export run stopped.")
            else:
                await self._emit_status(__event_emitter__, "Auto-export run failed.")
            return body

        if __id__ == "stop_current_job":
            result = self._request_stop_current_job()
            if result == "stopping":
                await self._emit_status(__event_emitter__, "Auto-export stop requested.")
            else:
                await self._emit_status(__event_emitter__, "No auto-export job is currently running.")
            return body

        await self._emit_status(__event_emitter__, "Unknown auto-export action.")
        return body
