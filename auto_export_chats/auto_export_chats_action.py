from __future__ import annotations

FUNCTION_ID = "auto_export_chats" # Set to the actual value

"""
title: Auto-Export Chats
id: auto_export_chats
author: Johan Grande, agents
repository: https://github.com/nahoj/open-webui-functions
version: 4.0
license: MIT
requirements: aiofiles, anyio
description: Automatically export chats to Markdown files.

This function creates a background job that exports new and changed chats every 5 minutes by default.
Chats are exported to folders mirroring OWUI's.

The export is opt-in per user with a UserValve (in a chat, Controls > Valves > Functions).

This function also provides an action button (enabled with Global/model toggle) to run the export once manually.

You can leave the button disabled if you only want the export to run in the background,
as long as the main toggle for this function is on.
"""

import asyncio
import json
import logging
import os
import re
import uuid
import aiofiles
import aiofiles.os

from anyio import Path as AnyPath
from datetime import datetime, timezone, tzinfo
from typing import Any, Dict, List, Optional, Set
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field

from open_webui.internal.db import get_db_context
from open_webui.models.chats import Chat, Chats
from open_webui.models.folders import Folder
from open_webui.models.users import UserModel, Users


log = logging.getLogger("auto_export_chats")
log.setLevel(logging.DEBUG)


# ═══════════════════════════════════════════════════════════════════════════════
# Action
# ═══════════════════════════════════════════════════════════════════════════════

class Action:
    actions = [
        {"id": "run_now", "name": "Run Auto-Export Now"},
    ]

    class Valves(BaseModel):
        POLL_INTERVAL_SECONDS: int = Field(
            default=300,
            description="How often to check for changes (seconds). Set to 0 to disable background auto-run. Default: 5 minutes.",
        )
        OPEN_WEBUI_BASE_URL: str = Field(
            default="",
            description="Base URL of the Open WebUI instance (e.g., https://your.domain), for Markdown frontmatter link.",
        )
        EXPORT_DIR: str = Field(
            default="/app/backend/data/Chats",
            description="Root directory under which user chats will be saved.",
        )

    class UserValves(BaseModel):
        ENABLED: bool = Field(
            default=False,
            description="Enable chat auto-export for this user.",
        )

    def __init__(self):
        self.valves = self.Valves()

        if int(self.valves.POLL_INTERVAL_SECONDS) > 0:
            self._worker = ExportWorker(self.valves)
        else:
            self._worker = None

        log.info(
            f"Auto-Export Chats initialized (polling every {self.valves.POLL_INTERVAL_SECONDS}s). "
            f"Saving to: {os.path.abspath(self.valves.EXPORT_DIR)}"
        )

    def __del__(self):
        if self._worker:
            self._worker.cancel()

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

    async def action(
        self,
        body: dict,
        __user__: Optional[dict] = None,
        __id__: Optional[str] = None,
        __request__=None,
        __event_emitter__=None,
    ):
        user_id = str((__user__ or {}).get("id", ""))
        if not user_id:
            await self._emit_status(__event_emitter__, "Auto-export requires a logged-in user.")
            return body

        # if __id__ == "run_now":
        #     if self._worker is None:
        #         # Create a temporary worker for one-off execution
        #         worker = ExportWorker(self.valves)
        #         result = await worker.run_export_job()
        #     else:
        #         result = await self._worker.run_export_job()
        #     if result == "success":
        #         await self._emit_status(__event_emitter__, "Auto-export run completed.")
        #     else:
        #         await self._emit_status(__event_emitter__, "Auto-export run failed.")
        #     return body

        await self._emit_status(__event_emitter__, "Unknown auto-export action.")
        return body


# ═══════════════════════════════════════════════════════════════════════════════
# ExportWorker
# ═══════════════════════════════════════════════════════════════════════════════

class ExportWorker:
    def __init__(self, valves):
        self.valves = valves
        self._task = asyncio.get_running_loop().create_task(self._run())

    def cancel(self):
        self._task.cancel()

    async def _run(self):
        try:
            while True:
                await asyncio.sleep(self.valves.POLL_INTERVAL_SECONDS)
                await AllUserExport.run(self.valves)
        except asyncio.CancelledError:
            pass


# ═══════════════════════════════════════════════════════════════════════════════
# AllUserExport
# ═══════════════════════════════════════════════════════════════════════════════

class AllUserExport:

    @staticmethod
    async def run(valves: Action.Valves) -> str:
        try:
            job_started_at = datetime.now(timezone.utc)
            user_ids = await AllUserExport._get_enabled_user_ids()
            log.info(f"Starting export job for {len(user_ids)} user(s): {user_ids}")
            for user_id in user_ids:
                await SingleUserExport.export_user(user_id, job_started_at, valves)
            return "success"
        except Exception:
            log.error(f"Error running chat export", exc_info=True)
            return "failed"

    @staticmethod
    async def _get_enabled_user_ids() -> Set[str]:
        try:
            result = await asyncio.to_thread(lambda: Users.get_users(limit=None))
            if isinstance(result, dict):
                users = result.get("users", [])
            elif isinstance(result, list):
                users = result
            else:
                log.warning("Unexpected return type from Users.get_users: %s", type(result).__name__)
                return set()
        except Exception:
            log.error("Error loading users for auto-export polling", exc_info=True)
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
                .get(FUNCTION_ID, {})
                .get("ENABLED")
            ):
                enabled_user_ids.add(user.id)
        return enabled_user_ids


# ═══════════════════════════════════════════════════════════════════════════════
# SingleUserExport
# ═══════════════════════════════════════════════════════════════════════════════

class SingleUserExport:

    @staticmethod
    async def export_user(user_id: str, job_started_at: datetime, valves: Action.Valves):
        user = await asyncio.to_thread(Users.get_user_by_id, user_id)

        user_export_dir = os.path.join(valves.EXPORT_DIR, _sanitize_filename(user.name))
        last_successful_export_at = await SingleUserExport._read_last_successful_export_at(user_export_dir)
        log.info(f"User {user_id}: root_dir={user_export_dir}, last_successful_export_at={last_successful_export_at}")

        cur_folders = await SingleUserExport._query_folders(user_id)
        # log.info(f"User {user_id}: {len(cur_folders)} folder(s) in DB")
        folder_path_map = FolderExport.build_folder_path_map(cur_folders)
        # log.debug(f"User {user_id}: folder_path_map={folder_path_map}")
        await SingleUserExport._cleanup_orphaned_folder_markers(user_export_dir, set(cur_folders.keys()))
        await FolderExport.reconcile_user_folders(user_export_dir, cur_folders, folder_path_map)

        await ChatExport.export_chats(user, user_export_dir, folder_path_map, last_successful_export_at,
                                      valves.OPEN_WEBUI_BASE_URL)

        await SingleUserExport._write_state(user_export_dir, job_started_at)

    @staticmethod
    def _state_file_path(user_export_dir: str) -> str:
        return os.path.join(user_export_dir, "_auto_export_state.json")

    @staticmethod
    async def _read_state(user_export_dir: str) -> Dict[str, Any]:
        try:
            async with aiofiles.open(SingleUserExport._state_file_path(user_export_dir), "r", encoding="utf-8") as f:
                data = json.loads(await f.read())
        except FileNotFoundError:
            return {}
        except Exception:
            log.error("Error reading auto-export state", exc_info=True)
            return {}

        return data if isinstance(data, dict) else {}

    @staticmethod
    async def _read_last_successful_export_at(user_export_dir: str) -> Optional[datetime]:
        data = await SingleUserExport._read_state(user_export_dir)

        value = data.get("last_successful_export_at") if isinstance(data, dict) else None
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            log.warning("Invalid last_successful_export_at in auto-export state file")
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed

    @staticmethod
    async def _write_state(user_export_dir: str, job_started_at: datetime):
        await aiofiles.os.makedirs(user_export_dir, exist_ok=True)
        content = json.dumps({"last_successful_export_at": job_started_at.isoformat()}, indent=2) + "\n"
        async with aiofiles.open(SingleUserExport._state_file_path(user_export_dir), "w", encoding="utf-8") as f:
            await f.write(content)

    @staticmethod
    async def _query_folders(user_id: str) -> Dict[str, _FolderSnapshot]:
        """Returns {folder_id: _FolderSnapshot}. Lightweight DB query (no chat JSON blob)."""
        def _sync():
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
        return await asyncio.to_thread(_sync)

    @staticmethod
    async def _cleanup_orphaned_folder_markers(user_export_dir: str, valid_folder_ids: Set[str]) -> None:
        """Remove .open_webui_id=* marker files whose folder ID is no longer in the DB."""
        root = AnyPath(user_export_dir)
        if not await root.exists():
            return
        async for marker_path in root.glob("**/.open_webui_id=*"):
            folder_id = FolderExport.parse_folder_id_file_name(marker_path.name)
            if folder_id and folder_id not in valid_folder_ids:
                try:
                    await aiofiles.os.remove(str(marker_path))
                    log.info(f"Removed orphaned folder marker: {marker_path}")
                except OSError:
                    pass


# ═══════════════════════════════════════════════════════════════════════════════
# FolderExport
# ═══════════════════════════════════════════════════════════════════════════════

class FolderExport:

    @staticmethod
    def build_folder_path_map(folders: Dict[str, _FolderSnapshot]) -> Dict[str, str]:
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

    @staticmethod
    def _folder_id_file_name(folder_id: str) -> str:
        return f".open_webui_id={folder_id}"

    @staticmethod
    def parse_folder_id_file_name(file_name: str) -> Optional[str]:
        prefix = ".open_webui_id="
        if not file_name.startswith(prefix):
            return None
        folder_id = file_name[len(prefix):]
        return folder_id or None

    @staticmethod
    async def _write_folder_id_marker(folder_dir: str, folder_id: str):
        await aiofiles.os.makedirs(folder_dir, exist_ok=True)
        expected_marker_name = FolderExport._folder_id_file_name(folder_id)
        for entry in await aiofiles.os.listdir(folder_dir):
            marker_id = FolderExport.parse_folder_id_file_name(entry)
            if marker_id is not None and entry != expected_marker_name:
                await aiofiles.os.remove(os.path.join(folder_dir, entry))
        marker_path = os.path.join(folder_dir, expected_marker_name)
        if not await aiofiles.os.path.exists(marker_path):
            async with aiofiles.open(marker_path, "w", encoding="utf-8"):
                pass

    @staticmethod
    async def _read_folder_id_marker(folder_dir: str) -> Optional[str]:
        try:
            for entry in await aiofiles.os.listdir(folder_dir):
                folder_id = FolderExport.parse_folder_id_file_name(entry)
                if folder_id is not None:
                    return folder_id
        except FileNotFoundError:
            return None
        return None

    @staticmethod
    async def _find_folder_dir_by_id(user_export_dir: str, folder_id: str) -> Optional[str]:
        root = AnyPath(user_export_dir)
        if not await root.is_dir():
            return None
        marker_name = FolderExport._folder_id_file_name(folder_id)
        async for marker_path in root.rglob(marker_name):
            if not await marker_path.is_file():
                continue
            relative_parent = marker_path.parent.relative_to(root)
            if any(part.startswith(".") for part in relative_parent.parts):
                continue
            return str(marker_path.parent)
        return None

    @staticmethod
    async def _iter_managed_folder_ids_under_path(path: str) -> Set[str]:
        managed_ids: Set[str] = set()
        root = AnyPath(path)
        if not await root.exists():
            return managed_ids
        async for marker_path in root.glob("**/.open_webui_id=*"):
            if not await marker_path.is_file():
                continue
            relative_parent = marker_path.parent.relative_to(root)
            if any(part.startswith(".") for part in relative_parent.parts):
                continue
            folder_id = FolderExport.parse_folder_id_file_name(marker_path.name)
            if folder_id is not None:
                managed_ids.add(folder_id)
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

    @staticmethod
    async def _is_folder_processable(
        folder_id: str,
        actual_dir: str,
        expected_dir: str,
        folders: Dict[str, _FolderSnapshot],
    ) -> bool:
        if FolderExport._is_relative_to(expected_dir, actual_dir):
            return False
        subtree_ids = FolderExport._collect_folder_subtree_ids(folder_id, folders)
        target_ids = await FolderExport._iter_managed_folder_ids_under_path(expected_dir)
        return target_ids.issubset(subtree_ids)

    @staticmethod
    async def _merge_directories(source_dir: str, target_dir: str):
        await aiofiles.os.makedirs(target_dir, exist_ok=True)
        for entry in await aiofiles.os.listdir(source_dir):
            source_path = os.path.join(source_dir, entry)
            target_path = os.path.join(target_dir, entry)
            if await aiofiles.os.path.isdir(source_path):
                if await aiofiles.os.path.isdir(target_path):
                    await FolderExport._merge_directories(source_path, target_path)
                    if not await aiofiles.os.listdir(source_path):
                        await aiofiles.os.rmdir(source_path)
                else:
                    await aiofiles.os.replace(source_path, target_path)
            else:
                if not await aiofiles.os.path.exists(target_path):
                    await aiofiles.os.replace(source_path, target_path)
                else:
                    src_mtime = (await aiofiles.os.stat(source_path)).st_mtime
                    tgt_mtime = (await aiofiles.os.stat(target_path)).st_mtime
                    if src_mtime > tgt_mtime:
                        await aiofiles.os.replace(source_path, target_path)
                    else:
                        await aiofiles.os.remove(source_path)
        if await aiofiles.os.path.isdir(source_dir) and not await aiofiles.os.listdir(source_dir):
            await aiofiles.os.rmdir(source_dir)

    @staticmethod
    async def _process_folder_relocation(
        folder_id: str,
        actual_dir: Optional[str],
        expected_dir: str,
    ):
        if actual_dir is None:
            await aiofiles.os.makedirs(expected_dir, exist_ok=True)
            await FolderExport._write_folder_id_marker(expected_dir, folder_id)
            return
        await aiofiles.os.makedirs(os.path.dirname(expected_dir), exist_ok=True)
        if not await aiofiles.os.path.exists(expected_dir):
            await aiofiles.os.replace(actual_dir, expected_dir)
        elif os.path.abspath(actual_dir) != os.path.abspath(expected_dir):
            await FolderExport._merge_directories(actual_dir, expected_dir)
        await FolderExport._write_folder_id_marker(expected_dir, folder_id)

    @staticmethod
    async def reconcile_user_folders(
        user_export_dir: str,
        folders: Dict[str, _FolderSnapshot],
        folder_path_map: Dict[str, str],
    ):
        await aiofiles.os.makedirs(user_export_dir, exist_ok=True)
        pending_folder_ids = sorted(folder_path_map, key=lambda folder_id: (folder_path_map[folder_id].count(os.sep), folder_path_map[folder_id]))
        log.debug(f"Reconciling {len(pending_folder_ids)} folder(s) under {user_export_dir}")
        while pending_folder_ids:
            processed_any = False
            for folder_id in list(pending_folder_ids):
                expected_dir = os.path.join(user_export_dir, folder_path_map[folder_id])
                marker_id = await FolderExport._read_folder_id_marker(expected_dir)
                if marker_id == folder_id:
                    await FolderExport._write_folder_id_marker(expected_dir, folder_id)
                    pending_folder_ids.remove(folder_id)
                    processed_any = True
                    log.debug(f"Folder {folder_id} already at {expected_dir}")
                    continue
                actual_dir = await FolderExport._find_folder_dir_by_id(user_export_dir, folder_id)
                if actual_dir is not None and not await FolderExport._is_folder_processable(folder_id, actual_dir, expected_dir, folders):
                    log.debug(f"Folder {folder_id}: not processable yet (actual={actual_dir}, expected={expected_dir})")
                    continue
                log.debug(f"Folder {folder_id}: relocating {actual_dir} -> {expected_dir}")
                await FolderExport._process_folder_relocation(folder_id, actual_dir, expected_dir)
                pending_folder_ids.remove(folder_id)
                processed_any = True
                break
            if not processed_any:
                log.error(f"Stuck reconciling folders: remaining={pending_folder_ids}")
                raise RuntimeError(f"Could not reconcile folder layout for user root {user_export_dir}")


# ═══════════════════════════════════════════════════════════════════════════════
# ChatExport
# ═══════════════════════════════════════════════════════════════════════════════

class ChatExport:

    @staticmethod
    async def export_chats(
        user: UserModel,
        user_export_dir: str,
        folder_path_map: dict[str, str],
        last_successful_export_at: Optional[datetime],
        open_webui_base_url: str
    ):
        chats_to_export_ids = await ChatExport._query_chat_ids_to_export(user.id, last_successful_export_at)
        log.info(f"User {user.name}: {len(chats_to_export_ids)} chat(s) to export")
        user_tz = ZoneInfo(user.timezone or "UTC")
        for i, chat_id in enumerate(chats_to_export_ids, 1):
            log.debug(f"User {user.name}: exporting chat {i}/{len(chats_to_export_ids)}: {chat_id}")
            chat = await asyncio.to_thread(Chats.get_chat_by_id, chat_id)
            if chat:
                await ChatExport._export_chat(open_webui_base_url, chat, folder_path_map, user_export_dir, user_tz)
            else:
                log.warning(f"Chat {chat_id} not found in DB, skipping")

        if chats_to_export_ids:
            log.info(
                f"User {user.name}: exported {len(chats_to_export_ids)} since {last_successful_export_at.isoformat() if last_successful_export_at else 'beginning'} "
            )

    @staticmethod
    async def _query_chat_ids_to_export(
        user_id: str,
        last_successful_export_at: Optional[datetime],
    ) -> List[str]:
        def _sync():
            with get_db_context() as db:
                query = db.query(Chat.id).filter_by(user_id=user_id)
                if last_successful_export_at is not None:
                    since_ts = int(last_successful_export_at.timestamp())
                    query = query.filter(Chat.updated_at > since_ts)
                return [row[0] for row in query.all()]
        return await asyncio.to_thread(_sync)

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

    @staticmethod
    async def _export_chat(
        open_webui_base_url,
        chat,
        folder_path_map: Dict[str, str],
        user_export_dir: str,
        user_tz: tzinfo,
    ):
        short_id = chat.id[-12:]
        display_title = chat.title or "Untitled Conversation"
        safe_title = _sanitize_filename(display_title)
        change_dt_local = ChatExport._get_chat_change_datetime_for_user(chat, user_tz)

        # Resolve destination directory
        rel_path = folder_path_map.get(chat.folder_id, "") if chat.folder_id else ""
        dest_dir = os.path.join(user_export_dir, rel_path) if rel_path else user_export_dir
        await aiofiles.os.makedirs(dest_dir, exist_ok=True)

        # Remove previous export for this chat (may be in a different directory)
        await ChatExport._remove_chat_files(user_export_dir, chat.id)

        # Build filename
        date_str = change_dt_local.strftime("%Y-%m-%d_%Hh%M")
        file_name = f"{date_str}_{safe_title}_{short_id}.md"
        file_path = os.path.join(dest_dir, file_name)

        # Extract messages from the active branch
        messages = ChatExport._extract_current_branch(chat.chat)

        # Tags from meta
        tags = sorted((chat.meta or {}).get("tags", []))

        # Build Markdown
        base_url = open_webui_base_url.rstrip("/")
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
        async with aiofiles.open(tmp_path, "w", encoding="utf-8") as f:
            await f.write(markdown_content)
        await aiofiles.os.replace(tmp_path, file_path)

        log.debug(f"Exported chat to {file_path}")

    @staticmethod
    def _get_chat_change_datetime_for_user(chat, user_tz: tzinfo) -> datetime:
        change_dt = ChatExport._get_chat_change_datetime(chat)
        return change_dt.astimezone(user_tz)

    @staticmethod
    def _get_chat_change_datetime(chat) -> datetime:
        try:
            return datetime.fromtimestamp(chat.updated_at, tz=timezone.utc)
        except (AttributeError, TypeError, ValueError, OSError, OverflowError):
            return datetime.now(timezone.utc)

    @staticmethod
    async def _remove_chat_files(root_dir: str, chat_id: str):
        """Remove all previously exported files for a chat (matched by short_id suffix)."""
        short_id = chat_id[-12:]
        try:
            root = AnyPath(root_dir)
            if not await root.exists():
                return
            async for file_path in root.glob(f"**/*{short_id}*.md"):
                if not await file_path.is_file():
                    continue
                try:
                    await aiofiles.os.remove(str(file_path))
                    log.debug(f"Removed {file_path}")
                except Exception as e:
                    log.warning(f"Could not remove {file_path}: {e}")
        except Exception as e:
            log.warning(f"Error scanning for files to remove (chat {chat_id}): {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# Per-user change-detection snapshot
# ═══════════════════════════════════════════════════════════════════════════════

class _FolderSnapshot:
    __slots__ = ("updated_at", "name", "parent_id")

    def __init__(self, updated_at: int, name: str, parent_id: Optional[str]):
        self.updated_at = updated_at
        self.name = name
        self.parent_id = parent_id

    def differs(self, other: _FolderSnapshot) -> bool:
        return (
            self.updated_at != other.updated_at
            or self.name != other.name
            or self.parent_id != other.parent_id
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

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
