import asyncio
import datetime
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Optional
from unittest.mock import AsyncMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from auto_export_chats.auto_export_chats_action import (
    Action,
    ExportScheduler,
    _FolderSnapshot,
    _sanitize_filename,
    ChatExport,
    FolderExport,
    SingleUserExport,
)


def make_chat(
    chat_id="chat-123456789012",
    title: Optional[str] = "Test Chat",
    folder_id=None,
    user_id="user-1",
    updated_at: Optional[int] = 1712345678,
    tags=None,
    chat_data=None,
):
    return SimpleNamespace(
        id=chat_id,
        title=title,
        folder_id=folder_id,
        user_id=user_id,
        updated_at=updated_at,
        meta={"tags": tags or []},
        chat=chat_data
        or {
            "history": {
                "currentId": "assistant-1",
                "messages": {
                    "user-1": {"role": "user", "content": "Hello", "parentId": None},
                    "assistant-1": {
                        "role": "assistant",
                        "content": "Hi there",
                        "parentId": "user-1",
                    },
                },
            }
        },
    )


def make_body(chat_id="chat-123456789012"):
    return {"chat_id": chat_id}


def make_user(enabled=True, user_id="user-1"):
    return {"id": user_id, "valves": {"ENABLED": enabled}}


class TestUtilities:
    def test_sanitize_filename(self):
        assert _sanitize_filename("Test: File / Name") == "Test-_File_-_Name"
        assert _sanitize_filename("<Invalid>") == "-Invalid-"
        assert _sanitize_filename("Normal Name") == "Normal_Name"
        assert _sanitize_filename("  spaces  ") == "spaces"


class TestExportScheduler:
    @pytest.mark.asyncio
    async def test_scheduler_starts_with_interval(self):
        valves = Action.Valves(EXPORT_INTERVAL_SECONDS=300)
        sched = ExportScheduler(valves)
        try:
            assert sched._scheduler.running
            job = sched._scheduler.get_job(ExportScheduler._JOB_ID)
            assert job is not None
        finally:
            sched.shutdown()

    @pytest.mark.asyncio
    async def test_scheduler_starts_without_interval(self):
        valves = Action.Valves(EXPORT_INTERVAL_SECONDS=0)
        sched = ExportScheduler(valves)
        try:
            assert sched._scheduler.running
            job = sched._scheduler.get_job(ExportScheduler._JOB_ID)
            assert job is None
        finally:
            sched.shutdown()

    @pytest.mark.asyncio
    async def test_shutdown_stops_scheduler(self):
        valves = Action.Valves(EXPORT_INTERVAL_SECONDS=300)
        sched = ExportScheduler(valves)
        sched.shutdown()
        await asyncio.sleep(0)
        assert not sched._scheduler.running

    @pytest.mark.asyncio
    async def test_run_skips_when_locked(self):
        valves = Action.Valves(EXPORT_INTERVAL_SECONDS=0)
        sched = ExportScheduler(valves)
        try:
            await sched._lock.acquire()
            result = await sched.run_export_with_lock()
            assert result == "skipped"
            sched._lock.release()
        finally:
            sched.shutdown()


def _make_action():
    """Build an Action without starting a real scheduler."""
    action = Action.__new__(Action)
    action.valves = Action.Valves(EXPORT_INTERVAL_SECONDS=0)
    action._scheduler = ExportScheduler(action.valves)
    return action


class TestAction:
    @pytest.mark.asyncio
    async def test_action_run_now_triggers_export(self):
        body = make_body()
        emitter = AsyncMock()
        action = _make_action()

        with patch.object(action._scheduler, "run", return_value="success") as mock_run:
            result = await action.action(
                body,
                __user__=make_user(),
                __id__="run_now",
                __event_emitter__=emitter,
            )

        assert result == body
        mock_run.assert_awaited_once()
        emitter.assert_awaited()
        assert emitter.await_args.args[0]["data"]["description"] == "Auto-export completed."
        action._scheduler.shutdown()

    @pytest.mark.asyncio
    async def test_action_run_now_reports_already_running(self):
        body = make_body()
        emitter = AsyncMock()
        action = _make_action()

        with patch.object(action._scheduler, "run", return_value="skipped") as mock_run:
            result = await action.action(
                body,
                __user__=make_user(),
                __id__="run_now",
                __event_emitter__=emitter,
            )

        assert result == body
        emitter.assert_awaited()
        assert emitter.await_args.args[0]["data"]["description"] == "Auto-export already running."
        action._scheduler.shutdown()

    @pytest.mark.asyncio
    async def test_action_requires_logged_in_user(self):
        body = make_body()
        emitter = AsyncMock()
        action = _make_action()

        result = await action.action(
            body,
            __user__={},
            __id__="run_now",
            __event_emitter__=emitter,
        )

        assert result == body
        emitter.assert_awaited()
        assert "requires a logged-in user" in emitter.await_args.args[0]["data"]["description"]
        action._scheduler.shutdown()


class TestBranchExtraction:
    def test_extract_current_branch_uses_active_path(self):
        chat_data = {
            "history": {
                "currentId": "assistant-b",
                "messages": {
                    "user-1": {"role": "user", "content": "Question", "parentId": None},
                    "assistant-a": {"role": "assistant", "content": "Wrong", "parentId": "user-1"},
                    "assistant-b": {"role": "assistant", "content": "Right", "parentId": "user-1"},
                },
            }
        }

        result = ChatExport._extract_current_branch(chat_data)

        assert result == [
            {"role": "user", "content": "Question", "parentId": None},
            {"role": "assistant", "content": "Right", "parentId": "user-1"},
        ]

    def test_extract_current_branch_returns_empty_without_current_id(self):
        assert ChatExport._extract_current_branch({"history": {"messages": {}}}) == []


class TestFolderPaths:
    def test_build_folder_path_map_builds_full_hierarchy(self):
        folders = {
            "root": _FolderSnapshot(updated_at=1, name="Root Folder", parent_id=None),
            "parent": _FolderSnapshot(updated_at=1, name="Parent Folder", parent_id="root"),
            "leaf": _FolderSnapshot(updated_at=1, name="Leaf Folder", parent_id="parent"),
        }

        path_map = FolderExport.build_folder_path_map(folders)

        assert path_map == {
            "root": "Root_Folder",
            "parent": "Root_Folder/Parent_Folder",
            "leaf": "Root_Folder/Parent_Folder/Leaf_Folder",
        }

    def test_folder_id_marker_helpers_roundtrip(self, tmp_path):
        folder_dir = tmp_path / "alice" / "Root"
        asyncio.run(FolderExport._write_folder_id_marker(str(folder_dir), "folder-1"))

        assert asyncio.run(FolderExport._read_folder_id_marker(str(folder_dir))) == "folder-1"
        assert asyncio.run(FolderExport._find_folder_dir_by_id(str(tmp_path / "alice"), "folder-1")) == str(folder_dir)

    def test_reconcile_user_folders_moves_managed_folder_to_expected_path(self, tmp_path):
        user_root_dir = tmp_path / "alice"
        actual_dir = user_root_dir / "Old"
        actual_dir.mkdir(parents=True)
        asyncio.run(FolderExport._write_folder_id_marker(str(actual_dir), "folder-1"))
        (actual_dir / "chat.md").write_text("content", encoding="utf-8")

        folders = {"folder-1": _FolderSnapshot(updated_at=1, name="Renamed", parent_id=None)}
        folder_path_map = {"folder-1": "Renamed"}

        asyncio.run(FolderExport.reconcile_user_folders(str(user_root_dir), folders, folder_path_map))

        expected_dir = user_root_dir / "Renamed"
        assert expected_dir.is_dir()
        assert asyncio.run(FolderExport._read_folder_id_marker(str(expected_dir))) == "folder-1"
        assert (expected_dir / "chat.md").exists()
        assert not actual_dir.exists()

    def test_reconcile_user_folders_merges_existing_target_and_keeps_newer_file(self, tmp_path):
        user_root_dir = tmp_path / "alice"
        source_dir = user_root_dir / "Old"
        source_dir.mkdir(parents=True)
        asyncio.run(FolderExport._write_folder_id_marker(str(source_dir), "folder-1"))
        source_file = source_dir / "chat.md"
        source_file.write_text("new", encoding="utf-8")

        target_dir = user_root_dir / "Renamed"
        target_dir.mkdir(parents=True)
        target_file = target_dir / "chat.md"
        target_file.write_text("old", encoding="utf-8")

        source_mtime = source_file.stat().st_mtime
        os.utime(target_file, (source_mtime - 100, source_mtime - 100))

        folders = {"folder-1": _FolderSnapshot(updated_at=1, name="Renamed", parent_id=None)}
        folder_path_map = {"folder-1": "Renamed"}

        asyncio.run(FolderExport.reconcile_user_folders(str(user_root_dir), folders, folder_path_map))

        assert target_file.read_text(encoding="utf-8") == "new"
        assert asyncio.run(FolderExport._read_folder_id_marker(str(target_dir))) == "folder-1"

    def test_cleanup_orphaned_folder_markers_removes_markers_not_in_db(self, tmp_path):
        user_root_dir = tmp_path / "alice"
        keep_dir = user_root_dir / "Keep"
        keep_dir.mkdir(parents=True)
        asyncio.run(FolderExport._write_folder_id_marker(str(keep_dir), "valid-folder-id"))
        (keep_dir / "chat.md").write_text("content", encoding="utf-8")

        orphan_dir = user_root_dir / "Orphan"
        orphan_dir.mkdir(parents=True)
        asyncio.run(FolderExport._write_folder_id_marker(str(orphan_dir), "deleted-folder-id"))
        (orphan_dir / "old.md").write_text("stale", encoding="utf-8")

        valid_folder_ids = {"valid-folder-id"}
        asyncio.run(SingleUserExport._cleanup_orphaned_folder_markers(str(user_root_dir), valid_folder_ids))

        assert keep_dir.is_dir()
        assert asyncio.run(FolderExport._read_folder_id_marker(str(keep_dir))) == "valid-folder-id"
        assert (keep_dir / "chat.md").exists()

        orphan_marker = orphan_dir / ".open_webui_id=deleted-folder-id"
        assert not orphan_marker.exists()
        assert orphan_dir.is_dir()
        assert (orphan_dir / "old.md").exists()


class TestMarkdownExport:
    def test_export_chat_writes_file_under_user_root_and_cleans_stale_exports(self, tmp_path):
        chat = make_chat(tags=["tag-a", "tag-b"], updated_at=1712345678)
        expected_prefix = "2024-04-05_21h34"
        user_tz = datetime.timezone(datetime.timedelta(hours=2))
        user_root_dir = tmp_path / "alice"
        user_root_dir.mkdir()
        # Stale file in a subdirectory of user_root_dir (should be cleaned up)
        stale_dir = user_root_dir / "old"
        stale_dir.mkdir()
        stale_file = stale_dir / f"older_{chat.id[-12:]}.md"
        stale_file.write_text("stale", encoding="utf-8")

        asyncio.run(ChatExport._export_chat("https://owui.example", chat, {}, str(user_root_dir), user_tz))

        md_files = list(user_root_dir.glob("*.md"))
        assert len(md_files) == 1
        assert not stale_file.exists()
        assert md_files[0].name.startswith(f"{expected_prefix}_")
        assert chat.id[-12:] in md_files[0].name
        assert ".tmp-" not in md_files[0].name

        content = md_files[0].read_text(encoding="utf-8")
        assert content.startswith("---\n")
        assert f"link: https://owui.example/c/{chat.id}" in content
        assert "last_updated: 2024-04-05T21:34:38+02:00" in content
        assert "# Test Chat" in content
        assert "  - ai_chat" in content
        assert "  - tag-a" in content
        assert "  - tag-b" in content
        assert "## User" in content
        assert "## Assistant" in content
        assert "Hello" in content
        assert "Hi there" in content

    def test_export_chat_uses_folder_hierarchy_when_available(self, tmp_path):
        chat = make_chat(title="Folder Chat", folder_id="leaf", updated_at=1712345678)
        folder_path_map = {"leaf": "Root_Folder/Parent_Folder/Leaf_Folder"}
        user_tz = datetime.timezone(datetime.timedelta(hours=2))
        user_root_dir = tmp_path / "alice"

        asyncio.run(ChatExport._export_chat("https://owui.example", chat, folder_path_map, str(user_root_dir), user_tz))

        expected_dir = user_root_dir / "Root_Folder" / "Parent_Folder" / "Leaf_Folder"
        assert expected_dir.is_dir()
        assert len(list(expected_dir.glob("*.md"))) == 1

    def test_export_chat_uses_untitled_fallback_in_filename_and_heading(self, tmp_path):
        chat = make_chat(title=None, updated_at=1712345678)
        expected_prefix = "2024-04-05_21h34"
        user_tz = datetime.timezone(datetime.timedelta(hours=2))
        user_root_dir = tmp_path / "alice"

        asyncio.run(ChatExport._export_chat("https://owui.example", chat, {}, str(user_root_dir), user_tz))

        md_files = list(user_root_dir.glob("*.md"))
        assert len(md_files) == 1
        assert md_files[0].name.startswith(f"{expected_prefix}_")
        assert "Untitled_Conversation" in md_files[0].name
        content = md_files[0].read_text(encoding="utf-8")
        assert "# Untitled Conversation" in content

    def test_export_chat_falls_back_to_now_when_updated_at_is_invalid(self, tmp_path):
        chat = make_chat(updated_at=None)
        fake_now = datetime.datetime(2026, 3, 29, 22, 39, tzinfo=datetime.timezone.utc)
        user_root_dir = tmp_path / "alice"

        with patch("auto_export_chats.auto_export_chats_action.datetime") as mock_datetime:
            mock_datetime.fromtimestamp.side_effect = TypeError("invalid timestamp")
            mock_datetime.now.return_value = fake_now
            asyncio.run(ChatExport._export_chat("https://owui.example", chat, {}, str(user_root_dir), datetime.timezone.utc))

        md_files = list(user_root_dir.glob("*.md"))
        assert len(md_files) == 1
        assert md_files[0].name.startswith("2026-03-29_22h39_")

    def test_remove_chat_files_deletes_matching_exports(self, tmp_path):
        root_dir = tmp_path / "alice"
        root_dir.mkdir()
        keep_file = root_dir / "keep_me.md"
        keep_file.write_text("keep", encoding="utf-8")
        remove_a = root_dir / "a_chat-123456789012.md"
        remove_a.write_text("drop", encoding="utf-8")
        nested_dir = root_dir / "nested"
        nested_dir.mkdir()
        remove_b = nested_dir / "b_chat-123456789012.md"
        remove_b.write_text("drop", encoding="utf-8")

        asyncio.run(ChatExport._remove_chat_files(str(root_dir), "chat-123456789012"))

        assert keep_file.exists()
        assert not remove_a.exists()
        assert not remove_b.exists()
