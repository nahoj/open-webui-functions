import asyncio
import datetime
import logging
import os
import sys
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Optional
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from auto_export_chats.auto_export_chats_action import (
    Action,
    _ExportWorker,
    _FolderSnapshot,
    _sanitize_filename,
    ChatExport,
    _get_user_timezone,
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


def build_filter(tmp_path, **valve_overrides):
    flt = Action.__new__(Action)
    valve_values = {
        "SAVE_FOLDER": str(tmp_path),
        "OPEN_WEBUI_BASE_URL": "https://owui.example",
        "POLL_INTERVAL_SECONDS": 300,
    }
    valve_values.update(valve_overrides)
    flt.valves = Action.Valves(**valve_values)
    flt.logger = logging.getLogger(f"tests.autosave.{id(tmp_path)}")
    flt._function_id = "auto_export_chats"
    flt._run_lock = threading.Lock()
    flt._stop_event = threading.Event()
    flt._job_stop_requested = threading.Event()
    flt._job_running = threading.Event()
    flt._poll_thread = None
    return flt


def build_worker(tmp_path, **valve_overrides):
    worker = _ExportWorker.__new__(_ExportWorker)
    valve_values = {
        "SAVE_FOLDER": str(tmp_path),
        "OPEN_WEBUI_BASE_URL": "https://owui.example",
        "POLL_INTERVAL_SECONDS": 300,
    }
    valve_values.update(valve_overrides)
    worker.valves = Action.Valves(**valve_values)
    worker.logger = logging.getLogger(f"tests.autosave.worker.{id(tmp_path)}")
    worker.function_id = "auto_export_chats"
    worker._task = SimpleNamespace(cancel=lambda: None)
    return worker


@pytest.fixture
def filter_instance(tmp_path):
    return build_filter(tmp_path)


@pytest.fixture
def worker_instance(tmp_path):
    return build_worker(tmp_path)


class TestUtilities:
    def test_sanitize_filename(self):
        assert _sanitize_filename("Test: File / Name") == "Test-_File_-_Name"
        assert _sanitize_filename("<Invalid>") == "-Invalid-"
        assert _sanitize_filename("Normal Name") == "Normal_Name"
        assert _sanitize_filename("  spaces  ") == "spaces"

"""
class TestActions:
    @pytest.mark.asyncio
    async def test_action_run_now_runs_export_job_and_emits_status(self):
        body = make_body()
        emitter = AsyncMock()
        action_instance = build_filter(Path("/tmp"))

        with patch.object(action_instance, "_run_export_job", return_value="success") as run_export_job:
            result = await action_instance.action(
                body,
                __user__=make_user(enabled=False),
                __id__="run_now",
                __request__=SimpleNamespace(path_params={"action_id": "auto_export_chats.run_now"}),
                __event_emitter__=emitter,
            )

        assert result == body
        run_export_job.assert_called_once_with()
        emitter.assert_awaited_once()
        assert emitter.await_args.args[0]["data"]["description"] == "Auto-export run completed."

    @pytest.mark.asyncio
    async def test_action_run_now_skips_when_a_previous_job_is_running(self):
        body = make_body()
        emitter = AsyncMock()
        action_instance = build_filter(Path("/tmp"))
        action_instance._run_lock.acquire()

        try:
            with patch.object(action_instance, "_run_export_job", return_value="busy") as run_export_job:
                result = await action_instance.action(
                    body,
                    __user__=make_user(enabled=False),
                    __id__="run_now",
                    __request__=SimpleNamespace(path_params={"action_id": "auto_export_chats.run_now"}),
                    __event_emitter__=emitter,
                )
        finally:
            action_instance._run_lock.release()

        assert result == body
        run_export_job.assert_called_once_with()
        emitter.assert_awaited_once()
        assert emitter.await_args.args[0]["data"]["description"] == "Auto-export already running; skipped."

    @pytest.mark.asyncio
    async def test_action_stop_current_job_requests_stop_and_emits_status(self):
        body = make_body()
        emitter = AsyncMock()
        action_instance = build_filter(Path("/tmp"))
        action_instance._job_running.set()

        result = await action_instance.action(
            body,
            __user__=make_user(enabled=False),
            __id__="stop_current_job",
            __request__=SimpleNamespace(path_params={"action_id": "auto_export_chats.stop_current_job"}),
            __event_emitter__=emitter,
        )

        assert result == body
        assert action_instance._job_stop_requested.is_set()
        emitter.assert_awaited_once()
        assert emitter.await_args.args[0]["data"]["description"] == "Auto-export stop requested."

    @pytest.mark.asyncio
    async def test_action_stop_current_job_reports_idle_when_nothing_is_running(self):
        body = make_body()
        emitter = AsyncMock()
        action_instance = build_filter(Path("/tmp"))

        result = await action_instance.action(
            body,
            __user__=make_user(enabled=False),
            __id__="stop_current_job",
            __request__=SimpleNamespace(path_params={"action_id": "auto_export_chats.stop_current_job"}),
            __event_emitter__=emitter,
        )

        assert result == body
        assert not action_instance._job_stop_requested.is_set()
        emitter.assert_awaited_once()
        assert emitter.await_args.args[0]["data"]["description"] == "No auto-export job is currently running."
"""

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

    def test_cleanup_orphaned_folder_markers_removes_markers_not_in_db(self, worker_instance, tmp_path):
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

        with patch("auto_export_chats.auto_export_chats_action.datetime.datetime") as mock_datetime:
            mock_datetime.fromtimestamp.side_effect = TypeError("invalid timestamp")
            mock_datetime.now.return_value = fake_now
            asyncio.run(ChatExport._export_chat("https://owui.example", chat, {}, str(user_root_dir), datetime.timezone.utc))

        md_files = list(user_root_dir.glob("*.md"))
        assert len(md_files) == 1
        assert md_files[0].name.startswith("2026-03-29_22h39_")

    def test_get_user_timezone_returns_utc_for_unknown_timezone(self):
        with patch("auto_export_chats.auto_export_chats_action.Users.get_user_by_id", return_value=SimpleNamespace(timezone="Nope/Nowhere")):
            assert asyncio.run(_get_user_timezone("user-1")) == datetime.timezone.utc

    def test_get_user_root_dir_uses_sanitized_user_name(self, tmp_path):
        with patch(
            "auto_export_chats.auto_export_chats_action.Users.get_user_by_id",
            return_value=SimpleNamespace(name="Alice Doe"),
        ):
            result = asyncio.run(FolderExport.get_user_export_dir(tmp_path.as_posix(), "user-1"))
            assert result.endswith("Alice_Doe")

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

"""
class TestPolling:
    def test_sync_runtime_valves_stops_running_poller_when_auto_run_disabled(self, filter_instance):
        fake_thread = SimpleNamespace(is_alive=lambda: True)
        filter_instance._poll_thread = fake_thread
        module_valves = Action.Valves(
            SAVE_FOLDER=filter_instance.valves.SAVE_FOLDER,
            OPEN_WEBUI_BASE_URL=filter_instance.valves.OPEN_WEBUI_BASE_URL,
            POLL_INTERVAL_SECONDS=0,
        )

        with patch("auto_export_chats.auto_export_chats_action.valves", module_valves, create=True):
            filter_instance._sync_runtime_valves()

        assert filter_instance._stop_event.is_set()

    def test_sync_runtime_valves_restarts_poller_when_interval_becomes_positive(self, filter_instance):
        filter_instance.valves.POLL_INTERVAL_SECONDS = 0
        started_threads = []

        class FakeThread:
            def __init__(self):
                self.started = False

            def start(self):
                self.started = True
                started_threads.append(self)

            def is_alive(self):
                return self.started

        module_valves = Action.Valves(
            SAVE_FOLDER=filter_instance.valves.SAVE_FOLDER,
            OPEN_WEBUI_BASE_URL=filter_instance.valves.OPEN_WEBUI_BASE_URL,
            POLL_INTERVAL_SECONDS=300,
        )

        with patch.object(filter_instance, "_make_poll_thread", side_effect=FakeThread):
            with patch("auto_export_chats.auto_export_chats_action.valves", module_valves, create=True):
                filter_instance._sync_runtime_valves()

        assert len(started_threads) == 1
        assert filter_instance._poll_thread is started_threads[0]
        assert filter_instance._poll_thread.started is True

    def test_get_next_poll_run_at_returns_none_when_auto_run_disabled(self, filter_instance):
        filter_instance.valves.POLL_INTERVAL_SECONDS = 0
        now = datetime.datetime(2026, 3, 30, 0, 3, 17, tzinfo=datetime.timezone.utc)

        assert filter_instance._get_next_poll_run_at(now) is None

    def test_get_next_poll_run_at_aligns_to_next_utc_boundary(self, filter_instance):
        filter_instance.valves.POLL_INTERVAL_SECONDS = 300
        now = datetime.datetime(2026, 3, 30, 0, 3, 17, tzinfo=datetime.timezone.utc)

        result = filter_instance._get_next_poll_run_at(now)

        assert result == datetime.datetime(2026, 3, 30, 0, 5, 0, tzinfo=datetime.timezone.utc)

    def test_get_next_poll_run_at_moves_past_exact_boundary(self, filter_instance):
        filter_instance.valves.POLL_INTERVAL_SECONDS = 300
        now = datetime.datetime(2026, 3, 30, 0, 5, 0, tzinfo=datetime.timezone.utc)

        result = filter_instance._get_next_poll_run_at(now)

        assert result == datetime.datetime(2026, 3, 30, 0, 10, 0, tzinfo=datetime.timezone.utc)

    def test_is_function_active_reflects_db_state(self, filter_instance):
        with patch("auto_export_chats.auto_export_chats_action.Functions.get_function_by_id", return_value=SimpleNamespace(is_active=False)):
            assert filter_instance._is_function_active() is False

        with patch("auto_export_chats.auto_export_chats_action.Functions.get_function_by_id", return_value=SimpleNamespace(is_active=True)):
            assert filter_instance._is_function_active() is True

    def test_run_export_job_skips_when_a_previous_job_is_running(self, filter_instance):
        filter_instance._run_lock.acquire()

        try:
            with patch.object(filter_instance, "_poll_all_users") as poll_all_users:
                result = filter_instance._run_export_job()
        finally:
            filter_instance._run_lock.release()

        assert result == "busy"
        poll_all_users.assert_not_called()

    def test_request_stop_current_job_returns_idle_when_not_running(self, filter_instance):
        assert filter_instance._request_stop_current_job() == "idle"
        assert not filter_instance._job_stop_requested.is_set()

    def test_request_stop_current_job_sets_stop_flag_when_running(self, filter_instance):
        filter_instance._job_running.set()

        assert filter_instance._request_stop_current_job() == "stopping"
        assert filter_instance._job_stop_requested.is_set()

    def test_poll_all_users_returns_stopped_when_stop_requested_before_user(self, filter_instance):
        filter_instance._job_stop_requested.set()

        with patch.object(filter_instance, "_get_enabled_user_ids", return_value={"user-1"}):
            with patch.object(filter_instance, "_poll_user") as poll_user:
                result = filter_instance._poll_all_users(datetime.datetime.now(datetime.timezone.utc))

        assert result == "stopped"
        poll_user.assert_not_called()

    def test_poll_all_users_does_not_write_state_when_stopped_mid_user(self, filter_instance):
        user_root_dir = os.path.join(filter_instance.valves.SAVE_FOLDER, "alice")
        job_started_at = datetime.datetime(2026, 3, 30, 0, 25, tzinfo=datetime.timezone.utc)

        filter_instance._job_stop_requested.set()  # stop is set after _poll_user runs

        def poll_user_sets_stop(*args, **kwargs):
            filter_instance._job_stop_requested.set()

        with patch.object(filter_instance, "_get_enabled_user_ids", return_value={"user-1"}):
            with patch.object(filter_instance, "_get_user_root_dir", return_value=user_root_dir):
                with patch.object(filter_instance, "_read_last_successful_export_at", return_value=None):
                    with patch.object(filter_instance, "_poll_user", side_effect=poll_user_sets_stop):
                        with patch.object(filter_instance, "_write_state") as write_state:
                            result = filter_instance._poll_all_users(job_started_at)

        assert result == "stopped"
        write_state.assert_not_called()

    def test_poll_user_stops_before_next_chat_when_stop_requested(self, filter_instance):
        current_folders = {}
        since = datetime.datetime.fromtimestamp(1, tz=datetime.timezone.utc)
        job_started_at = datetime.datetime.fromtimestamp(2, tz=datetime.timezone.utc)
        chat_a = make_chat(chat_id="chat-a-123456", updated_at=10)
        user_tz = datetime.timezone.utc
        user_root_dir = os.path.join(filter_instance.valves.SAVE_FOLDER, "alice")

        with patch.object(filter_instance, "_query_folders", return_value=current_folders):
            with patch.object(filter_instance, "_query_chat_ids_to_export", return_value=[chat_a.id, "chat-b"]):
                with patch.object(filter_instance, "_get_user_timezone", return_value=user_tz):
                    with patch.object(filter_instance, "_reconcile_user_folders"):
                        with patch("auto_export_chats.auto_export_chats_action.Chats.get_chat_by_id", return_value=chat_a):
                            with patch.object(filter_instance, "_export_chat") as export_chat:
                                export_chat.side_effect = lambda *args, **kwargs: filter_instance._job_stop_requested.set()
                                filter_instance._poll_user("user-1", since, job_started_at, user_root_dir)

        export_chat.assert_called_once_with(chat_a, {}, user_root_dir, user_tz)

    def test_read_write_state_uses_iso_datetime_in_underscore_file(self, filter_instance, tmp_path):
        job_started_at = datetime.datetime(2026, 3, 29, 22, 39, tzinfo=datetime.timezone.utc)
        user_root_dir = str(tmp_path)

        filter_instance._write_state(user_root_dir, job_started_at)

        state_file = tmp_path / "_auto_export_state.json"
        assert state_file.exists()
        assert state_file.read_text(encoding="utf-8") == (
            "{\n"
            "  \"last_successful_export_at\": \"2026-03-29T22:39:00+00:00\"\n"
            "}\n"
        )
        assert json.loads(state_file.read_text(encoding="utf-8")) == {
            "last_successful_export_at": "2026-03-29T22:39:00+00:00",
        }
        assert filter_instance._read_last_successful_export_at(user_root_dir) == job_started_at

    def test_query_chat_ids_to_export_uses_last_successful_export_at(self, filter_instance):
        since = datetime.datetime.fromtimestamp(200, tz=datetime.timezone.utc)

        mock_query = SimpleNamespace(
            filter_by=lambda **kw: SimpleNamespace(
                filter=lambda cond: SimpleNamespace(
                    all=lambda: [("new-chat",)]
                ),
                all=lambda: [("old-chat",), ("new-chat",), ("folder-chat",)],
            )
        )
        mock_db = SimpleNamespace(query=lambda *args: mock_query)

        from contextlib import contextmanager

        @contextmanager
        def fake_db_context():
            yield mock_db

        with patch("auto_export_chats.auto_export_chats_action.get_db_context", fake_db_context):
            result = Action._query_chat_ids_to_export("user-1", since)

        assert result == ["new-chat"]

    def test_query_chat_ids_to_export_returns_all_when_no_last_export(self, filter_instance):
        mock_query = SimpleNamespace(
            filter_by=lambda **kw: SimpleNamespace(
                all=lambda: [("chat-1",), ("chat-2",)]
            )
        )
        mock_db = SimpleNamespace(query=lambda *args: mock_query)

        from contextlib import contextmanager

        @contextmanager
        def fake_db_context():
            yield mock_db

        with patch("auto_export_chats.auto_export_chats_action.get_db_context", fake_db_context):
            result = Action._query_chat_ids_to_export("user-1", None)

        assert set(result) == {"chat-1", "chat-2"}

    def test_poll_loop_stops_when_function_is_disabled(self, filter_instance):
        with patch.object(filter_instance, "_sync_runtime_valves"):
            with patch.object(filter_instance, "_is_function_active", return_value=False):
                with patch.object(filter_instance, "_run_export_job") as run_export_job:
                    filter_instance._poll_loop()

        assert filter_instance._stop_event.is_set()
        run_export_job.assert_not_called()

    def test_poll_user_exports_changed_chat_ids(self, filter_instance):
        current_folders = {
            "folder-1": _FolderSnapshot(updated_at=2, name="Renamed", parent_id=None),
        }
        chat_obj = make_chat(chat_id="chat-changed", title="After", folder_id="folder-1", tags=["b"])
        since = datetime.datetime.fromtimestamp(1, tz=datetime.timezone.utc)
        job_started_at = datetime.datetime.fromtimestamp(2, tz=datetime.timezone.utc)
        user_tz = datetime.timezone.utc
        user_root_dir = os.path.join(filter_instance.valves.SAVE_FOLDER, "alice")

        with patch.object(filter_instance, "_query_folders", return_value=current_folders):
            with patch.object(filter_instance, "_query_chat_ids_to_export", return_value=["chat-changed"]):
                with patch.object(filter_instance, "_get_user_timezone", return_value=user_tz):
                    with patch.object(filter_instance, "_reconcile_user_folders") as reconcile_user_folders:
                        with patch("auto_export_chats.auto_export_chats_action.Chats.get_chat_by_id", return_value=chat_obj):
                            with patch.object(filter_instance, "_export_chat") as export_chat:
                                filter_instance._poll_user("user-1", since, job_started_at, user_root_dir)

        reconcile_user_folders.assert_called_once_with(user_root_dir, current_folders, {"folder-1": "Renamed"})
        export_chat.assert_called_once_with(chat_obj, {"folder-1": "Renamed"}, user_root_dir, user_tz)

    def test_poll_user_resolves_timezone_once_per_run(self, filter_instance):
        current_folders = {}
        since = datetime.datetime.fromtimestamp(1, tz=datetime.timezone.utc)
        job_started_at = datetime.datetime.fromtimestamp(2, tz=datetime.timezone.utc)
        chat_a = make_chat(chat_id="chat-a-123456", updated_at=10)
        chat_b = make_chat(chat_id="chat-b-123456", updated_at=20)
        user_tz = datetime.timezone(datetime.timedelta(hours=2))
        user_root_dir = os.path.join(filter_instance.valves.SAVE_FOLDER, "alice")

        with patch.object(filter_instance, "_query_folders", return_value=current_folders):
            with patch.object(filter_instance, "_query_chat_ids_to_export", return_value=[chat_a.id, chat_b.id]):
                with patch.object(filter_instance, "_get_user_timezone", return_value=user_tz) as get_user_timezone:
                    with patch.object(filter_instance, "_reconcile_user_folders"):
                        with patch("auto_export_chats.auto_export_chats_action.Chats.get_chat_by_id", side_effect=[chat_a, chat_b]):
                            with patch.object(filter_instance, "_export_chat") as export_chat:
                                filter_instance._poll_user("user-1", since, job_started_at, user_root_dir)

        get_user_timezone.assert_called_once_with("user-1")
        assert export_chat.call_count == 2
        assert export_chat.call_args_list[0].args == (chat_a, {}, user_root_dir, user_tz)
        assert export_chat.call_args_list[1].args == (chat_b, {}, user_root_dir, user_tz)

    def test_poll_all_users_writes_state_per_user_after_success(self, filter_instance):
        user_root_dir = os.path.join(filter_instance.valves.SAVE_FOLDER, "alice")
        job_started_at = datetime.datetime(2026, 3, 30, 0, 25, tzinfo=datetime.timezone.utc)

        with patch.object(filter_instance, "_get_enabled_user_ids", return_value={"user-1"}):
            with patch.object(filter_instance, "_get_user_root_dir", return_value=user_root_dir):
                with patch.object(filter_instance, "_read_last_successful_export_at", return_value=None):
                    with patch.object(filter_instance, "_poll_user"):
                        with patch.object(filter_instance, "_write_state") as write_state:
                            result = filter_instance._poll_all_users(job_started_at)

        assert result == "success"
        write_state.assert_called_once_with(user_root_dir, job_started_at)
"""
