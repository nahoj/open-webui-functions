"""
Integration tests for Auto-Export Chats.

These tests exercise the full export flow end-to-end:
- Database layer is mocked
- Files are actually written to disk
- Tests are written in Given/When/Then style for readability
"""

import asyncio
import datetime
import json
import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from auto_export_chats.auto_export_chats_action import Action, ExportWorker, ChatExport, SingleUserExport, AllUserExport

# -----------------------------------------------------------------------------
# Test Data Builders
# -----------------------------------------------------------------------------

def make_chat(chat_id="chat-abc123456789", title="Test Chat", folder_id=None,
              user_id="user-alice", updated_at=1712345678, tags=None, messages=None):
    """Build a chat object simulating OpenWebUI's Chat model."""
    default_messages = {
        "user-1": {"role": "user", "content": "Hello assistant", "parentId": None},
        "assistant-1": {"role": "assistant", "content": "Hello! How can I help?", "parentId": "user-1"},
    }
    return SimpleNamespace(
        id=chat_id,
        title=title,
        folder_id=folder_id,
        user_id=user_id,
        updated_at=updated_at,
        meta={"tags": tags or ["test-tag"]},
        chat={"history": {"currentId": "assistant-1", "messages": messages or default_messages}}
    )


def make_folder(folder_id, name, parent_id=None, updated_at=1):
    """Build a folder snapshot."""
    from auto_export_chats.auto_export_chats_action import _FolderSnapshot
    return _FolderSnapshot(updated_at=updated_at, name=name, parent_id=parent_id)


def make_user(user_id, name, timezone="UTC", enabled=True):
    """Build a user object with settings."""
    settings = {
        "functions": {
            "valves": {
                "auto_export_chats": {"ENABLED": enabled}
            }
        }
    }
    return SimpleNamespace(
        id=user_id,
        name=name,
        timezone=timezone,
        settings=settings
    )


# -----------------------------------------------------------------------------
# Integration Test Suite
# -----------------------------------------------------------------------------

class TestAutoExportIntegration:
    """
    End-to-end integration tests for the auto-export functionality.

    These tests verify that:
    - Chats are exported to the correct filesystem locations
    - Folder hierarchies are mirrored from OWUI
    - State is tracked correctly between runs
    - Only changed chats are exported on subsequent runs
    """

    @pytest.fixture
    def export_root(self, tmp_path):
        """Provide a temporary directory for exports."""
        return tmp_path / "exports"

    @pytest.fixture
    def action(self, export_root):
        """Provide a configured Action instance."""
        act = Action.__new__(Action)
        act.valves = Action.Valves(
            EXPORT_DIR=str(export_root),
            OPEN_WEBUI_BASE_URL="https://owui.example.com",
            POLL_INTERVAL_SECONDS=0,  # Disable background polling
        )
        act._function_id = "auto_export_chats"
        act.logger = MagicMock()
        worker = ExportWorker.__new__(ExportWorker)
        worker.valves = act.valves
        worker.function_id = act._function_id
        worker.logger = act.logger
        worker._task = SimpleNamespace(cancel=lambda: None)
        act._worker = worker
        return act

    @contextmanager
    def mock_db(self, users=None, chats=None, folders=None, function_active=True):
        """Context manager to mock all database dependencies."""
        users = users or []
        chats = chats or {}
        folders = folders or {}

        # Mock Users.get_users
        def mock_get_users(limit=None):
            return {"users": users}

        # Mock Users.get_user_by_id
        def mock_get_user_by_id(user_id):
            for u in users:
                if u.id == user_id:
                    return u
            return None

        # Mock Chats.get_chat_by_id
        def mock_get_chat_by_id(chat_id):
            return chats.get(chat_id)

        # Mock Functions.get_function_by_id
        def mock_get_function_by_id(function_id):
            return SimpleNamespace(is_active=function_active)

        # Mock _query_folders static method
        async def mock_query_folders(user_id):
            return folders

        # Mock _query_chat_ids_to_export static method
        async def mock_query_chat_ids_to_export(user_id, since):
            result = []
            for cid, chat in chats.items():
                if chat.user_id == user_id:
                    if since is None or chat.updated_at > since.timestamp():
                        result.append(cid)
            return result

        patches = [
            patch("auto_export_chats.auto_export_chats_action.Users.get_users", mock_get_users),
            patch("auto_export_chats.auto_export_chats_action.Users.get_user_by_id", mock_get_user_by_id),
            patch("auto_export_chats.auto_export_chats_action.Chats.get_chat_by_id", mock_get_chat_by_id),
            patch.object(SingleUserExport, "_query_folders", staticmethod(mock_query_folders)),
            patch.object(ChatExport, "_query_chat_ids_to_export", staticmethod(mock_query_chat_ids_to_export)),
        ]

        for p in patches:
            p.start()
        try:
            yield
        finally:
            for p in patches:
                p.stop()

    @staticmethod
    def _run_export_job(action):
        return asyncio.run(AllUserExport.run(action.valves))

    # -------------------------------------------------------------------------
    # Scenario: First-time export for a new user
    # -------------------------------------------------------------------------

    def test_given_new_user_with_chats_when_export_runs_then_files_created_in_user_folder(self, action, export_root):
        """
        Given: A user with auto-export enabled has chats but no prior exports
        When: The export job runs
        Then: All chats are exported to a folder named after the user
        """
        # Given
        user = make_user("user-alice", "Alice", enabled=True)
        chat = make_chat(chat_id="chat-123", title="My First Chat", user_id="user-alice")

        with self.mock_db(users=[user], chats={"chat-123": chat}):
            # When
            self._run_export_job(action)

        # Then
        user_dir = export_root / "Alice"
        assert user_dir.exists(), "User directory should be created"

        md_files = list(user_dir.glob("*.md"))
        assert len(md_files) == 1, "Exactly one markdown file should be created"

        content = md_files[0].read_text()
        assert "# My First Chat" in content
        assert "Hello assistant" in content
        assert "https://owui.example.com/c/chat-123" in content

    # -------------------------------------------------------------------------
    # Scenario: Export with folder hierarchy
    # -------------------------------------------------------------------------

    def test_given_nested_folders_when_chat_exported_then_folder_structure_mirrored(self, action, export_root):
        """
        Given: A chat is in a nested folder structure (Projects > Work > Q1)
        When: The export job runs
        Then: The folder hierarchy is created and the chat is in the leaf folder
        """
        # Given
        user = make_user("user-bob", "Bob", enabled=True)
        folders = {
            "folder-root": make_folder("folder-root", "Projects"),
            "folder-work": make_folder("folder-work", "Work", parent_id="folder-root"),
            "folder-q1": make_folder("folder-q1", "Q1", parent_id="folder-work"),
        }
        chat = make_chat(chat_id="chat-456", title="Q1 Planning",
                        user_id="user-bob", folder_id="folder-q1")

        with self.mock_db(users=[user], chats={"chat-456": chat}, folders=folders):
            # When
            self._run_export_job(action)

        # Then
        expected_path = export_root / "Bob" / "Projects" / "Work" / "Q1"
        assert expected_path.exists(), f"Expected path {expected_path} to exist"

        md_files = list(expected_path.glob("*.md"))
        assert len(md_files) == 1
        assert "Q1 Planning" in md_files[0].read_text()

    # -------------------------------------------------------------------------
    # Scenario: Incremental export (only new/changed chats)
    # -------------------------------------------------------------------------

    def test_given_previous_export_when_only_some_chats_changed_then_only_changed_exported(self, action, export_root):
        """
        Given: Previous export exists with state tracking
        When: New export runs and only some chats have been modified
        Then: Only the modified chats are re-exported
        """
        # Given - First export: two chats (use realistic recent timestamps)
        base_time = int(datetime.datetime.now().timestamp())
        user = make_user("user-carol", "Carol", enabled=True)
        old_chat = make_chat(chat_id="chat-old", title="Old Chat",
                            user_id="user-carol", updated_at=base_time - 1000)
        new_chat = make_chat(chat_id="chat-new", title="New Chat",
                            user_id="user-carol", updated_at=base_time)

        with self.mock_db(users=[user],
                         chats={"chat-old": old_chat, "chat-new": new_chat}):
            self._run_export_job(action)

        user_dir = export_root / "Carol"
        initial_files = set(f.name for f in user_dir.glob("*.md"))
        assert len(initial_files) == 2

        # Simulate time passing - only new_chat is "modified" now with newer timestamp
        updated_chat = make_chat(chat_id="chat-new", title="Updated Chat",
                                user_id="user-carol", updated_at=base_time + 1000)

        with self.mock_db(users=[user],
                         chats={"chat-old": old_chat, "chat-new": updated_chat}):
            # When - Second export run
            self._run_export_job(action)

        # Then
        final_files = set(f.name for f in user_dir.glob("*.md"))
        # Should still have 2 files, but one should have "Updated" in name
        assert len(final_files) == 2

        current_content = " ".join(f.read_text() for f in user_dir.glob("*.md"))
        assert "Updated Chat" in current_content
        assert "Old Chat" in current_content  # Still present, not re-exported

    # -------------------------------------------------------------------------
    # Scenario: Folder rename handling
    # -------------------------------------------------------------------------

    def test_given_folder_renamed_when_export_runs_then_files_moved_to_new_location(self, action, export_root):
        """
        Given: A chat was exported to folder "OldName"
        When: The folder is renamed to "NewName" in OWUI and export runs
        Then: The folder on disk is renamed and the chat file moves with it
        """
        # Given - Initial export with old folder name
        user = make_user("user-dave", "Dave", enabled=True)
        old_folders = {"folder-1": make_folder("folder-1", "OldName")}
        chat = make_chat(chat_id="chat-789", title="My Chat",
                        user_id="user-dave", folder_id="folder-1")

        with self.mock_db(users=[user], chats={"chat-789": chat}, folders=old_folders):
            self._run_export_job(action)

        old_path = export_root / "Dave" / "OldName"
        assert old_path.exists()
        old_files = list(old_path.glob("*.md"))
        assert len(old_files) == 1

        # When - Folder renamed in OWUI
        new_folders = {"folder-1": make_folder("folder-1", "NewName")}

        with self.mock_db(users=[user], chats={"chat-789": chat}, folders=new_folders):
            self._run_export_job(action)

        # Then
        new_path = export_root / "Dave" / "NewName"
        assert new_path.exists(), "New folder path should exist"
        assert not old_path.exists(), "Old folder path should be gone"
        assert any(new_path.glob("*.md")), "Chat file should be in new location"

    # -------------------------------------------------------------------------
    # Scenario: Chat moved to different folder
    # -------------------------------------------------------------------------

    def test_given_chat_moved_to_different_folder_when_export_runs_then_file_relocated(self, action, export_root):
        """
        Given: A chat was exported to folder "Inbox"
        When: The chat is moved to "Archive" in OWUI and export runs
        Then: The chat file is removed from Inbox and created in Archive
        """
        # Given
        base_time = int(datetime.datetime.now().timestamp())
        user = make_user("user-eve", "Eve", enabled=True)
        folders = {
            "inbox": make_folder("inbox", "Inbox"),
            "archive": make_folder("archive", "Archive"),
        }
        chat = make_chat(chat_id="chat-move", title="To Archive",
                        user_id="user-eve", folder_id="inbox", updated_at=base_time)

        with self.mock_db(users=[user], chats={"chat-move": chat}, folders=folders):
            self._run_export_job(action)

        inbox_path = export_root / "Eve" / "Inbox"
        assert any("chat-move" in f.name for f in inbox_path.glob("*.md")), \
            "Chat should initially be in Inbox"

        # When - Chat moved to Archive (timestamp well after first export)
        moved_chat = make_chat(chat_id="chat-move", title="To Archive",
                              user_id="user-eve", folder_id="archive",
                              updated_at=base_time + 3600)  # 1 hour later

        with self.mock_db(users=[user], chats={"chat-move": moved_chat}, folders=folders):
            self._run_export_job(action)

        # Then
        archive_path = export_root / "Eve" / "Archive"
        assert any("chat-move" in f.name for f in archive_path.glob("*.md")), \
            "Chat should be in Archive"
        assert not any("chat-move" in f.name for f in inbox_path.glob("*.md")), \
            "Chat should no longer be in Inbox"

    # -------------------------------------------------------------------------
    # Scenario: Multiple users with different settings
    # -------------------------------------------------------------------------

    def test_given_multiple_users_when_only_some_enabled_then_only_enabled_exported(self, action, export_root):
        """
        Given: Multiple users exist, but only some have auto-export enabled
        When: The export job runs
        Then: Only enabled users have their chats exported
        """
        # Given
        enabled_user = make_user("user-enabled", "EnabledUser", enabled=True)
        disabled_user = make_user("user-disabled", "DisabledUser", enabled=False)

        enabled_chat = make_chat(chat_id="chat-enabled", title="Enabled Chat",
                                user_id="user-enabled")
        disabled_chat = make_chat(chat_id="chat-disabled", title="Disabled Chat",
                                 user_id="user-disabled")

        with self.mock_db(
            users=[enabled_user, disabled_user],
            chats={"chat-enabled": enabled_chat, "chat-disabled": disabled_chat}
        ):
            # When
            self._run_export_job(action)

        # Then
        assert (export_root / "EnabledUser").exists(), "Enabled user should have exports"
        assert not (export_root / "DisabledUser").exists(), "Disabled user should not have exports"

    # -------------------------------------------------------------------------
    # Scenario: Export state persistence
    # -------------------------------------------------------------------------

    def test_given_export_completes_when_state_written_then_subsequent_run_uses_state(self, action, export_root):
        """
        Given: An export job completes successfully
        When: The state file is written and a new job starts
        Then: The next job reads the state and only exports chats newer than last run
        """
        # Given - First run exports chat
        base_time = int(datetime.datetime.now().timestamp())
        user = make_user("user-frank", "Frank", enabled=True)
        first_chat = make_chat(chat_id="chat-first", title="First",
                              user_id="user-frank", updated_at=base_time)

        with self.mock_db(users=[user], chats={"chat-first": first_chat}):
            self._run_export_job(action)

        # Verify state file exists
        user_dir = export_root / "Frank"
        state_file = user_dir / "_auto_export_state.json"
        assert state_file.exists(), "State file should track last export time"

        state = json.loads(state_file.read_text())
        assert "last_successful_export_at" in state

        # When - Second run with newer chat only
        second_chat = make_chat(chat_id="chat-second", title="Second",
                               user_id="user-frank", updated_at=base_time + 1000)

        call_count = {"count": 0}
        original_get_chat = lambda cid: {"chat-first": first_chat, "chat-second": second_chat}.get(cid)

        def tracking_get_chat(cid):
            call_count["count"] += 1
            return original_get_chat(cid)

        with self.mock_db(users=[user], chats={"chat-first": first_chat, "chat-second": second_chat}):
            with patch("auto_export_chats.auto_export_chats_action.Chats.get_chat_by_id") as mock_get:
                mock_get.side_effect = tracking_get_chat
                self._run_export_job(action)

        # Then - Only the new chat should have been fetched (old one filtered by timestamp)
        # Note: get_chat_by_id is only called for chats that pass the timestamp filter
        assert call_count["count"] == 1, "Only new chat should be processed"

    # -------------------------------------------------------------------------
    # Scenario: Folder deletion cleanup
    # -------------------------------------------------------------------------

    def test_given_folder_deleted_in_owui_when_export_runs_then_marker_removed_but_files_preserved(self, action, export_root):
        """
        Given: A folder was exported with its marker file
        When: The folder is deleted in OWUI and export runs
        Then: The marker file is removed but the folder and chat files remain
        """
        # Given - Export with folder
        user = make_user("user-grace", "Grace", enabled=True)
        folders = {"folder-del": make_folder("folder-del", "ToDelete")}
        chat = make_chat(chat_id="chat-del", title="In Deleted Folder",
                        user_id="user-grace", folder_id="folder-del")

        with self.mock_db(users=[user], chats={"chat-del": chat}, folders=folders):
            self._run_export_job(action)

        folder_path = export_root / "Grace" / "ToDelete"
        marker_file = folder_path / ".open_webui_id=folder-del"
        assert marker_file.exists(), "Marker file should exist after export"

        # When - Folder deleted in OWUI (empty folders dict)
        with self.mock_db(users=[user], chats={"chat-del": chat}, folders={}):
            self._run_export_job(action)

        # Then
        assert not marker_file.exists(), "Marker should be removed for deleted folder"
        assert folder_path.exists(), "Folder should still exist"
        assert any(folder_path.glob("*.md")), "Chat files should be preserved"

    # -------------------------------------------------------------------------
    # Scenario: Untitled chat handling
    # -------------------------------------------------------------------------

    def test_given_untitled_chat_when_exported_then_uses_fallback_name(self, action, export_root):
        """
        Given: A chat has no title (None or empty)
        When: The chat is exported
        Then: The filename uses "Untitled_Conversation" and the heading reflects this
        """
        # Given
        user = make_user("user-henry", "Henry", enabled=True)
        chat = make_chat(chat_id="chat-untitled", title=None,
                        user_id="user-henry")

        with self.mock_db(users=[user], chats={"chat-untitled": chat}):
            self._run_export_job(action)

        # Then
        user_dir = export_root / "Henry"
        md_files = list(user_dir.glob("*.md"))
        assert len(md_files) == 1

        assert "Untitled_Conversation" in md_files[0].name
        content = md_files[0].read_text()
        assert "# Untitled Conversation" in content

    # -------------------------------------------------------------------------
    # Scenario: Chat with tags in frontmatter
    # -------------------------------------------------------------------------

    def test_given_chat_with_multiple_tags_when_exported_then_tags_in_frontmatter(self, action, export_root):
        """
        Given: A chat has multiple tags in OWUI
        When: The chat is exported
        Then: All tags appear in the YAML frontmatter, plus the default 'ai_chat' tag
        """
        # Given
        user = make_user("user-ivy", "Ivy", enabled=True)
        chat = make_chat(chat_id="chat-tags", title="Tagged Chat",
                        user_id="user-ivy", tags=["important", "work", "follow-up"])

        with self.mock_db(users=[user], chats={"chat-tags": chat}):
            self._run_export_job(action)

        # Then
        user_dir = export_root / "Ivy"
        md_files = list(user_dir.glob("*.md"))
        assert len(md_files) == 1
        content = md_files[0].read_text()

        assert "tags:" in content
        assert "  - ai_chat" in content
        assert "  - important" in content
        assert "  - work" in content
        assert "  - follow-up" in content

    # -------------------------------------------------------------------------
    # Scenario: Concurrent chat modification handling
    # -------------------------------------------------------------------------

    def test_given_chat_modified_during_export_when_job_runs_then_old_export_cleaned_up(self, action, export_root):
        """
        Given: A chat was previously exported
        When: The chat is modified and re-exported
        Then: The old export file is removed and replaced with the new one
        """
        # Given - Initial export with realistic timestamp
        base_time = int(datetime.datetime.now().timestamp())
        user = make_user("user-jack", "Jack", enabled=True)
        original_chat = make_chat(chat_id="chat-mod", title="Original Title",
                                 user_id="user-jack", updated_at=base_time)

        with self.mock_db(users=[user], chats={"chat-mod": original_chat}):
            self._run_export_job(action)

        user_dir = export_root / "Jack"
        original_files = list(user_dir.glob("*chat-mod*.md"))
        assert len(original_files) == 1
        original_name = original_files[0].name

        # When - Chat modified with new title and newer timestamp
        modified_chat = make_chat(chat_id="chat-mod", title="New Title",
                                 user_id="user-jack", updated_at=base_time + 1000)

        with self.mock_db(users=[user], chats={"chat-mod": modified_chat}):
            self._run_export_job(action)

        # Then
        current_files = list(user_dir.glob("*chat-mod*.md"))
        assert len(current_files) == 1, "Should still have exactly one file for this chat"

        new_name = current_files[0].name
        assert new_name != original_name, "Filename should change with new title/timestamp"
        assert "New Title" in current_files[0].read_text()

    # -------------------------------------------------------------------------
    # Scenario: Empty chat handling
    # -------------------------------------------------------------------------

    def test_given_empty_chat_when_exported_then_creates_minimal_file(self, action, export_root):
        """
        Given: A chat exists but has no messages
        When: The chat is exported
        Then: A minimal markdown file is created with just the header
        """
        # Given
        user = make_user("user-kate", "Kate", enabled=True)
        empty_chat = make_chat(chat_id="chat-empty", title="Empty Chat",
                              user_id="user-kate", messages={})
        empty_chat.chat = {"history": {"currentId": None, "messages": {}}}

        with self.mock_db(users=[user], chats={"chat-empty": empty_chat}):
            self._run_export_job(action)

        # Then
        user_dir = export_root / "Kate"
        files = list(user_dir.glob("*.md"))
        assert len(files) == 1

        content = files[0].read_text()
        assert "# Empty Chat" in content
        assert "---" in content  # Has frontmatter
        assert "## User" not in content  # No message sections
