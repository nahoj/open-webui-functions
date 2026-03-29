This repository contains plugins for [Open WebUI](https://github.com/open-webui/open-webui).

## Auto-Export Chats

A background job that periodically exports chats to Markdown files.
- Supports folders, tags, opt-in per user.
- 2 action buttons for manual control.

**Example use cases:** Read your chats in a notes app such as Nextcloud Notes or Obsidian, gather chats from multiple apps, backup.

## Global System Prompt

A filter function that injects a system prompt into all conversations (similar to the user system prompt, but for all users), unless the model in use has specific tags.

This prompt is added to the existing system prompt that contains model + folder + user or chat system prompt.

**Example use case:** You want custom instructions to apply to all conversations for general-purpose assistant use, but you don't want them for roleplay. → Use the filter with `skip_tags` set to "character, roleplay" (and make sure your models are tagged appropriately).
