This repository contains Functions, i.e., plugins, for [Open WebUI](https://github.com/open-webui/open-webui).

## Global System Prompt

A filter function that injects a default system prompt into all conversations (similar to the user system prompt, but for all users), unless the model in use has specific tags.

Example use case: You want custom instructions to apply to all conversations for general-purpose assistant use, but you don't want them for roleplay. → Use the filter with `skip_tags = "character, roleplay"` (and make sure your models are tagged appropriately).
