"""
title: Global System Prompt
author: Johan Grande
repository: https://github.com/nahoj/open-webui-functions
version: 1.1
license: MIT
description: Inject a system prompt into all chats unless the model has specific tags.
"""

import logging
from functools import reduce
from typing import List, Optional

from pydantic import BaseModel, Field

from open_webui.utils.misc import add_or_update_system_message


def _safe_get(data, path, default=None):
    return reduce(lambda d, key: d.get(key, default) if isinstance(d, dict) else default, path, data)


class Filter:
    class Valves(BaseModel):
        SYSTEM_PROMPT: str = Field(
            default="",
            description="The system prompt to inject",
        )
        SKIP_TAGS: List[str] = Field(
            default=["tag1", "tag2"],
            description="List of model tags that opt out of the global prompt",
        )
        PRIORITY: int = Field(
            default=-1,
            description="Filter priority (lower runs first, OWUI default = 0)",
        )

    def __init__(self):
        self.valves = self.Valves()
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    def inlet(
        self,
        body: dict,
        __metadata__: Optional[dict] = None,
        __model__: Optional[dict] = None,
        __user__: Optional[dict] = None,
    ) -> dict:
        if not __user__ or not _safe_get(__metadata__, ["chat_id"], ""):
            return body

        if not self.valves.SYSTEM_PROMPT:
            return body

        model_tags = _safe_get(__model__, ["info", "meta", "tags"], [])

        if any(_safe_get(tag, ["name"], "") in self.valves.SKIP_TAGS for tag in model_tags):
            return body

        body["messages"] = add_or_update_system_message(
            self.valves.SYSTEM_PROMPT,
            body.get("messages", []),
        )
        return body
