"""
title: Global System Prompt
author: Johan Grande
repository: https://github.com/nahoj/open-webui-functions
version: 2.0
license: MIT
description: Inject a system prompt into all chats, filtering on model tags. The filter can also be manually added to individual models.
"""

import logging
from datetime import datetime
from functools import reduce
from typing import List, Optional
from zoneinfo import ZoneInfo

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
        INCLUDE_TAGS: List[str] = Field(
            default=[],
            description="If non-empty, only apply the prompt to models having at least one of these tags",
        )
        EXCLUDE_TAGS: List[str] = Field(
            default=["exclude_tag_1", "exclude_tag_2"],
            description="Never apply the prompt to models having at least one of these tags (takes precedence over INCLUDE_TAGS)",
        )
        APPEND_DATE: bool = Field(
            default=True,
            description="Append the current date to the system prompt",
        )
        priority: int = Field(
            default=-10,
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

        model_tag_names = _safe_get(__model__, ["info", "meta", "tags", "name"], [])

        if any(tag_name in self.valves.EXCLUDE_TAGS for tag_name in model_tag_names):
            return body

        if self.valves.INCLUDE_TAGS and not any(
            tag_name in self.valves.INCLUDE_TAGS for tag_name in model_tag_names
        ):
            return body

        prompt = self.valves.SYSTEM_PROMPT + "\n"
        if self.valves.APPEND_DATE:
            today = datetime.now(tz=ZoneInfo(__user__["timezone"] or "UTC")).date()
            prompt = f"{prompt}\nCurrent date: {today.strftime('%A %Y-%m-%d')}\n"
        body["messages"] = add_or_update_system_message(
            prompt,
            body.get("messages", []),
            append=False,
        )
        return body
