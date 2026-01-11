"""
title: Global System Prompt
description: Injects a default system prompt unless the model has specific tags.
author: Johan Grande
repository: https://github.com/nahoj/open-webui-functions
version: 1.0
license: MIT
"""

import logging
from typing import List

from pydantic import BaseModel, Field

from open_webui.utils.misc import add_or_update_system_message


class Filter:
    class Valves(BaseModel):
        system_prompt: str = Field(
            default="",
            description="The system prompt to inject by default",
        )
        skip_tags: List[str] = Field(
            default=["tag1", "tag2"],
            description="List of model tags that opt out of the global prompt",
        )
        priority: int = Field(
            default=-1,
            description="Filter priority (lower runs first, OWUI default = 0)",
        )
        log_level: str = Field(
            default="INFO",
            description="Logging level (e.g., DEBUG, INFO, WARNING, ERROR)",
        )

    def __init__(self):
        self.valves = self.Valves()
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(self.valves.log_level.upper())

    def inlet(self, body: dict, __model__: dict, **_) -> dict:
        if not self.valves.system_prompt:
            return body

        model_tags = __model__["info"]["meta"].get("tags", [])

        if any(tag["name"] in self.valves.skip_tags for tag in model_tags):
            return body

        body["messages"] = add_or_update_system_message(
            self.valves.system_prompt,
            body.get("messages", []),
        )
        return body
