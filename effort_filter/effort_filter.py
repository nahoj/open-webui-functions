"""
title: Reasoning Effort Filter
author: Johan Grande
repository: https://github.com/nahoj/open-webui-functions
version: 0.1
license: MIT
description: Set a default reasoning effort on every request via a valve. Emits the Chat-Completions-style top-level `reasoning_effort` field.
"""

import logging
from typing import Literal, Optional

from pydantic import BaseModel, Field


class Filter:
    class Valves(BaseModel):
        REASONING_EFFORT: Literal["", "none", "minimal", "low", "medium", "high", "xhigh"] = Field(
            default="medium",
            description="Set the top-level `reasoning_effort` on every request (empty = leave unset).",
        )
        priority: int = Field(
            default=-10,
            description="Filter priority (lower runs first, OWUI default = 0). Keep this below any filter that translates `reasoning_effort`.",
        )

    def __init__(self):
        self.valves = self.Valves()
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.toggle = True

    def inlet(
        self,
        body: dict,
        __metadata__: Optional[dict] = None,
    ) -> dict:
        effort = self.valves.REASONING_EFFORT
        if not effort:
            return body

        body["reasoning_effort"] = effort
        self.logger.info("Set reasoning_effort=%s", effort)
        return body
