"""
title: OpenAI Responses Filter
author: Johan Grande
repository: https://github.com/nahoj/open-webui-functions
version: 0.2
license: MIT
description: For connections that speak the OpenAI Responses API, translate Open WebUI's UI controls into native Responses fields: the top-level `reasoning_effort` param becomes `reasoning.effort`, and the built-in web-search toggle becomes a provider-side `tools: [{"type": "web_search"}]` with `include: ["web_search_call.action.sources"]`.
"""

import logging
from typing import Optional

from pydantic import BaseModel, Field


class Filter:
    class Valves(BaseModel):
        TRANSLATE_WEB_SEARCH: bool = Field(
            default=True,
            description="Turn Open WebUI's web-search toggle into a provider-side `web_search` tool instead of OWUI's own retrieval-based search.",
        )
        REASONING_SUMMARY: str = Field(
            default="auto",
            description="Value for `reasoning.summary` so the model's reasoning trace is returned (e.g. `auto`, `concise`, `detailed`). Leave empty to not set it.",
        )
        priority: int = Field(
            default=10,
            description="Filter priority (lower runs first, OWUI default = 0).",
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
        self._apply_reasoning_effort(body)

        if self.valves.REASONING_SUMMARY:
            self._set_reasoning_summary(body)

        if self.valves.TRANSLATE_WEB_SEARCH:
            self._translate_web_search(body)

        return body

    def _apply_reasoning_effort(self, body: dict) -> None:
        # OWUI's default `reasoning_effort` (a Chat Completions param) is rejected
        # by the Responses API. Move it into `reasoning.effort`.
        effort = body.pop("reasoning_effort", None)
        if not effort:
            return

        reasoning = body.get("reasoning")
        if not isinstance(reasoning, dict):
            reasoning = {}
        reasoning["effort"] = effort
        body["reasoning"] = reasoning
        self.logger.info("Set reasoning.effort=%s", effort)

    def _set_reasoning_summary(self, body: dict) -> None:
        # Ask the Responses API to return a reasoning summary so OWUI can render
        # the model's thinking. Don't clobber an explicitly-set summary.
        reasoning = body.get("reasoning")
        if not isinstance(reasoning, dict):
            reasoning = {}
        reasoning.setdefault("summary", self.valves.REASONING_SUMMARY)
        body["reasoning"] = reasoning

    def _translate_web_search(self, body: dict) -> None:
        # The web-search toggle arrives as body["features"]["web_search"] and is
        # consumed later in the request pipeline (after filter inlets run). If it
        # is on, switch it off so OWUI doesn't run its own retrieval search, and
        # inject OpenAI's provider-side web_search tool instead.
        features = body.get("features")
        if not isinstance(features, dict) or not features.get("web_search"):
            return

        features["web_search"] = False

        tools = body.get("tools")
        if not isinstance(tools, list):
            tools = []
        if not any(isinstance(t, dict) and t.get("type") == "web_search" for t in tools):
            tools.append({"type": "web_search"})
        body["tools"] = tools

        include = body.get("include")
        if not isinstance(include, list):
            include = []
        if "web_search_call.action.sources" not in include:
            include.append("web_search_call.action.sources")
        body["include"] = include

        self.logger.info("Translated web-search toggle to provider-side web_search tool")
