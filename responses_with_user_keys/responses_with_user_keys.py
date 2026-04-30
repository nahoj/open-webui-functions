"""
title: Responses API with Per-User Keys
id: # This will appear as model-ID prefix in the model list.
author: Johan Grande
repository: https://github.com/nahoj/open-webui-functions
version: 0.3
license: MIT
description: Manifold pipe exposing an OpenAI-compatible Responses API endpoint where each user supplies their own API key via UserValves. Reuses Open WebUI's built-in payload conversion, header building, and SSE streaming helpers.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Optional

import aiohttp
from fastapi import Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from open_webui.env import AIOHTTP_CLIENT_SESSION_SSL, AIOHTTP_CLIENT_TIMEOUT
from open_webui.routers.openai import (
    _clean_proxy_headers,
    convert_responses_result,
    convert_to_responses_payload,
    get_headers_and_cookies,
)
from open_webui.utils.misc import stream_chunks_handler
from open_webui.utils.session_pool import (
    cleanup_response,
    get_session,
    stream_wrapper,
)


class Pipe:
    class Valves(BaseModel):
        BASE_URL: str = Field(
            default="https://api.openai.com/v1",
            description="OAI-compatible base URL (no trailing slash). Must speak the Responses API.",
        )
        DEFAULT_API_KEY: str = Field(
            default="",
            description="Admin-side default API key. Used to fetch the model list at startup and as a fallback when a user hasn't set their own.",
        )
        MODEL_FILTER_REGEX: str = Field(
            default=r".*",
            description="Regex to filter models, e.g. `(model-id-partial-match|^model-id-exact-match$|...)`. Use `.*` for no filtering.",
        )

    class UserValves(BaseModel):
        api_key: str = Field(
            default="",
            description="Your personal API key for the upstream provider.",
        )

    def __init__(self):
        self.valves = self.Valves()
        self.logger = logging.getLogger(__name__)
        self.type = "manifold"
        self.id = "responses_with_user_keys"
        self.name = ""

    async def pipes(self) -> list[dict]:
        key = self.valves.DEFAULT_API_KEY
        if not key:
            self.logger.warning("DEFAULT_API_KEY is not set; no models will be listed.")
            return []
        ids = await self._fetch_models(key)
        return [{"id": m, "name": m} for m in ids]

    async def _fetch_models(self, key: str) -> list[str]:
        url = f"{self.valves.BASE_URL.rstrip('/')}/models"
        headers = {"Authorization": f"Bearer {key}"}
        timeout = aiohttp.ClientTimeout(total=10)
        try:
            async with aiohttp.ClientSession(trust_env=True) as sess:
                async with sess.get(
                    url, headers=headers, timeout=timeout, ssl=AIOHTTP_CLIENT_SESSION_SSL
                ) as r:
                    data = await r.json()
        except Exception as e:
            self.logger.error(f"Failed to fetch models from {url}: {e}")
            return []

        items = data.get("data", []) if isinstance(data, dict) else []
        ids = [it["id"] for it in items if isinstance(it, dict) and it.get("id")]

        pattern = self.valves.MODEL_FILTER_REGEX or ""
        if pattern:
            try:
                rx = re.compile(pattern)
                ids = [i for i in ids if rx.search(i)]
            except re.error as e:
                self.logger.error(f"Invalid MODEL_FILTER_REGEX {pattern!r}: {e}")
        return sorted(ids)

    @staticmethod
    def _resolve_key(user_valves: Any, fallback: str) -> str:
        user_key = getattr(user_valves, "api_key", "") if user_valves is not None else ""
        return user_key or fallback

    @staticmethod
    def _strip_manifold_prefix(model_id: str) -> str:
        return model_id.split(".", 1)[1] if "." in model_id else model_id

    async def pipe(
        self,
        body: dict,
        __request__: Request,
        __user__: Optional[dict] = None,
        __metadata__: Optional[dict] = None,
    ):
        key = self._resolve_key((__user__ or {}).get("valves"), self.valves.DEFAULT_API_KEY)
        if not key:
            return {
                "error": {
                    "message": (
                        "No API key set. Open Settings → Account → Valves "
                        "for the 'responses_with_user_keys' function and set your api_key."
                    )
                }
            }

        body = {**body, "model": self._strip_manifold_prefix(body.get("model", ""))}
        url = self.valves.BASE_URL.rstrip("/")
        api_config = {"auth_type": "bearer"}

        payload = convert_to_responses_payload(body)

        headers, cookies = await get_headers_and_cookies(
            __request__, url, key, api_config, __metadata__, user=__user__
        )

        timeout = aiohttp.ClientTimeout(total=AIOHTTP_CLIENT_TIMEOUT)

        session = await get_session()
        r = None
        streaming = False
        try:
            r = await session.request(
                method="POST",
                url=f"{url}/responses",
                data=json.dumps(payload),
                headers=headers,
                cookies=cookies,
                ssl=AIOHTTP_CLIENT_SESSION_SSL,
                timeout=timeout,
            )

            if "text/event-stream" in r.headers.get("Content-Type", ""):
                if r.status >= 400:
                    error_body = await r.text()
                    try:
                        return JSONResponse(status_code=r.status, content=json.loads(error_body))
                    except json.JSONDecodeError:
                        return JSONResponse(
                            status_code=r.status,
                            content={"error": {"message": error_body, "code": r.status}},
                        )
                streaming = True
                return StreamingResponse(
                    stream_wrapper(r, content_handler=stream_chunks_handler),
                    status_code=r.status,
                    headers=_clean_proxy_headers(r.headers),
                )

            try:
                data = await r.json()
            except Exception:
                data = await r.text()

            if r.status >= 400:
                if isinstance(data, (dict, list)):
                    return JSONResponse(status_code=r.status, content=data)
                return JSONResponse(
                    status_code=r.status,
                    content={"error": {"message": str(data), "code": r.status}},
                )

            if isinstance(data, dict):
                data = convert_responses_result(data)
            return data

        except Exception as e:
            self.logger.exception("responses_with_user_keys upstream error")
            return {"error": {"message": str(e)}}
        finally:
            if not streaming and r is not None:
                await cleanup_response(r)
