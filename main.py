from __future__ import annotations

import asyncio
import contextlib
import html
import json
import logging
import os
import re
import time
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import websockets
from dotenv import load_dotenv


LOGGER = logging.getLogger("bili_qq_monitor")
GROUP_EVENT_INTENT = 1 << 25
QQ_API_BASE = "https://api.sgroup.qq.com"
QQ_TOKEN_URL = "https://bots.qq.com/app/getAppAccessToken"
BILI_VIEW_URL = "https://api.bilibili.com/x/web-interface/view"
BILI_REPLY_URL = "https://api.bilibili.com/x/v2/reply"
BILI_REPLY_MAIN_URL = "https://api.bilibili.com/x/v2/reply/main"
BILI_SUB_REPLY_URL = "https://api.bilibili.com/x/v2/reply/reply"
BILI_DYNAMIC_SPACE_URL = "https://api.bilibili.com/x/polymer/web-dynamic/v1/feed/space"
BILI_DYNAMIC_SPACE_DESKTOP_URL = "https://api.bilibili.com/x/polymer/web-dynamic/desktop/v1/feed/space"
BILI_DYNAMIC_DETAIL_URL = "https://api.bilibili.com/x/polymer/web-dynamic/v1/detail"
BILI_DYNAMIC_DETAIL_DESKTOP_URL = "https://api.bilibili.com/x/polymer/web-dynamic/desktop/v1/detail"
BILI_USER_SEARCH_URL = "https://api.bilibili.com/x/web-interface/search/type"
BILI_ROOT_SORT = 0
BILI_REPLY_MODE_LATEST = 2
BILI_REPLY_PAGE_SIZE = 20
DYNAMIC_UNAVAILABLE_CODES = {-404, 404, 410}
DYNAMIC_UNAVAILABLE_KEYWORDS = (
    "删除",
    "不存在",
    "失效",
    "不可见",
    "仅自己可见",
)
BVID_RE = re.compile(r"BV[0-9A-Za-z]{10}", re.IGNORECASE)
USER_MID_RE = re.compile(r"\d{1,20}")
HTML_TAG_RE = re.compile(r"<[^>]+>")
INITIAL_STATE_RE = re.compile(r"__INITIAL_STATE__\s*=\s*(\{.*?\});\s*\(function", re.S)
DEFAULT_HEADERS = {"User-Agent": "bili-qq-monitor/1.0"}
BILI_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.bilibili.com/",
}
BILI_HTML_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.bilibili.com/",
    "Accept-Language": "zh-CN,zh;q=0.9",
}


def normalize_bvid(value: str) -> str | None:
    value = value.strip()
    if len(value) != 12 or not BVID_RE.fullmatch(value):
        return None
    return "BV" + value[2:]


def normalize_sessdata(value: str) -> str | None:
    value = value.strip()
    if not value:
        return None
    if value.startswith("SESSDATA="):
        value = value[len("SESSDATA="):]
    return value.rstrip(";").strip() or None


def extract_dynamic_comment_thread(item: dict[str, Any]) -> tuple[str | None, int]:
    basic = item.get("basic") or {}
    oid = str(basic.get("comment_id_str") or basic.get("rid_str") or "").strip() or None
    try:
        comment_type = int(basic.get("comment_type") or 0)
    except (TypeError, ValueError):
        comment_type = 0
    return oid, comment_type


def is_dynamic_unavailable_payload(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    code = payload.get("code")
    if isinstance(code, int) and code in DYNAMIC_UNAVAILABLE_CODES:
        return True
    text = " ".join(
        str(payload.get(key) or "").strip()
        for key in ("message", "msg")
        if str(payload.get(key) or "").strip()
    )
    compact = text.replace(" ", "")
    return any(keyword in compact for keyword in DYNAMIC_UNAVAILABLE_KEYWORDS)


def is_dynamic_unavailable_item(item: dict[str, Any]) -> bool:
    return str(item.get("type") or "").strip() == "DYNAMIC_TYPE_NONE"


class QQBotAPIError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, payload: Any = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload


class BiliUserLookupError(RuntimeError):
    pass


class BiliDynamicUnavailableError(BiliUserLookupError):
    pass


@dataclass(slots=True)
class Config:
    qq_appid: str
    qq_app_secret: str
    target_group_openids: list[str]
    target_user_openids: list[str]
    initial_bvids: list[str]
    initial_commenter_mids: list[str]
    initial_commenter_names: list[str]
    initial_dynamic_uids: list[str]
    initial_dynamic_comment_ids: list[str]
    bili_sessdata: str | None
    bili_poll_interval: int
    bili_max_pages: int
    bili_dynamic_max_pages: int
    bili_failure_notify_threshold: int
    state_path: Path
    qq_openapi_timeout: float = 15.0
    bili_timeout: float = 15.0

    @classmethod
    def from_env(cls) -> "Config":
        load_dotenv()
        qq_appid = os.getenv("QQ_APPID", "").strip()
        qq_app_secret = os.getenv("QQ_APP_SECRET", "").strip()
        if not qq_appid or not qq_app_secret:
            raise RuntimeError("缺少 QQ_APPID 或 QQ_APP_SECRET 环境变量")

        def split_csv(name: str) -> list[str]:
            raw = os.getenv(name, "")
            return [item.strip() for item in raw.split(",") if item.strip()]

        return cls(
            qq_appid=qq_appid,
            qq_app_secret=qq_app_secret,
            target_group_openids=split_csv("TARGET_GROUP_OPENIDS"),
            target_user_openids=split_csv("TARGET_USER_OPENIDS"),
            initial_bvids=[bvid for raw in split_csv("BILI_BVIDS") if (bvid := normalize_bvid(raw))],
            initial_commenter_mids=[mid for mid in split_csv("BILI_WATCH_USER_MIDS") if USER_MID_RE.fullmatch(mid)],
            initial_commenter_names=split_csv("BILI_WATCH_USER_NAMES"),
            initial_dynamic_uids=[mid for mid in split_csv("BILI_WATCH_DYNAMIC_UIDS") if USER_MID_RE.fullmatch(mid)],
            initial_dynamic_comment_ids=[
                dynamic_id
                for dynamic_id in split_csv("BILI_WATCH_DYNAMIC_COMMENT_IDS")
                if USER_MID_RE.fullmatch(dynamic_id)
            ],
            bili_sessdata=normalize_sessdata(os.getenv("BILI_SESSDATA", "")),
            bili_poll_interval=max(10, int(os.getenv("BILI_POLL_INTERVAL", "30"))),
            bili_max_pages=max(1, int(os.getenv("BILI_MAX_PAGES", "10"))),
            bili_dynamic_max_pages=max(1, int(os.getenv("BILI_DYNAMIC_MAX_PAGES", "2"))),
            bili_failure_notify_threshold=max(2, int(os.getenv("BILI_FAILURE_NOTIFY_THRESHOLD", "3"))),
            state_path=Path(os.getenv("STATE_PATH", "./state.json")).expanduser().resolve(),
        )


class StateStore:
    def __init__(
        self,
        path: Path,
        *,
        initial_bvids: list[str],
        manual_targets: list[str],
        manual_user_targets: list[str],
        initial_commenter_mids: list[str],
        initial_commenter_names: list[str],
        initial_dynamic_uids: list[str],
        initial_dynamic_comment_ids: list[str],
    ) -> None:
        self.path = path
        self.data = self._load()
        changed = False

        for bvid in initial_bvids:
            if bvid not in self.data["subscriptions"]:
                self.data["subscriptions"].append(bvid)
                changed = True

        normalized_subscriptions = []
        for raw_bvid in self.data["subscriptions"]:
            normalized = normalize_bvid(raw_bvid)
            if normalized and normalized not in normalized_subscriptions:
                normalized_subscriptions.append(normalized)
        if normalized_subscriptions != self.data["subscriptions"]:
            self.data["subscriptions"] = normalized_subscriptions
            changed = True

        for group_openid in manual_targets:
            target = self.data["targets"].get(group_openid)
            if not target or not target.get("enabled"):
                self.data["targets"][group_openid] = {
                    "enabled": True,
                    "source": "manual",
                    "last_event": "MANUAL_CONFIG",
                    "updated_at": self._now(),
                }
                changed = True

        for openid in manual_user_targets:
            target = self.data["user_targets"].get(openid)
            if not target or not target.get("enabled"):
                self.data["user_targets"][openid] = {
                    "enabled": True,
                    "source": "manual",
                    "last_event": "MANUAL_CONFIG",
                    "updated_at": self._now(),
                }
                changed = True

        for mid in initial_commenter_mids:
            if mid not in self.data["commenter_mids"]:
                self.data["commenter_mids"].append(mid)
                changed = True

        for name in initial_commenter_names:
            if name not in self.data["commenter_names"]:
                self.data["commenter_names"].append(name)
                changed = True

        for uid in initial_dynamic_uids:
            if uid not in self.data["dynamic_uids"]:
                self.data["dynamic_uids"].append(uid)
                changed = True

        for dynamic_id in initial_dynamic_comment_ids:
            if dynamic_id not in self.data["dynamic_comment_ids"]:
                self.data["dynamic_comment_ids"].append(dynamic_id)
                changed = True

        if changed:
            self._save()

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return self._default_state()

        with self.path.open("r", encoding="utf-8") as fh:
            loaded = json.load(fh)

        state = self._default_state()
        state.update({k: v for k, v in loaded.items() if k in state})
        state["targets"] = loaded.get("targets", {})
        state["user_targets"] = loaded.get("user_targets", {})
        state["subscriptions"] = loaded.get("subscriptions", [])
        state["commenter_mids"] = loaded.get("commenter_mids", [])
        state["commenter_names"] = loaded.get("commenter_names", [])
        state["dynamic_uids"] = loaded.get("dynamic_uids", [])
        state["dynamic_comment_ids"] = loaded.get("dynamic_comment_ids", [])
        state["dynamic_uid_names"] = loaded.get("dynamic_uid_names", {})
        state["seen_rpids"] = loaded.get("seen_rpids", {})
        state["seen_dynamic_ids"] = loaded.get("seen_dynamic_ids", {})
        state["seen_dynamic_comment_rpids"] = loaded.get("seen_dynamic_comment_rpids", {})
        state["handled_dispatch_ids"] = loaded.get("handled_dispatch_ids", [])
        return state

    def _default_state(self) -> dict[str, Any]:
        return {
            "targets": {},
            "user_targets": {},
            "subscriptions": [],
            "commenter_mids": [],
            "commenter_names": [],
            "dynamic_uids": [],
            "dynamic_comment_ids": [],
            "dynamic_uid_names": {},
            "seen_rpids": {},
            "seen_dynamic_ids": {},
            "seen_dynamic_comment_rpids": {},
            "handled_dispatch_ids": [],
        }

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", encoding="utf-8") as fh:
            json.dump(self.data, fh, ensure_ascii=False, indent=2)

    def _now(self) -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    def remember_dispatch(self, dispatch_id: str | None) -> bool:
        if not dispatch_id:
            return True
        handled = self.data["handled_dispatch_ids"]
        if dispatch_id in handled:
            return False
        handled.append(dispatch_id)
        del handled[:-2000]
        self._save()
        return True

    def upsert_target(self, group_openid: str, *, enabled: bool, source: str, last_event: str) -> None:
        self.data["targets"][group_openid] = {
            "enabled": enabled,
            "source": source,
            "last_event": last_event,
            "updated_at": self._now(),
        }
        self._save()

    def enabled_targets(self) -> list[str]:
        return [
            group_openid
            for group_openid, meta in self.data["targets"].items()
            if meta.get("enabled")
        ]

    def list_targets(self) -> dict[str, dict[str, Any]]:
        return dict(self.data["targets"])

    def upsert_user_target(self, openid: str, *, enabled: bool, source: str, last_event: str) -> None:
        self.data["user_targets"][openid] = {
            "enabled": enabled,
            "source": source,
            "last_event": last_event,
            "updated_at": self._now(),
        }
        self._save()

    def enabled_user_targets(self) -> list[str]:
        return [
            openid
            for openid, meta in self.data["user_targets"].items()
            if meta.get("enabled")
        ]

    def list_user_targets(self) -> dict[str, dict[str, Any]]:
        return dict(self.data["user_targets"])

    def add_subscription(self, bvid: str) -> bool:
        bvid = normalize_bvid(bvid) or ""
        if not bvid:
            return False
        if bvid in self.data["subscriptions"]:
            return False
        self.data["subscriptions"].append(bvid)
        self._save()
        return True

    def remove_subscription(self, bvid: str) -> bool:
        bvid = normalize_bvid(bvid) or ""
        if not bvid:
            return False
        if bvid not in self.data["subscriptions"]:
            return False
        self.data["subscriptions"].remove(bvid)
        self.data["seen_rpids"].pop(bvid, None)
        self._save()
        return True

    def subscriptions(self) -> list[str]:
        return list(self.data["subscriptions"])

    def clear_subscriptions(self) -> int:
        count = len(self.data["subscriptions"])
        if not count:
            return 0
        self.data["subscriptions"] = []
        self.data["seen_rpids"] = {}
        self._save()
        return count

    def add_commenter_mid(self, mid: str) -> bool:
        if mid in self.data["commenter_mids"]:
            return False
        self.data["commenter_mids"].append(mid)
        self._save()
        return True

    def remove_commenter_mid(self, mid: str) -> bool:
        if mid not in self.data["commenter_mids"]:
            return False
        self.data["commenter_mids"].remove(mid)
        self._save()
        return True

    def commenter_mids(self) -> list[str]:
        return list(self.data["commenter_mids"])

    def add_commenter_name(self, name: str) -> bool:
        if name in self.data["commenter_names"]:
            return False
        self.data["commenter_names"].append(name)
        self._save()
        return True

    def remove_commenter_name(self, name: str) -> bool:
        if name not in self.data["commenter_names"]:
            return False
        self.data["commenter_names"].remove(name)
        self._save()
        return True

    def commenter_names(self) -> list[str]:
        return list(self.data["commenter_names"])

    def add_dynamic_uid(self, uid: str) -> bool:
        if uid in self.data["dynamic_uids"]:
            return False
        self.data["dynamic_uids"].append(uid)
        self._save()
        return True

    def set_dynamic_uid_name(self, uid: str, name: str) -> bool:
        cleaned = name.strip()
        if not uid or not cleaned:
            return False
        if self.data["dynamic_uid_names"].get(uid) == cleaned:
            return False
        self.data["dynamic_uid_names"][uid] = cleaned
        self._save()
        return True

    def remove_dynamic_uid(self, uid: str) -> bool:
        if uid not in self.data["dynamic_uids"]:
            return False
        self.data["dynamic_uids"].remove(uid)
        self.data["dynamic_uid_names"].pop(uid, None)
        self.data["seen_dynamic_ids"].pop(uid, None)
        prefix = f"{uid}:"
        for key in [key for key in self.data["seen_dynamic_comment_rpids"] if key.startswith(prefix)]:
            self.data["seen_dynamic_comment_rpids"].pop(key, None)
        self._save()
        return True

    def dynamic_uids(self) -> list[str]:
        return list(self.data["dynamic_uids"])

    def add_dynamic_comment_id(self, dynamic_id: str) -> bool:
        if dynamic_id in self.data["dynamic_comment_ids"]:
            return False
        self.data["dynamic_comment_ids"].append(dynamic_id)
        self._save()
        return True

    def remove_dynamic_comment_id(self, dynamic_id: str) -> bool:
        if dynamic_id not in self.data["dynamic_comment_ids"]:
            return False
        self.data["dynamic_comment_ids"].remove(dynamic_id)
        self.data["seen_dynamic_comment_rpids"].pop(f"dynamic:{dynamic_id}", None)
        self._save()
        return True

    def dynamic_comment_ids(self) -> list[str]:
        return list(self.data["dynamic_comment_ids"])

    def dynamic_uid_name(self, uid: str) -> str:
        return str(self.data["dynamic_uid_names"].get(uid) or "").strip()

    def dynamic_uid_names(self) -> dict[str, str]:
        return dict(self.data["dynamic_uid_names"])

    def has_dynamic_seed(self, uid: str) -> bool:
        return uid in self.data["seen_dynamic_ids"]

    def is_seen_dynamic(self, uid: str, dynamic_id: str) -> bool:
        return dynamic_id in self.data["seen_dynamic_ids"].get(uid, [])

    def prime_dynamic_ids(self, uid: str, dynamic_ids: list[str]) -> None:
        existed = uid in self.data["seen_dynamic_ids"]
        seen = self.data["seen_dynamic_ids"].setdefault(uid, [])
        changed = False
        for dynamic_id in dynamic_ids:
            if not dynamic_id or dynamic_id in seen:
                continue
            seen.append(dynamic_id)
            changed = True
        del seen[:-5000]
        if changed or not existed:
            self._save()

    def mark_seen_dynamic(self, uid: str, dynamic_id: str) -> None:
        seen = self.data["seen_dynamic_ids"].setdefault(uid, [])
        if dynamic_id in seen:
            return
        seen.append(dynamic_id)
        del seen[:-5000]
        self._save()

    def has_dynamic_comment_seed(self, thread_key: str) -> bool:
        return thread_key in self.data["seen_dynamic_comment_rpids"]

    def is_seen_dynamic_comment_reply(self, thread_key: str, rpid: str) -> bool:
        return rpid in self.data["seen_dynamic_comment_rpids"].get(thread_key, [])

    def prime_dynamic_comment_rpids(self, thread_key: str, rpids: list[str]) -> None:
        existed = thread_key in self.data["seen_dynamic_comment_rpids"]
        seen = self.data["seen_dynamic_comment_rpids"].setdefault(thread_key, [])
        changed = False
        for rpid in rpids:
            if not rpid or rpid in seen:
                continue
            seen.append(rpid)
            changed = True
        del seen[:-50000]
        if changed or not existed:
            self._save()

    def mark_seen_dynamic_comment_reply(self, thread_key: str, rpid: str) -> None:
        seen = self.data["seen_dynamic_comment_rpids"].setdefault(thread_key, [])
        if rpid in seen:
            return
        seen.append(rpid)
        del seen[:-50000]
        self._save()

    def is_seen_reply(self, bvid: str, rpid: str) -> bool:
        return rpid in self.data["seen_rpids"].get(bvid, [])

    def mark_seen_reply(self, bvid: str, rpid: str) -> None:
        seen = self.data["seen_rpids"].setdefault(bvid, [])
        if rpid in seen:
            return
        seen.append(rpid)
        del seen[:-50000]
        self._save()


class QQBotClient:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.client = httpx.AsyncClient(
            timeout=config.qq_openapi_timeout,
            headers=DEFAULT_HEADERS,
        )
        self._access_token: str | None = None
        self._access_token_expiry = 0.0
        self._token_lock = asyncio.Lock()

    async def aclose(self) -> None:
        await self.client.aclose()

    async def get_access_token(self, *, force_refresh: bool = False) -> str:
        async with self._token_lock:
            now = time.time()
            if not force_refresh and self._access_token and now < self._access_token_expiry - 60:
                return self._access_token

            response = await self.client.post(
                QQ_TOKEN_URL,
                json={
                    "appId": self.config.qq_appid,
                    "clientSecret": self.config.qq_app_secret,
                },
            )
            response.raise_for_status()
            payload = response.json()
            access_token = payload.get("access_token")
            expires_in = int(payload.get("expires_in", 0))
            if not access_token or not expires_in:
                raise QQBotAPIError(f"获取 access_token 失败: {payload}", status_code=response.status_code, payload=payload)

            self._access_token = access_token
            self._access_token_expiry = now + expires_in
            LOGGER.info("refreshed QQ access token, expires in %ss", expires_in)
            return access_token

    async def api_request(
        self,
        method: str,
        path: str,
        *,
        json_data: dict[str, Any] | None = None,
        retry_on_401: bool = True,
    ) -> Any:
        token = await self.get_access_token()
        response = await self.client.request(
            method,
            f"{QQ_API_BASE}{path}",
            headers={"Authorization": f"QQBot {token}"},
            json=json_data,
        )

        if response.status_code == 401 and retry_on_401:
            token = await self.get_access_token(force_refresh=True)
            response = await self.client.request(
                method,
                f"{QQ_API_BASE}{path}",
                headers={"Authorization": f"QQBot {token}"},
                json=json_data,
            )

        payload: Any
        try:
            payload = response.json()
        except json.JSONDecodeError:
            payload = response.text

        if response.status_code >= 400:
            raise QQBotAPIError(
                f"QQ openapi 请求失败: {response.status_code} {payload}",
                status_code=response.status_code,
                payload=payload,
            )
        return payload

    async def get_gateway_url(self) -> str:
        payload = await self.api_request("GET", "/gateway")
        url = payload.get("url")
        if not url:
            raise QQBotAPIError(f"获取 gateway 失败: {payload}", payload=payload)
        return url

    async def send_group_text(
        self,
        group_openid: str,
        content: str,
        *,
        msg_id: str | None = None,
        event_id: str | None = None,
        msg_seq: int | None = None,
    ) -> Any:
        body: dict[str, Any] = {
            "content": content,
            "msg_type": 0,
        }
        if msg_id:
            body["msg_id"] = msg_id
            body["msg_seq"] = msg_seq or 1
        if event_id:
            body["event_id"] = event_id
        return await self.api_request("POST", f"/v2/groups/{group_openid}/messages", json_data=body)

    async def send_user_text(
        self,
        openid: str,
        content: str,
        *,
        msg_id: str | None = None,
        event_id: str | None = None,
        msg_seq: int | None = None,
    ) -> Any:
        body: dict[str, Any] = {
            "content": content,
            "msg_type": 0,
        }
        if msg_id:
            body["msg_id"] = msg_id
            body["msg_seq"] = msg_seq or 1
        if event_id:
            body["event_id"] = event_id
        return await self.api_request("POST", f"/v2/users/{openid}/messages", json_data=body)


class QQGatewayClient:
    def __init__(self, config: Config, qq: QQBotClient, state: StateStore) -> None:
        self.config = config
        self.qq = qq
        self.state = state
        self.sequence: int | None = None
        cookies: dict[str, str] = {}
        if config.bili_sessdata:
            cookies["SESSDATA"] = config.bili_sessdata
        self.bili_client = httpx.AsyncClient(
            timeout=config.bili_timeout,
            headers=BILI_HEADERS,
            cookies=cookies,
        )

    async def aclose(self) -> None:
        await self.bili_client.aclose()

    async def run_forever(self) -> None:
        while True:
            try:
                gateway_url = await self.qq.get_gateway_url()
                LOGGER.info("connecting QQ gateway: %s", gateway_url)
                await self._run_session(gateway_url)
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("QQ gateway loop failed, retrying in 5 seconds")
                await asyncio.sleep(5)

    async def _run_session(self, gateway_url: str) -> None:
        async with websockets.connect(
            gateway_url,
            ping_interval=None,
            max_size=4 * 1024 * 1024,
            user_agent_header="bili-qq-monitor/1.0",
        ) as ws:
            hello = json.loads(await ws.recv())
            if hello.get("op") != 10:
                raise RuntimeError(f"unexpected hello payload: {hello}")

            heartbeat_interval = hello["d"]["heartbeat_interval"] / 1000.0
            token = await self.qq.get_access_token()
            identify = {
                "op": 2,
                "d": {
                    "token": f"QQBot {token}",
                    "intents": GROUP_EVENT_INTENT,
                    "shard": [0, 1],
                    "properties": {
                        "$os": "linux",
                        "$browser": "bili-qq-monitor",
                        "$device": "bili-qq-monitor",
                    },
                },
            }
            await ws.send(json.dumps(identify, ensure_ascii=False))
            heartbeat_task = asyncio.create_task(self._heartbeat_loop(ws, heartbeat_interval))
            try:
                async for raw in ws:
                    payload = json.loads(raw)
                    op = payload.get("op")
                    if op == 0:
                        self.sequence = payload.get("s")
                        await self._handle_dispatch(payload)
                    elif op == 7:
                        LOGGER.warning("QQ gateway asked to reconnect")
                        return
                    elif op == 9:
                        LOGGER.warning("QQ gateway invalid session: %s", payload)
                        return
            finally:
                heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat_task

    async def _heartbeat_loop(self, ws: websockets.WebSocketClientProtocol, interval: float) -> None:
        while True:
            payload = {"op": 1, "d": self.sequence}
            await ws.send(json.dumps(payload))
            await asyncio.sleep(interval)

    async def _handle_dispatch(self, payload: dict[str, Any]) -> None:
        dispatch_id = payload.get("id")
        if not self.state.remember_dispatch(dispatch_id):
            LOGGER.info("skip duplicated dispatch id=%s", dispatch_id)
            return

        event_type = payload.get("t")
        data = payload.get("d", {})
        LOGGER.info("QQ event %s", event_type)

        if event_type == "READY":
            LOGGER.info("QQ gateway ready: %s", data.get("session_id"))
            return

        if event_type == "GROUP_ADD_ROBOT":
            await self._handle_group_add_robot(data, dispatch_id)
            return

        if event_type == "FRIEND_ADD":
            await self._handle_friend_add(data, dispatch_id)
            return

        if event_type == "GROUP_DEL_ROBOT":
            group_openid = data.get("group_openid")
            if group_openid:
                self.state.upsert_target(group_openid, enabled=False, source="event", last_event=event_type)
            return

        if event_type == "FRIEND_DEL":
            openid = data.get("openid")
            if openid:
                self.state.upsert_user_target(openid, enabled=False, source="event", last_event=event_type)
            return

        if event_type == "GROUP_MSG_RECEIVE":
            group_openid = data.get("group_openid")
            if group_openid:
                self.state.upsert_target(group_openid, enabled=True, source="event", last_event=event_type)
                if dispatch_id:
                    await self._safe_group_reply(
                        group_openid,
                        "已记录该群可接收通知。使用 @机器人 /watch BV号 添加视频，/watchdyn UID或昵称 添加动态监控，/watchdyncomment 动态ID 监听单条图文动态评论，或发送 /h 查看全部命令。",
                        event_id=dispatch_id,
                    )
            return

        if event_type == "C2C_MSG_RECEIVE":
            openid = data.get("openid")
            if openid:
                self.state.upsert_user_target(openid, enabled=True, source="event", last_event=event_type)
                if dispatch_id:
                    await self._safe_user_reply(
                        openid,
                        "已记录你可接收提醒。直接发送 /watch BV号 添加视频，/watchdyn UID或昵称 添加动态监控，/watchdyncomment 动态ID 监听单条图文动态评论，或发送 /h 查看全部命令。",
                        event_id=dispatch_id,
                    )
            return

        if event_type == "GROUP_MSG_REJECT":
            group_openid = data.get("group_openid")
            if group_openid:
                self.state.upsert_target(group_openid, enabled=False, source="event", last_event=event_type)
            return

        if event_type == "C2C_MSG_REJECT":
            openid = data.get("openid")
            if openid:
                self.state.upsert_user_target(openid, enabled=False, source="event", last_event=event_type)
            return

        if event_type == "GROUP_AT_MESSAGE_CREATE":
            await self._handle_group_at_message(data)
            return

        if event_type == "C2C_MESSAGE_CREATE":
            await self._handle_c2c_message(data)
            return

    async def _handle_group_add_robot(self, data: dict[str, Any], event_id: str | None) -> None:
        group_openid = data.get("group_openid")
        if not group_openid:
            return
        self.state.upsert_target(group_openid, enabled=True, source="event", last_event="GROUP_ADD_ROBOT")
        if event_id:
            await self._safe_group_reply(
                group_openid,
                "已接入。@我发送 /watch BV号 添加视频，/watchuser 昵称 添加评论用户监听，/watchdyn UID或昵称 添加动态监控，/watchdyncomment 动态ID 监听单条图文动态评论，/h 查看全部命令。",
                event_id=event_id,
            )

    async def _handle_friend_add(self, data: dict[str, Any], event_id: str | None) -> None:
        openid = data.get("openid")
        if not openid:
            return
        self.state.upsert_user_target(openid, enabled=True, source="event", last_event="FRIEND_ADD")
        if event_id:
            await self._safe_user_reply(
                openid,
                "已接入。直接发送 /watch BV号 添加视频，/watchuser 昵称 添加评论用户监听，/watchdyn UID或昵称 添加动态监控，/watchdyncomment 动态ID 监听单条图文动态评论，/h 查看全部命令。",
                event_id=event_id,
            )

    async def _handle_group_at_message(self, data: dict[str, Any]) -> None:
        group_openid = data.get("group_openid")
        msg_id = data.get("id")
        raw_content = (data.get("content") or "").strip()
        if not group_openid or not msg_id:
            return

        self.state.upsert_target(group_openid, enabled=True, source="event", last_event="GROUP_AT_MESSAGE_CREATE")
        command = self._normalize_command(raw_content)
        response = await self._run_command(command)
        await self._safe_group_reply(group_openid, response, msg_id=msg_id)

    async def _handle_c2c_message(self, data: dict[str, Any]) -> None:
        author = data.get("author") or {}
        openid = author.get("user_openid")
        msg_id = data.get("id")
        raw_content = (data.get("content") or "").strip()
        if not openid or not msg_id:
            return

        self.state.upsert_user_target(openid, enabled=True, source="event", last_event="C2C_MESSAGE_CREATE")
        command = self._normalize_command(raw_content)
        response = await self._run_command(command)
        await self._safe_user_reply(openid, response, msg_id=msg_id)

    async def _run_command(self, command: str) -> str:
        if not command:
            return self._help_text()

        if command in {"/h", "/help"}:
            return self._help_text()

        if command == "/status":
            return self._build_status_text()

        if command.startswith("/watch "):
            bvid = self._extract_bvid(command)
            if not bvid:
                return "格式错误，示例: /watch BV1xx411c7mD"
            added = self.state.add_subscription(bvid)
            return f"已添加监控 {bvid}" if added else f"{bvid} 已在监控列表中"

        if command.startswith("/unwatch "):
            bvid = self._extract_bvid(command)
            if not bvid:
                return "格式错误，示例: /unwatch BV1xx411c7mD"
            removed = self.state.remove_subscription(bvid)
            return f"已移除监控 {bvid}" if removed else f"{bvid} 不在监控列表中"

        if command.startswith("/watchdyncomment "):
            dynamic_id = self._extract_dynamic_id_arg(command, "/watchdyncomment ")
            if not dynamic_id:
                return "格式错误，示例: /watchdyncomment 1184285425981718532"
            try:
                resolved = await self._resolve_dynamic_comment_target(dynamic_id)
            except BiliUserLookupError as exc:
                return f"添加失败: {exc}"
            added = self.state.add_dynamic_comment_id(dynamic_id)
            tip = "；注意当前未配置 BILI_SESSDATA，动态详情和评论接口稳定性可能较差" if not self.config.bili_sessdata else ""
            label = f"{resolved['type_label']} {dynamic_id} | {resolved['owner_name']}"
            return f"已添加图文动态评论监听 {label}{tip}" if added else f"{label} 已在图文动态评论监听列表中{tip}"

        if command.startswith("/unwatchdyncomment "):
            dynamic_id = self._extract_dynamic_id_arg(command, "/unwatchdyncomment ")
            if not dynamic_id:
                return "格式错误，示例: /unwatchdyncomment 1184285425981718532"
            removed = self.state.remove_dynamic_comment_id(dynamic_id)
            return f"已移除图文动态评论监听 {dynamic_id}" if removed else f"{dynamic_id} 不在图文动态评论监听列表中"

        if command.startswith("/watchdyn "):
            target = self._extract_command_arg(command, "/watchdyn ")
            if not target:
                return "格式错误，示例: /watchdyn 279321940 或 /watchdyn 某个UP昵称"
            try:
                resolved = await self._resolve_dynamic_target(target)
            except BiliUserLookupError as exc:
                return f"添加失败: {exc}"
            if resolved.get("name"):
                self.state.set_dynamic_uid_name(resolved["uid"], resolved["name"])
            added = self.state.add_dynamic_uid(resolved["uid"])
            tip = "；注意当前未配置 BILI_SESSDATA，动态接口大概率会失败" if not self.config.bili_sessdata else ""
            return (
                f"已添加动态监控 {self._format_dynamic_target(resolved)}{tip}"
                if added
                else f"{self._format_dynamic_target(resolved)} 已在动态监控列表中{tip}"
            )

        if command.startswith("/unwatchdyn "):
            target = self._extract_command_arg(command, "/unwatchdyn ")
            if not target:
                return "格式错误，示例: /unwatchdyn 279321940 或 /unwatchdyn 某个UP昵称"
            try:
                resolved = await self._resolve_dynamic_target(target)
            except BiliUserLookupError as exc:
                return f"移除失败: {exc}"
            removed = self.state.remove_dynamic_uid(resolved["uid"])
            return (
                f"已移除动态监控 {self._format_dynamic_target(resolved)}"
                if removed
                else f"{self._format_dynamic_target(resolved)} 不在动态监控列表中"
            )

        if command == "/clearwatch":
            cleared = self.state.clear_subscriptions()
            return f"已清空 {cleared} 个监控视频" if cleared else "当前没有监控中的视频"

        if command.startswith("/watchuser ") or command.startswith("/watchuid "):
            if command.startswith("/watchuid "):
                mid = self._extract_user_mid(command, "/watchuid ")
                if not mid:
                    return "格式错误，示例: /watchuid 279321940"
                added = self.state.add_commenter_mid(mid)
                return f"已添加评论用户UID监听 {mid}" if added else f"{mid} 已在评论用户UID监听列表中"

            target = self._extract_command_arg(command, "/watchuser ")
            if not target:
                return "格式错误，示例: /watchuser 某个昵称"
            if USER_MID_RE.fullmatch(target):
                added = self.state.add_commenter_mid(target)
                return f"已添加评论用户UID监听 {target}" if added else f"{target} 已在评论用户UID监听列表中"
            added = self.state.add_commenter_name(target)
            return f"已添加评论用户昵称监听 {target}" if added else f"{target} 已在评论用户昵称监听列表中"

        if command.startswith("/unwatchuser ") or command.startswith("/unwatchuid "):
            if command.startswith("/unwatchuid "):
                mid = self._extract_user_mid(command, "/unwatchuid ")
                if not mid:
                    return "格式错误，示例: /unwatchuid 279321940"
                removed = self.state.remove_commenter_mid(mid)
                return f"已移除评论用户UID监听 {mid}" if removed else f"{mid} 不在评论用户UID监听列表中"

            target = self._extract_command_arg(command, "/unwatchuser ")
            if not target:
                return "格式错误，示例: /unwatchuser 某个昵称"
            if USER_MID_RE.fullmatch(target):
                removed = self.state.remove_commenter_mid(target)
                return f"已移除评论用户UID监听 {target}" if removed else f"{target} 不在评论用户UID监听列表中"
            removed = self.state.remove_commenter_name(target)
            return f"已移除评论用户昵称监听 {target}" if removed else f"{target} 不在评论用户昵称监听列表中"

        return self._help_text()

    def _normalize_command(self, content: str) -> str:
        cleaned = re.sub(r"<@!?.+?>", " ", content)
        cleaned = " ".join(cleaned.split())
        lowered = cleaned.lower()
        if lowered.startswith("/watch "):
            return f"/watch {cleaned.split(maxsplit=1)[1]}"
        if lowered.startswith("/unwatch "):
            return f"/unwatch {cleaned.split(maxsplit=1)[1]}"
        if lowered.startswith("/watchuser "):
            return f"/watchuser {cleaned.split(maxsplit=1)[1]}"
        if lowered.startswith("/watchuid "):
            return f"/watchuid {cleaned.split(maxsplit=1)[1]}"
        if lowered.startswith("/watchdyncomment "):
            return f"/watchdyncomment {cleaned.split(maxsplit=1)[1]}"
        if lowered.startswith("/watchdynreply "):
            return f"/watchdyncomment {cleaned.split(maxsplit=1)[1]}"
        if lowered.startswith("/watchdyn "):
            return f"/watchdyn {cleaned.split(maxsplit=1)[1]}"
        if lowered.startswith("/watchdynamic "):
            return f"/watchdyn {cleaned.split(maxsplit=1)[1]}"
        if lowered.startswith("/unwatchuser "):
            return f"/unwatchuser {cleaned.split(maxsplit=1)[1]}"
        if lowered.startswith("/unwatchuid "):
            return f"/unwatchuid {cleaned.split(maxsplit=1)[1]}"
        if lowered.startswith("/unwatchdyncomment "):
            return f"/unwatchdyncomment {cleaned.split(maxsplit=1)[1]}"
        if lowered.startswith("/unwatchdynreply "):
            return f"/unwatchdyncomment {cleaned.split(maxsplit=1)[1]}"
        if lowered.startswith("/unwatchdyn "):
            return f"/unwatchdyn {cleaned.split(maxsplit=1)[1]}"
        if lowered.startswith("/unwatchdynamic "):
            return f"/unwatchdyn {cleaned.split(maxsplit=1)[1]}"
        if lowered == "/clearwatch":
            return "/clearwatch"
        if lowered in {"/h", "/help"}:
            return lowered
        if lowered == "/status":
            return "/status"
        return cleaned

    def _extract_bvid(self, text: str) -> str | None:
        match = BVID_RE.search(text)
        return normalize_bvid(match.group(0)) if match else None

    def _extract_command_arg(self, text: str, prefix: str) -> str:
        return text[len(prefix):].strip()

    def _extract_user_mid(self, text: str, prefix: str) -> str | None:
        target = self._extract_command_arg(text, prefix)
        return target if USER_MID_RE.fullmatch(target) else None

    def _extract_dynamic_id_arg(self, text: str, prefix: str) -> str | None:
        target = self._extract_command_arg(text, prefix)
        return target if USER_MID_RE.fullmatch(target) else None

    def _help_text(self) -> str:
        return (
            "命令列表:\n"
            "/watch BV号: 添加监控视频\n"
            "/unwatch BV号: 移除单个监控视频\n"
            "/clearwatch: 清空所有监控视频\n"
            "/watchuser 昵称: 按昵称监听评论用户\n"
            "/unwatchuser 昵称: 移除昵称监听\n"
            "/watchuid UID: 按UID监听评论用户\n"
            "/unwatchuid UID: 移除UID监听\n"
            "/watchdyncomment 动态ID: 监听单条图文动态评论\n"
            "/unwatchdyncomment 动态ID: 移除单条图文动态评论监听\n"
            "/watchdyn UID或昵称: 监听UP主新动态\n"
            "/unwatchdyn UID或昵称: 移除动态监听\n"
            "/status: 查看当前状态\n"
            "/h 或 /help: 查看本帮助\n"
            "说明1: 动态昵称按精确匹配解析，重名请改用UID\n"
            "说明2: /watchdyncomment 支持 /watchdynreply 作为别名\n"
            "说明3: 已开启动态监控的 UID，会自动监听其最近图文动态的评论"
        )

    def _build_status_text(self) -> str:
        bvids = self.state.subscriptions()
        commenter_mids = self.state.commenter_mids()
        commenter_names = self.state.commenter_names()
        dynamic_uids = self.state.dynamic_uids()
        dynamic_comment_ids = self.state.dynamic_comment_ids()
        dynamic_labels = [
            self._format_dynamic_target({"uid": uid, "name": self.state.dynamic_uid_name(uid)})
            for uid in dynamic_uids
        ]
        group_targets = self.state.list_targets()
        user_targets = self.state.list_user_targets()
        active_group_targets = [group_openid for group_openid, meta in group_targets.items() if meta.get("enabled")]
        active_user_targets = [openid for openid, meta in user_targets.items() if meta.get("enabled")]
        return (
            "状态:\n"
            f"监控视频: {', '.join(bvids) if bvids else '无'}\n"
            f"指定评论UID: {', '.join(commenter_mids) if commenter_mids else '无'}\n"
            f"指定评论昵称: {', '.join(commenter_names) if commenter_names else '无'}\n"
            f"动态监控: {', '.join(dynamic_labels) if dynamic_labels else '无'}\n"
            f"单条图文动态评论监听: {', '.join(dynamic_comment_ids) if dynamic_comment_ids else '无'}\n"
            "图文动态评论监听: 已随动态监控自动启用\n"
            f"有效群目标: {len(active_group_targets)} 个\n"
            f"有效私聊目标: {len(active_user_targets)} 个\n"
            "注意: 官方文档已提示主动推送能力可能被拒绝，若告警发送失败请查看服务日志。"
        )

    async def _resolve_dynamic_target(self, target: str) -> dict[str, str]:
        cleaned = target.strip()
        if USER_MID_RE.fullmatch(cleaned):
            return {"uid": cleaned, "name": self.state.dynamic_uid_name(cleaned)}

        stored_matches = self._match_stored_dynamic_names(cleaned)
        if len(stored_matches) == 1:
            return stored_matches[0]
        if len(stored_matches) > 1:
            candidates = "、".join(self._format_dynamic_target(item) for item in stored_matches[:3])
            raise BiliUserLookupError(f"昵称 {cleaned} 在本地记录中对应多个 UID: {candidates}，请改用UID")
        return await self._resolve_bili_user_by_name(cleaned)

    async def _resolve_bili_user_by_name(self, name: str) -> dict[str, str]:
        try:
            response = await self.bili_client.get(
                BILI_USER_SEARCH_URL,
                params={
                    "search_type": "bili_user",
                    "keyword": name,
                    "page": 1,
                },
            )
            if response.status_code == 412:
                raise BiliUserLookupError("B站用户搜索触发 412，请稍后重试或直接改用UID")
            response.raise_for_status()
        except BiliUserLookupError:
            raise
        except httpx.HTTPError as exc:
            raise BiliUserLookupError("B站用户搜索请求失败，请稍后重试或直接改用UID") from exc
        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise BiliUserLookupError("B站用户搜索返回异常内容，请稍后重试或直接改用UID") from exc

        code = payload.get("code")
        if code == -352:
            raise BiliUserLookupError("B站用户搜索触发风控，请稍后重试或直接改用UID")
        if code != 0:
            message = str(payload.get("message") or payload.get("msg") or "未知错误").strip() or "未知错误"
            raise BiliUserLookupError(f"B站用户搜索失败: code={code} message={message}")

        result = ((payload.get("data") or {}).get("result") or [])
        matches: list[dict[str, str]] = []
        target_name = name.casefold()
        seen_uids: set[str] = set()
        for item in result:
            uid = str(item.get("mid") or "").strip()
            uname = self._strip_html_tags(item.get("uname"))
            if not uid or not uname or uid in seen_uids:
                continue
            if uname.casefold() != target_name:
                continue
            matches.append({"uid": uid, "name": uname})
            seen_uids.add(uid)

        if not matches:
            raise BiliUserLookupError(f"未找到昵称精确匹配 {name} 的 UP，请检查昵称或直接使用UID")
        if len(matches) > 1:
            candidates = "、".join(f"{item['name']}({item['uid']})" for item in matches[:3])
            raise BiliUserLookupError(f"昵称 {name} 存在多个精确匹配结果: {candidates}，请改用UID")
        return matches[0]

    def _strip_html_tags(self, value: Any) -> str:
        text = html.unescape(str(value or ""))
        return HTML_TAG_RE.sub("", text).strip()

    def _match_stored_dynamic_names(self, name: str) -> list[dict[str, str]]:
        target_name = name.casefold()
        return [
            {"uid": uid, "name": stored_name}
            for uid, stored_name in self.state.dynamic_uid_names().items()
            if stored_name.casefold() == target_name
        ]

    def _format_dynamic_target(self, target: dict[str, str]) -> str:
        name = target.get("name", "").strip()
        uid = target["uid"]
        return f"{name} (UID {uid})" if name else f"UID {uid}"

    async def _resolve_dynamic_comment_target(self, dynamic_id: str) -> dict[str, str]:
        item = await self._fetch_dynamic_detail_item(dynamic_id)
        type_label = self._dynamic_comment_target_type_label(item)
        if not type_label:
            raise BiliUserLookupError(f"动态 {dynamic_id} 不是图文/文字动态，当前仅支持这两类动态评论监听")

        oid, comment_type = extract_dynamic_comment_thread(item)
        if not oid:
            raise BiliUserLookupError(f"动态 {dynamic_id} 未拿到有效评论线程，暂时无法监听")
        if comment_type not in {0, 11}:
            raise BiliUserLookupError(
                f"动态 {dynamic_id} 返回了不支持的评论类型 {comment_type}，暂时无法监听"
            )

        basic = item.get("basic") or {}
        author = self._dynamic_author_info_from_item(item, str(basic.get("uid") or ""))
        return {
            "dynamic_id": dynamic_id,
            "owner_name": author["name"],
            "owner_mid": author["mid"],
            "type_label": type_label,
            "oid": oid,
        }

    async def _fetch_dynamic_detail_item(self, dynamic_id: str) -> dict[str, Any]:
        params = {
            "id": dynamic_id,
            "timezone_offset": -480,
            "features": "itemOpusStyle",
        }
        last_exc: Exception | None = None
        for url in (BILI_DYNAMIC_DETAIL_DESKTOP_URL, BILI_DYNAMIC_DETAIL_URL):
            try:
                response = await self.bili_client.get(url, params=params)
                if response.status_code == 412:
                    raise BiliUserLookupError("B站动态详情接口触发 412，请稍后重试")
                response.raise_for_status()
                payload = response.json()
                if payload.get("code") == 0:
                    data = payload.get("data") or {}
                    item = data.get("item") or data.get("detail") or data
                    if isinstance(item, dict) and item.get("id_str"):
                        if is_dynamic_unavailable_item(item):
                            raise BiliDynamicUnavailableError(f"动态 {dynamic_id} 已删除、失效或当前不可见")
                        return item
                    raise BiliUserLookupError("B站动态详情接口未返回有效动态内容")
                if payload.get("code") == -352:
                    last_exc = BiliUserLookupError("B站动态详情接口触发风控，改用页面兜底解析")
                    continue
                if is_dynamic_unavailable_payload(payload):
                    raise BiliDynamicUnavailableError(f"动态 {dynamic_id} 已删除、失效或当前不可见")
                message = str(payload.get("message") or payload.get("msg") or "未知错误").strip() or "未知错误"
                raise BiliUserLookupError(f"B站动态详情接口失败: code={payload.get('code')} message={message}")
            except BiliUserLookupError as exc:
                last_exc = exc
                if "风控" not in str(exc):
                    raise
            except Exception as exc:
                last_exc = exc

        return await self._fetch_dynamic_detail_item_from_page(dynamic_id, last_exc)

    async def _fetch_dynamic_detail_item_from_page(
        self,
        dynamic_id: str,
        last_exc: Exception | None = None,
    ) -> dict[str, Any]:
        try:
            response = await self.bili_client.get(
                f"https://www.bilibili.com/opus/{dynamic_id}",
                headers=BILI_HTML_HEADERS,
                follow_redirects=True,
            )
            response.raise_for_status()
        except Exception as exc:
            if last_exc:
                if isinstance(last_exc, BiliDynamicUnavailableError):
                    raise last_exc
                raise BiliUserLookupError(f"{last_exc}；且页面兜底解析失败") from exc
            raise BiliUserLookupError("打开动态详情页失败，请稍后重试") from exc

        match = INITIAL_STATE_RE.search(response.text)
        if not match:
            if isinstance(last_exc, BiliDynamicUnavailableError):
                raise last_exc
            raise BiliUserLookupError("动态详情页未找到初始化数据，请确认动态ID是否有效")
        try:
            payload = json.loads(match.group(1))
        except json.JSONDecodeError as exc:
            raise BiliUserLookupError("动态详情页初始化数据解析失败，请稍后重试") from exc

        item = payload.get("detail") or {}
        if not isinstance(item, dict) or str(item.get("id_str") or "").strip() != dynamic_id:
            if isinstance(last_exc, BiliDynamicUnavailableError):
                raise last_exc
            raise BiliUserLookupError("动态详情页未返回目标动态，请确认动态ID是否有效")
        if is_dynamic_unavailable_item(item):
            raise BiliDynamicUnavailableError(f"动态 {dynamic_id} 已删除、失效或当前不可见")
        return item

    def _dynamic_modules_map_from_item(self, item: dict[str, Any]) -> dict[str, Any]:
        modules = item.get("modules") or {}
        if isinstance(modules, dict):
            return modules
        if not isinstance(modules, list):
            return {}

        flattened: dict[str, Any] = {}
        for module in modules:
            if not isinstance(module, dict):
                continue
            for key, value in module.items():
                if key == "module_type":
                    continue
                flattened.setdefault(key, value)
        return flattened

    def _dynamic_author_info_from_item(self, item: dict[str, Any], watched_uid: str) -> dict[str, Any]:
        author = self._dynamic_modules_map_from_item(item).get("module_author") or {}
        user = author.get("user") or {}
        return {
            "name": author.get("name") or user.get("name") or f"UID {watched_uid}",
            "mid": str(author.get("mid") or user.get("mid") or watched_uid),
        }

    def _dynamic_type_label_from_item(self, item: dict[str, Any]) -> str:
        item_type = str(item.get("type") or "").strip()
        mapping = {
            "DYNAMIC_TYPE_WORD": "文字动态",
            "DYNAMIC_TYPE_DRAW": "图文动态",
            "DYNAMIC_TYPE_AV": "视频动态",
            "DYNAMIC_TYPE_ARTICLE": "专栏动态",
            "DYNAMIC_TYPE_FORWARD": "转发动态",
            "DYNAMIC_TYPE_COMMON_SQUARE": "卡片动态",
            "DYNAMIC_TYPE_LIVE_RCMD": "直播动态",
            "DYNAMIC_TYPE_NONE": "失效动态",
        }
        return mapping.get(item_type, item_type or "动态")

    def _dynamic_comment_target_type_label(self, item: dict[str, Any]) -> str | None:
        item_type = str(item.get("type") or "").strip()
        if item_type == "DYNAMIC_TYPE_DRAW":
            return "图文动态"
        if item_type == "DYNAMIC_TYPE_WORD":
            return "文字动态"

        modules = self._dynamic_modules_map_from_item(item)
        top = modules.get("module_top") or {}
        display = top.get("display") or {}
        album = display.get("album") or {}
        if album.get("pics"):
            return "图文动态"
        if modules.get("module_content"):
            return "文字动态"
        return None

    async def _safe_group_reply(
        self,
        group_openid: str,
        content: str,
        *,
        msg_id: str | None = None,
        event_id: str | None = None,
    ) -> None:
        try:
            await self.qq.send_group_text(group_openid, content, msg_id=msg_id, event_id=event_id)
        except Exception:
            LOGGER.exception("failed to send passive group reply")

    async def _safe_user_reply(
        self,
        openid: str,
        content: str,
        *,
        msg_id: str | None = None,
        event_id: str | None = None,
    ) -> None:
        try:
            await self.qq.send_user_text(openid, content, msg_id=msg_id, event_id=event_id)
        except Exception:
            LOGGER.exception("failed to send passive C2C reply")


class BilibiliMonitor:
    def __init__(self, config: Config, qq: QQBotClient, state: StateStore) -> None:
        cookies: dict[str, str] = {}
        if config.bili_sessdata:
            cookies["SESSDATA"] = config.bili_sessdata
        self.client = httpx.AsyncClient(
            timeout=config.bili_timeout,
            headers=BILI_HEADERS,
            cookies=cookies,
        )
        self.config = config
        self.qq = qq
        self.state = state
        self.video_cache: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self.dynamic_comment_meta_cache: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self.failure_streaks: dict[str, int] = {}
        self.failure_notified: set[str] = set()
        self.scan_backoff_until: dict[str, float] = {}
        self.scan_backoff_seconds: dict[str, int] = {}

    async def aclose(self) -> None:
        await self.client.aclose()

    async def run_forever(self) -> None:
        while True:
            subscriptions = self.state.subscriptions()
            dynamic_uids = self.state.dynamic_uids()
            dynamic_comment_ids = self.state.dynamic_comment_ids()
            if not subscriptions and not dynamic_uids and not dynamic_comment_ids:
                LOGGER.info(
                    "no BVID, dynamic UID, or dynamic comment ID configured yet, waiting for /watch /watchdyn /watchdyncomment"
                )
            for bvid in subscriptions:
                if self._is_scan_backoff_active(bvid):
                    continue
                try:
                    await self._scan_video(bvid)
                    self._record_scan_success(bvid)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    LOGGER.exception("Bilibili scan failed for bvid=%s", bvid)
                    await self._record_scan_failure(bvid, exc)
            for uid in dynamic_uids:
                try:
                    await self._scan_dynamic_user(uid)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    LOGGER.exception("Bilibili dynamic scan failed for uid=%s", uid)
            for dynamic_id in dynamic_comment_ids:
                try:
                    await self._scan_specific_dynamic_comment(dynamic_id)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    LOGGER.exception("Bilibili specific dynamic comment scan failed for dynamic_id=%s", dynamic_id)
            await asyncio.sleep(self.config.bili_poll_interval)

    def _record_scan_success(self, bvid: str) -> None:
        if self.failure_streaks.get(bvid, 0) > 0:
            LOGGER.info("Bilibili scan recovered for bvid=%s after %s failures", bvid, self.failure_streaks[bvid])
        self.failure_streaks[bvid] = 0
        self.failure_notified.discard(bvid)
        self.scan_backoff_until.pop(bvid, None)
        self.scan_backoff_seconds.pop(bvid, None)

    async def _record_scan_failure(self, bvid: str, exc: Exception) -> None:
        if self._is_transient_bili_error(exc):
            self._schedule_scan_backoff(bvid, exc)
        else:
            self.scan_backoff_until.pop(bvid, None)
            self.scan_backoff_seconds.pop(bvid, None)

        streak = self.failure_streaks.get(bvid, 0) + 1
        self.failure_streaks[bvid] = streak
        threshold = self.config.bili_failure_notify_threshold
        if streak < threshold or bvid in self.failure_notified:
            return

        self.failure_notified.add(bvid)
        await self._fanout_failure_alert(bvid, streak, exc)

    async def _fanout_failure_alert(self, bvid: str, streak: int, exc: Exception) -> None:
        user_targets = self.state.enabled_user_targets()
        if not user_targets:
            LOGGER.warning("Bilibili scan failure reached threshold for %s, but no enabled user QQ targets", bvid)
            return

        title = self.video_cache.get(bvid, {}).get("title", "未知视频")
        error_text = self._format_error_text(exc)
        text = (
            "【B站监控异常提醒】\n"
            f"视频: {title}\n"
            f"BV号: {bvid}\n"
            f"连续失败: {streak} 次\n"
            f"错误: {error_text}\n"
            "说明: 服务会继续自动重试。"
        )

        for openid in user_targets:
            try:
                await self.qq.send_user_text(openid, text)
                LOGGER.info("sent bili failure alert to user_openid=%s bvid=%s streak=%s", openid, bvid, streak)
            except QQBotAPIError as notify_exc:
                LOGGER.error(
                    "failed to send bili failure alert to user %s: status=%s payload=%s",
                    openid,
                    notify_exc.status_code,
                    notify_exc.payload,
                )
            except Exception:
                LOGGER.exception("failed to send bili failure alert to user %s", openid)

    def _format_error_text(self, exc: Exception) -> str:
        text = " ".join(str(exc).split())
        return text[:160] if text else exc.__class__.__name__

    def _is_scan_backoff_active(self, bvid: str) -> bool:
        until = self.scan_backoff_until.get(bvid)
        if not until:
            return False
        if time.monotonic() >= until:
            self.scan_backoff_until.pop(bvid, None)
            return False
        return True

    def _schedule_scan_backoff(self, bvid: str, exc: Exception) -> None:
        previous = self.scan_backoff_seconds.get(bvid, 0)
        base_seconds = max(self.config.bili_poll_interval * 2, 60)
        wait_seconds = base_seconds if previous <= 0 else min(previous * 2, 300)
        self.scan_backoff_seconds[bvid] = wait_seconds
        self.scan_backoff_until[bvid] = time.monotonic() + wait_seconds
        LOGGER.warning(
            "Bilibili scan entering backoff for bvid=%s wait=%ss reason=%s",
            bvid,
            wait_seconds,
            self._format_error_text(exc),
        )

    def _is_transient_bili_error(self, exc: Exception) -> bool:
        text = self._format_error_text(exc)
        return "触发风控" in text or "触发 412" in text or "Precondition Failed" in text

    async def _scan_video(self, bvid: str) -> None:
        meta = await self._get_video_meta(bvid)
        found: dict[str, dict[str, Any]] = {}
        watched_mids = set(self.state.commenter_mids())
        watched_names = set(self.state.commenter_names())

        next_cursor = 0
        completed_pages = 0
        for _ in range(self.config.bili_max_pages):
            try:
                replies, cursor = await self._get_root_replies_page(meta["aid"], next_cursor)
            except Exception as exc:
                if completed_pages > 0 and self._is_transient_bili_error(exc):
                    LOGGER.warning(
                        "Bilibili scan degraded for bvid=%s after %s pages, stop early: %s",
                        bvid,
                        completed_pages,
                        self._format_error_text(exc),
                    )
                    break
                raise

            completed_pages += 1
            if not replies:
                break

            for root in replies:
                root_rpid = str(root["rpid"])
                match = self._match_commenter(
                    root,
                    meta,
                    watched_mids=watched_mids,
                    watched_names=watched_names,
                )
                if match:
                    found.setdefault(root_rpid, {"reply": root, "match": match})

                preview_sub_replies = root.get("replies") or []
                for sub in preview_sub_replies:
                    sub_rpid = str(sub["rpid"])
                    match = self._match_commenter(
                        sub,
                        meta,
                        watched_mids=watched_mids,
                        watched_names=watched_names,
                    )
                    if match:
                        found.setdefault(sub_rpid, {"reply": sub, "match": match})

                has_extra_sub_replies = (root.get("rcount") or 0) > len(preview_sub_replies)
                need_full_sub_scan = (
                    (root.get("reply_control") or {}).get("up_reply")
                    or (
                        has_extra_sub_replies
                        and (watched_mids or watched_names)
                    )
                )
                if need_full_sub_scan:
                    async for sub in self._iter_sub_replies(meta["aid"], root["rpid"]):
                        sub_rpid = str(sub["rpid"])
                        match = self._match_commenter(
                            sub,
                            meta,
                            watched_mids=watched_mids,
                            watched_names=watched_names,
                        )
                        if match:
                            found.setdefault(sub_rpid, {"reply": sub, "match": match})

            if cursor.get("is_end"):
                break
            next_cursor = int(cursor.get("next") or 0)
            if not next_cursor:
                break
            await asyncio.sleep(0.2)

        new_replies = [
            item for item in found.values()
            if not self.state.is_seen_reply(bvid, str(item["reply"]["rpid"]))
        ]
        new_replies.sort(key=lambda item: item["reply"].get("ctime", 0))

        for item in new_replies:
            reply = item["reply"]
            text = self._build_message(meta, bvid, reply, item["match"])
            await self._fanout_message(text)
            self.state.mark_seen_reply(bvid, str(reply["rpid"]))
            LOGGER.info("recorded watched comment bvid=%s rpid=%s mid=%s", bvid, reply["rpid"], item["match"]["mid"])

    async def _get_root_replies_page(self, aid: int, next_cursor: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        response = await self.client.get(
            BILI_REPLY_MAIN_URL,
            params={
                "type": 1,
                "oid": aid,
                "mode": BILI_REPLY_MODE_LATEST,
                "next": next_cursor,
                "ps": BILI_REPLY_PAGE_SIZE,
            },
        )
        if response.status_code == 412:
            raise RuntimeError("B站评论主列表接口触发 412")
        response.raise_for_status()
        payload = response.json()
        if payload.get("code") != 0:
            if payload.get("code") == -352:
                raise RuntimeError(f"B站评论主列表接口触发风控: {payload}")
            raise RuntimeError(f"B站评论主列表接口失败: {payload}")
        data = payload.get("data") or {}
        return data.get("replies") or [], data.get("cursor") or {}

    async def _scan_dynamic_user(self, uid: str) -> None:
        if not self.config.bili_sessdata:
            raise RuntimeError("监控 B 站动态需要配置 BILI_SESSDATA")

        items = await self._get_dynamic_items(uid)
        if items:
            author_name = self._dynamic_author_info(items[0], uid).get("name", "").strip()
            if author_name and author_name != f"UID {uid}":
                self.state.set_dynamic_uid_name(uid, author_name)
        dynamic_ids = [dynamic_id for item in items if (dynamic_id := self._extract_dynamic_id(item))]
        if not self.state.has_dynamic_seed(uid):
            self.state.prime_dynamic_ids(uid, dynamic_ids)
            await self._scan_dynamic_comment_items(uid, items)
            LOGGER.info("primed dynamic watch uid=%s with %s seen items", uid, len(dynamic_ids))
            return

        new_items = [
            item for item in items
            if (dynamic_id := self._extract_dynamic_id(item)) and not self.state.is_seen_dynamic(uid, dynamic_id)
        ]
        new_items.sort(key=self._dynamic_pub_ts)

        for item in new_items:
            dynamic_id = self._extract_dynamic_id(item)
            if not dynamic_id:
                continue
            text = self._build_dynamic_message(uid, item)
            await self._fanout_message(text)
            self.state.mark_seen_dynamic(uid, dynamic_id)
            LOGGER.info("recorded watched dynamic uid=%s dynamic_id=%s", uid, dynamic_id)

        await self._scan_dynamic_comment_items(uid, items)

    async def _scan_dynamic_comment_items(self, uid: str, items: list[dict[str, Any]]) -> None:
        seen_thread_keys: set[str] = set()
        for item in items:
            meta = self._extract_dynamic_comment_watch_meta(uid, item)
            if not meta:
                continue
            thread_key = str(meta["thread_key"])
            if thread_key in seen_thread_keys:
                continue
            seen_thread_keys.add(thread_key)
            try:
                await self._scan_dynamic_comment_thread(meta)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self._is_transient_bili_error(exc):
                    LOGGER.warning(
                        "Bilibili dynamic comment scan degraded for uid=%s dynamic_id=%s: %s",
                        uid,
                        meta["dynamic_id"],
                        self._format_error_text(exc),
                    )
                    continue
                LOGGER.exception(
                    "Bilibili dynamic comment scan failed for uid=%s dynamic_id=%s",
                    uid,
                    meta["dynamic_id"],
                )

    def _extract_dynamic_comment_watch_meta(self, watched_uid: str, item: dict[str, Any]) -> dict[str, Any] | None:
        item_type = str(item.get("type") or "").strip()
        if item_type not in {"DYNAMIC_TYPE_DRAW", "DYNAMIC_TYPE_WORD"}:
            return None

        basic = item.get("basic") or {}
        rtype = str(basic.get("rtype") or "").strip()
        if rtype and rtype != "2":
            return None

        oid = str(basic.get("rid_str") or "").strip()
        dynamic_id = self._extract_dynamic_id(item)
        if not oid or not dynamic_id:
            return None

        author = self._dynamic_author_info(item, watched_uid)
        jump_url = self._extract_dynamic_jump_url(item, dynamic_id)
        return {
            "thread_key": f"dynamic:{dynamic_id}",
            "dynamic_id": dynamic_id,
            "oid": oid,
            "comment_type": 11,
            "owner_mid": author["mid"],
            "owner_name": author["name"],
            "type_label": self._dynamic_type_label(item),
            "summary": self._summarize_dynamic_item(item),
            "jump_url": jump_url,
        }

    async def _scan_dynamic_comment_thread(
        self,
        meta: dict[str, Any],
        *,
        prime_on_first_seen: bool = True,
    ) -> None:
        found: dict[str, dict[str, Any]] = {}
        watched_mids = set(self.state.commenter_mids())
        watched_names = set(self.state.commenter_names())
        thread_key = str(meta["thread_key"])
        oid = str(meta["oid"])

        next_cursor = 0
        completed_pages = 0
        for _ in range(self.config.bili_max_pages):
            try:
                replies, cursor = await self._get_dynamic_root_replies_page(oid, next_cursor)
            except Exception as exc:
                if completed_pages > 0 and self._is_transient_bili_error(exc):
                    LOGGER.warning(
                        "Bilibili dynamic comment scan stopped early for dynamic_id=%s after %s pages: %s",
                        meta["dynamic_id"],
                        completed_pages,
                        self._format_error_text(exc),
                    )
                    break
                raise

            completed_pages += 1
            if not replies:
                break

            for root in replies:
                root_rpid = str(root["rpid"])
                match = self._match_commenter(
                    root,
                    meta,
                    watched_mids=watched_mids,
                    watched_names=watched_names,
                )
                if match:
                    found.setdefault(root_rpid, {"reply": root, "match": match})

                preview_sub_replies = root.get("replies") or []
                for sub in preview_sub_replies:
                    sub_rpid = str(sub["rpid"])
                    match = self._match_commenter(
                        sub,
                        meta,
                        watched_mids=watched_mids,
                        watched_names=watched_names,
                    )
                    if match:
                        found.setdefault(sub_rpid, {"reply": sub, "match": match})

                has_extra_sub_replies = (root.get("rcount") or 0) > len(preview_sub_replies)
                need_full_sub_scan = (
                    (root.get("reply_control") or {}).get("up_reply")
                    or (
                        has_extra_sub_replies
                        and (watched_mids or watched_names)
                    )
                )
                if need_full_sub_scan:
                    async for sub in self._iter_dynamic_sub_replies(oid, root["rpid"]):
                        sub_rpid = str(sub["rpid"])
                        match = self._match_commenter(
                            sub,
                            meta,
                            watched_mids=watched_mids,
                            watched_names=watched_names,
                        )
                        if match:
                            found.setdefault(sub_rpid, {"reply": sub, "match": match})

            if cursor.get("is_end"):
                break
            next_cursor = int(cursor.get("next") or 0)
            if not next_cursor:
                break
            await asyncio.sleep(0.2)

        if prime_on_first_seen and not self.state.has_dynamic_comment_seed(thread_key):
            self.state.prime_dynamic_comment_rpids(
                thread_key,
                [str(item["reply"]["rpid"]) for item in found.values()],
            )
            LOGGER.info(
                "primed dynamic comment watch dynamic_id=%s with %s seen replies",
                meta["dynamic_id"],
                len(found),
            )
            return

        new_replies = [
            item for item in found.values()
            if not self.state.is_seen_dynamic_comment_reply(thread_key, str(item["reply"]["rpid"]))
        ]
        new_replies.sort(key=lambda item: item["reply"].get("ctime", 0))

        for item in new_replies:
            reply = item["reply"]
            text = self._build_dynamic_comment_message(meta, reply, item["match"])
            await self._fanout_message(text)
            self.state.mark_seen_dynamic_comment_reply(thread_key, str(reply["rpid"]))
            LOGGER.info(
                "recorded watched dynamic comment dynamic_id=%s rpid=%s mid=%s",
                meta["dynamic_id"],
                reply["rpid"],
                item["match"]["mid"],
            )

    async def _scan_specific_dynamic_comment(self, dynamic_id: str) -> None:
        try:
            meta = await self._resolve_dynamic_comment_target_by_id(dynamic_id, force_refresh=True)
        except BiliDynamicUnavailableError as exc:
            await self._handle_unavailable_dynamic_comment_watch(dynamic_id, str(exc))
            return
        await self._scan_dynamic_comment_thread(meta, prime_on_first_seen=False)

    async def _resolve_dynamic_comment_target_by_id(
        self,
        dynamic_id: str,
        *,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        cached = self.dynamic_comment_meta_cache.get(dynamic_id)
        if cached and not force_refresh:
            self.dynamic_comment_meta_cache.move_to_end(dynamic_id)
            return dict(cached)

        item = await self._fetch_dynamic_detail_item(dynamic_id)
        type_label = self._specific_dynamic_comment_type_label(item)
        if not type_label:
            raise RuntimeError(f"动态 {dynamic_id} 不是图文/文字动态，当前无法按动态ID监听评论")

        oid, comment_type = extract_dynamic_comment_thread(item)
        if not oid:
            raise RuntimeError(f"动态 {dynamic_id} 未拿到有效评论线程，暂时无法监听")
        if comment_type not in {0, 11}:
            raise RuntimeError(f"动态 {dynamic_id} 返回了不支持的评论类型 {comment_type}，暂时无法监听")

        basic = item.get("basic") or {}
        author = self._dynamic_author_info(item, str(basic.get("uid") or ""))
        meta = {
            "thread_key": f"dynamic:{dynamic_id}",
            "dynamic_id": dynamic_id,
            "oid": oid,
            "comment_type": 11,
            "owner_mid": author["mid"],
            "owner_name": author["name"],
            "type_label": type_label,
            "summary": self._summarize_dynamic_item(item),
            "jump_url": self._extract_dynamic_jump_url(item, dynamic_id),
        }
        self.dynamic_comment_meta_cache[dynamic_id] = dict(meta)
        while len(self.dynamic_comment_meta_cache) > 200:
            self.dynamic_comment_meta_cache.popitem(last=False)
        return meta

    async def _handle_unavailable_dynamic_comment_watch(self, dynamic_id: str, reason: str) -> None:
        self.dynamic_comment_meta_cache.pop(dynamic_id, None)
        removed = self.state.remove_dynamic_comment_id(dynamic_id)
        LOGGER.info(
            "auto removed unavailable dynamic comment watch dynamic_id=%s reason=%s removed=%s",
            dynamic_id,
            reason,
            removed,
        )
        if not removed:
            return
        text = (
            "【图文动态评论监听已自动移除】\n"
            f"动态ID: {dynamic_id}\n"
            f"原因: {reason}\n"
            "说明: 这条动态已删除、失效或当前不可见，后续不会继续扫描。"
        )
        await self._fanout_message(text)

    def _specific_dynamic_comment_type_label(self, item: dict[str, Any]) -> str | None:
        item_type = str(item.get("type") or "").strip()
        if item_type == "DYNAMIC_TYPE_DRAW":
            return "图文动态"
        if item_type == "DYNAMIC_TYPE_WORD":
            return "文字动态"

        modules = self._dynamic_modules_map(item)
        top = modules.get("module_top") or {}
        display = top.get("display") or {}
        album = display.get("album") or {}
        if album.get("pics"):
            return "图文动态"
        if modules.get("module_content"):
            return "文字动态"
        return None

    async def _get_dynamic_items(self, uid: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        offset = ""
        for _ in range(self.config.bili_dynamic_max_pages):
            payload = await self._request_dynamic_space(uid, offset)
            data = payload.get("data") or {}
            page_items = data.get("items") or []
            if not page_items:
                break
            items.extend(page_items)
            if not data.get("has_more"):
                break
            offset = str(data.get("offset") or "")
            if not offset:
                break
            await asyncio.sleep(0.2)
        return items

    async def _request_dynamic_space(self, uid: str, offset: str) -> dict[str, Any]:
        params: dict[str, Any] = {
            "host_mid": uid,
            "timezone_offset": -480,
            "features": "itemOpusStyle",
        }
        if offset:
            params["offset"] = offset

        last_exc: Exception | None = None
        for url in (BILI_DYNAMIC_SPACE_DESKTOP_URL, BILI_DYNAMIC_SPACE_URL):
            try:
                response = await self.client.get(url, params=params)
                if response.status_code == 412:
                    raise RuntimeError(f"B站动态接口触发 412: {url}")
                response.raise_for_status()
                payload = response.json()
                if payload.get("code") == 0:
                    return payload
                if payload.get("code") == -352:
                    raise RuntimeError(f"B站动态接口触发风控: {payload}")
                raise RuntimeError(f"B站动态接口失败: {payload}")
            except Exception as exc:
                last_exc = exc
        raise last_exc or RuntimeError(f"获取用户动态失败: uid={uid}")

    async def _get_dynamic_root_replies_page(self, oid: str, next_cursor: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        response = await self.client.get(
            BILI_REPLY_MAIN_URL,
            params={
                "type": 11,
                "oid": oid,
                "mode": BILI_REPLY_MODE_LATEST,
                "next": next_cursor,
                "ps": BILI_REPLY_PAGE_SIZE,
            },
        )
        if response.status_code == 412:
            raise RuntimeError("B站动态评论主列表接口触发 412")
        response.raise_for_status()
        payload = response.json()
        if payload.get("code") != 0:
            if payload.get("code") == -352:
                raise RuntimeError(f"B站动态评论主列表接口触发风控: {payload}")
            raise RuntimeError(f"B站动态评论主列表接口失败: {payload}")
        data = payload.get("data") or {}
        return data.get("replies") or [], data.get("cursor") or {}

    async def _fetch_dynamic_detail_item(self, dynamic_id: str) -> dict[str, Any]:
        params = {
            "id": dynamic_id,
            "timezone_offset": -480,
            "features": "itemOpusStyle",
        }
        last_exc: Exception | None = None
        for url in (BILI_DYNAMIC_DETAIL_DESKTOP_URL, BILI_DYNAMIC_DETAIL_URL):
            try:
                response = await self.client.get(url, params=params)
                if response.status_code == 412:
                    raise RuntimeError("B站动态详情接口触发 412")
                response.raise_for_status()
                payload = response.json()
                if payload.get("code") == 0:
                    data = payload.get("data") or {}
                    item = data.get("item") or data.get("detail") or data
                    if isinstance(item, dict) and item.get("id_str"):
                        if is_dynamic_unavailable_item(item):
                            raise BiliDynamicUnavailableError(f"动态 {dynamic_id} 已删除、失效或当前不可见")
                        return item
                    raise RuntimeError("B站动态详情接口未返回有效动态内容")
                if payload.get("code") == -352:
                    last_exc = RuntimeError("B站动态详情接口触发风控")
                    continue
                if is_dynamic_unavailable_payload(payload):
                    raise BiliDynamicUnavailableError(f"动态 {dynamic_id} 已删除、失效或当前不可见")
                raise RuntimeError(f"B站动态详情接口失败: {payload}")
            except BiliDynamicUnavailableError:
                raise
            except Exception as exc:
                last_exc = exc

        return await self._fetch_dynamic_detail_item_from_page(dynamic_id, last_exc)

    async def _fetch_dynamic_detail_item_from_page(
        self,
        dynamic_id: str,
        last_exc: Exception | None = None,
    ) -> dict[str, Any]:
        try:
            response = await self.client.get(
                f"https://www.bilibili.com/opus/{dynamic_id}",
                headers=BILI_HTML_HEADERS,
                follow_redirects=True,
            )
            response.raise_for_status()
        except Exception as exc:
            if last_exc:
                if isinstance(last_exc, BiliDynamicUnavailableError):
                    raise last_exc
                raise RuntimeError(f"{last_exc}；且页面兜底解析失败") from exc
            raise RuntimeError("打开动态详情页失败") from exc

        match = INITIAL_STATE_RE.search(response.text)
        if not match:
            if isinstance(last_exc, BiliDynamicUnavailableError):
                raise last_exc
            raise RuntimeError("动态详情页未找到初始化数据")
        payload = json.loads(match.group(1))
        item = payload.get("detail") or {}
        if not isinstance(item, dict) or str(item.get("id_str") or "").strip() != dynamic_id:
            if isinstance(last_exc, BiliDynamicUnavailableError):
                raise last_exc
            raise RuntimeError(f"动态详情页未返回目标动态: {dynamic_id}")
        if is_dynamic_unavailable_item(item):
            raise BiliDynamicUnavailableError(f"动态 {dynamic_id} 已删除、失效或当前不可见")
        return item

    async def _iter_sub_replies(self, aid: int, root_rpid: int):
        page_no = 1
        while True:
            response = await self.client.get(
                BILI_SUB_REPLY_URL,
                params={"type": 1, "oid": aid, "root": root_rpid, "pn": page_no},
            )
            if response.status_code == 412:
                LOGGER.warning("B站子评论接口触发 412，跳过该楼层: root=%s", root_rpid)
                return
            response.raise_for_status()
            payload = response.json()
            if payload.get("code") != 0:
                if payload.get("code") == -352:
                    LOGGER.warning("B站子评论接口触发风控，跳过该楼层: root=%s payload=%s", root_rpid, payload)
                    return
                raise RuntimeError(f"B站子评论接口失败: {payload}")
            data = payload.get("data") or {}
            replies = data.get("replies") or []
            page = data.get("page") or {}
            if not replies:
                return

            for reply in replies:
                yield reply

            size = page.get("size", 10)
            count = page.get("count", 0)
            if page_no * size >= count:
                return

            page_no += 1
            await asyncio.sleep(0.2)

    async def _iter_dynamic_sub_replies(self, oid: str, root_rpid: int):
        page_no = 1
        while True:
            response = await self.client.get(
                BILI_SUB_REPLY_URL,
                params={"type": 11, "oid": oid, "root": root_rpid, "pn": page_no},
            )
            if response.status_code == 412:
                LOGGER.warning("B站动态子评论接口触发 412，跳过该楼层: root=%s", root_rpid)
                return
            response.raise_for_status()
            payload = response.json()
            if payload.get("code") != 0:
                if payload.get("code") == -352:
                    LOGGER.warning("B站动态子评论接口触发风控，跳过该楼层: root=%s payload=%s", root_rpid, payload)
                    return
                raise RuntimeError(f"B站动态子评论接口失败: {payload}")
            data = payload.get("data") or {}
            replies = data.get("replies") or []
            page = data.get("page") or {}
            if not replies:
                return

            for reply in replies:
                yield reply

            size = page.get("size", 10)
            count = page.get("count", 0)
            if page_no * size >= count:
                return

            page_no += 1
            await asyncio.sleep(0.2)

    async def _get_video_meta(self, bvid: str) -> dict[str, Any]:
        if bvid in self.video_cache:
            cached = self.video_cache.pop(bvid)
            self.video_cache[bvid] = cached
            return cached

        response = await self.client.get(BILI_VIEW_URL, params={"bvid": bvid})
        response.raise_for_status()
        payload = response.json()
        if payload.get("code") != 0 or not payload.get("data"):
            raise RuntimeError(f"获取视频信息失败: {payload}")

        data = payload["data"]
        meta = {
            "aid": data["aid"],
            "title": data["title"],
            "owner_mid": str(data["owner"]["mid"]),
            "owner_name": data["owner"]["name"],
        }
        self.video_cache[bvid] = meta
        while len(self.video_cache) > 100:
            self.video_cache.popitem(last=False)
        return meta

    def _is_up_comment(self, reply: dict[str, Any], owner_mid: str) -> bool:
        member = reply.get("member") or {}
        return str(member.get("mid")) == owner_mid

    def _match_commenter(
        self,
        reply: dict[str, Any],
        meta: dict[str, Any],
        *,
        watched_mids: set[str] | None = None,
        watched_names: set[str] | None = None,
    ) -> dict[str, str] | None:
        member = reply.get("member") or {}
        mid = str(member.get("mid") or "")
        uname = member.get("uname") or "未知用户"
        if not mid:
            return None

        watched_mids = watched_mids or set()
        watched_names = watched_names or set()

        if mid == meta["owner_mid"]:
            return {
                "mid": mid,
                "uname": uname,
                "kind": "UP主",
            }

        if mid in watched_mids:
            return {
                "mid": mid,
                "uname": uname,
                "kind": "指定UID",
            }

        if uname in watched_names:
            return {
                "mid": mid,
                "uname": uname,
                "kind": "指定昵称",
            }

        return None

    def _build_message(self, meta: dict[str, Any], bvid: str, reply: dict[str, Any], match: dict[str, str]) -> str:
        ctime = reply.get("ctime", 0)
        local_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ctime))
        content = (reply.get("content") or {}).get("message", "").strip() or "<空评论>"
        return (
            "【B站评论提醒】\n"
            f"视频: {meta['title']}\n"
            f"UP: {meta['owner_name']}\n"
            f"评论者: {match['uname']} ({match['mid']})\n"
            f"类型: {match['kind']}\n"
            f"时间: {local_time}\n"
            f"评论: {content}\n"
            f"BV号: {bvid}"
        )

    def _build_dynamic_comment_message(
        self,
        meta: dict[str, Any],
        reply: dict[str, Any],
        match: dict[str, str],
    ) -> str:
        ctime = reply.get("ctime", 0)
        local_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ctime))
        content = (reply.get("content") or {}).get("message", "").strip() or "<空评论>"
        lines = [
            "【B站动态评论提醒】",
            f"UP: {meta['owner_name']} ({meta['owner_mid']})",
            f"动态类型: {meta['type_label']}",
            f"动态内容: {meta['summary']}",
            f"评论者: {match['uname']} ({match['mid']})",
            f"类型: {match['kind']}",
            f"时间: {local_time}",
            f"评论: {content}",
            f"动态ID: {meta['dynamic_id']}",
        ]
        if meta.get("jump_url"):
            lines.append(f"链接: {meta['jump_url']}")
        return "\n".join(lines)

    def _extract_dynamic_id(self, item: dict[str, Any]) -> str | None:
        dynamic_id = str(item.get("id_str") or "").strip()
        return dynamic_id or None

    def _dynamic_modules_map(self, item: dict[str, Any]) -> dict[str, Any]:
        modules = item.get("modules") or {}
        if isinstance(modules, dict):
            return modules
        if not isinstance(modules, list):
            return {}

        flattened: dict[str, Any] = {}
        for module in modules:
            if not isinstance(module, dict):
                continue
            for key, value in module.items():
                if key == "module_type":
                    continue
                flattened.setdefault(key, value)
        return flattened

    def _dynamic_author_info(self, item: dict[str, Any], watched_uid: str) -> dict[str, Any]:
        author = self._dynamic_modules_map(item).get("module_author") or {}
        user = author.get("user") or {}
        return {
            "name": author.get("name") or user.get("name") or f"UID {watched_uid}",
            "mid": str(author.get("mid") or user.get("mid") or watched_uid),
            "pub_ts": int(author.get("pub_ts") or 0),
            "pub_text": str(author.get("pub_action") or author.get("pub_text") or "").strip(),
            "more": author.get("more") or {},
        }

    def _dynamic_pub_ts(self, item: dict[str, Any]) -> int:
        return int(self._dynamic_author_info(item, "").get("pub_ts") or 0)

    def _build_dynamic_message(self, watched_uid: str, item: dict[str, Any]) -> str:
        author = self._dynamic_author_info(item, watched_uid)
        dynamic_id = self._extract_dynamic_id(item) or "未知"
        author_name = author["name"]
        author_mid = author["mid"]
        pub_ts = int(author["pub_ts"] or 0)
        local_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(pub_ts)) if pub_ts else "未知"
        pub_text = str(author["pub_text"] or "").strip() or self._dynamic_type_label(item)
        content = self._summarize_dynamic_item(item)
        dynamic_bvid = self._extract_dynamic_bvid(item)
        jump_url = self._extract_dynamic_jump_url(item, dynamic_id)
        lines = [
            "【B站动态提醒】",
            f"UP: {author_name} ({author_mid})",
            f"发布信息: {pub_text}",
            f"类型: {self._dynamic_type_label(item)}",
            f"时间: {local_time}",
            f"内容: {content}",
        ]
        if dynamic_bvid:
            lines.append(f"BV号: {dynamic_bvid}")
        lines.extend([
            f"动态ID: {dynamic_id}",
        ])
        if jump_url:
            lines.append(f"链接: {jump_url}")
        return "\n".join(lines)

    def _dynamic_type_label(self, item: dict[str, Any]) -> str:
        item_type = str(item.get("type") or "").strip()
        mapping = {
            "DYNAMIC_TYPE_WORD": "文字动态",
            "DYNAMIC_TYPE_DRAW": "图文动态",
            "DYNAMIC_TYPE_AV": "视频动态",
            "DYNAMIC_TYPE_ARTICLE": "专栏动态",
            "DYNAMIC_TYPE_FORWARD": "转发动态",
            "DYNAMIC_TYPE_COMMON_SQUARE": "卡片动态",
            "DYNAMIC_TYPE_LIVE_RCMD": "直播动态",
            "DYNAMIC_TYPE_NONE": "失效动态",
        }
        return mapping.get(item_type, item_type or "动态")

    def _extract_dynamic_desc_text(self, item: dict[str, Any]) -> str:
        modules = self._dynamic_modules_map(item)
        module_desc = modules.get("module_desc") or {}
        if isinstance(module_desc, dict):
            text = str(module_desc.get("text") or "").strip()
            if text:
                return text

        module_dynamic = modules.get("module_dynamic") or {}
        desc = module_dynamic.get("desc")
        if isinstance(desc, dict):
            text = str(desc.get("text") or "").strip()
            if text:
                return text
        if isinstance(desc, str):
            text = desc.strip()
            if text:
                return text
        return ""

    def _extract_forward_original(self, item: dict[str, Any]) -> dict[str, Any] | None:
        if isinstance(item.get("orig"), dict):
            return item["orig"]

        module_dynamic = self._dynamic_modules_map(item).get("module_dynamic") or {}
        dyn_forward = module_dynamic.get("dyn_forward") or {}
        original = dyn_forward.get("item")
        return original if isinstance(original, dict) else None

    def _summarize_dynamic_item(self, item: dict[str, Any]) -> str:
        modules = self._dynamic_modules_map(item)
        module_dynamic = modules.get("module_dynamic") or {}
        text = self._extract_dynamic_desc_text(item)
        if text:
            summary = text
        else:
            summary = self._summarize_dynamic_major(module_dynamic.get("major") or {})
            if not summary:
                summary = self._summarize_dynamic_module(module_dynamic)

        if str(item.get("type") or "") == "DYNAMIC_TYPE_FORWARD":
            original = self._extract_forward_original(item) or {}
            original_summary = self._summarize_dynamic_item(original) if original else ""
            if original_summary:
                summary = f"{summary}\n转发内容: {original_summary}" if summary else f"转发内容: {original_summary}"

        return self._truncate_text(summary or "<无文字内容>", 220)

    def _summarize_dynamic_module(self, module_dynamic: dict[str, Any]) -> str:
        dyn_archive = module_dynamic.get("dyn_archive") or {}
        if dyn_archive:
            return self._join_summary_parts(dyn_archive.get("title"), dyn_archive.get("desc"))

        dyn_article = module_dynamic.get("dyn_article") or {}
        if dyn_article:
            return self._join_summary_parts(dyn_article.get("title"), dyn_article.get("desc"))

        dyn_common = module_dynamic.get("dyn_common") or {}
        if dyn_common:
            return self._join_summary_parts(dyn_common.get("title"), dyn_common.get("desc"))

        dyn_music = module_dynamic.get("dyn_music") or {}
        if dyn_music:
            return self._join_summary_parts(dyn_music.get("title"), dyn_music.get("label"))

        dyn_pgc = module_dynamic.get("dyn_pgc") or {}
        if dyn_pgc:
            return self._join_summary_parts(dyn_pgc.get("title"), dyn_pgc.get("desc"))

        dyn_cour = module_dynamic.get("dyn_cour_season") or {}
        if dyn_cour:
            return self._join_summary_parts(dyn_cour.get("title"), dyn_cour.get("desc"))

        dyn_ugc = module_dynamic.get("dyn_ugc") or {}
        if dyn_ugc:
            return self._join_summary_parts(dyn_ugc.get("title"), dyn_ugc.get("desc"))

        dyn_draw = module_dynamic.get("dyn_draw") or {}
        if dyn_draw:
            count = len(dyn_draw.get("items") or [])
            return f"发布了{count}张图片" if count else "发布了图片动态"

        dyn_live = module_dynamic.get("dyn_live_rcmd") or {}
        if dyn_live:
            return self._summarize_live_rcmd(dyn_live)

        return ""

    def _summarize_dynamic_major(self, major: dict[str, Any]) -> str:
        major_type = str(major.get("type") or "")
        if major_type == "MAJOR_TYPE_ARCHIVE":
            archive = major.get("archive") or {}
            return self._join_summary_parts(archive.get("title"), archive.get("desc"))
        if major_type == "MAJOR_TYPE_ARTICLE":
            article = major.get("article") or {}
            return self._join_summary_parts(article.get("title"), article.get("desc"))
        if major_type == "MAJOR_TYPE_COMMON":
            common = major.get("common") or {}
            return self._join_summary_parts(common.get("title"), common.get("desc"))
        if major_type == "MAJOR_TYPE_OPUS":
            opus = major.get("opus") or {}
            summary = opus.get("summary") or {}
            return self._join_summary_parts(opus.get("title"), summary.get("text"))
        if major_type == "MAJOR_TYPE_PGC":
            pgc = major.get("pgc") or {}
            return self._join_summary_parts(pgc.get("title"), pgc.get("desc"))
        if major_type == "MAJOR_TYPE_COURSES":
            courses = major.get("courses") or {}
            return self._join_summary_parts(courses.get("title"), courses.get("desc"))
        if major_type == "MAJOR_TYPE_MUSIC":
            music = major.get("music") or {}
            return self._join_summary_parts(music.get("title"), music.get("label"))
        if major_type == "MAJOR_TYPE_UGC_SEASON":
            season = major.get("ugc_season") or {}
            return self._join_summary_parts(season.get("title"), season.get("desc"))
        if major_type == "MAJOR_TYPE_DRAW":
            draw = major.get("draw") or {}
            count = len(draw.get("items") or [])
            return f"发布了{count}张图片" if count else "发布了图片动态"
        if major_type == "MAJOR_TYPE_LIVE_RCMD":
            return self._summarize_live_rcmd(major.get("live_rcmd") or {})
        if major_type == "MAJOR_TYPE_NONE":
            return "动态已失效"
        return ""

    def _summarize_live_rcmd(self, live_rcmd: dict[str, Any]) -> str:
        content = live_rcmd.get("content")
        if not content:
            return "发布了直播动态"
        try:
            payload = json.loads(content)
        except (TypeError, json.JSONDecodeError):
            return "发布了直播动态"
        live_play_info = (((payload.get("live_play_info") or {}).get("live_play_info")) or {})
        title = live_play_info.get("title")
        area_name = live_play_info.get("area_name")
        return self._join_summary_parts(title, area_name) or "发布了直播动态"

    def _extract_dynamic_bvid(self, item: dict[str, Any]) -> str | None:
        modules = self._dynamic_modules_map(item)
        module_dynamic = modules.get("module_dynamic") or {}
        major = module_dynamic.get("major") or {}
        bvid_candidates = [
            (major.get("archive") or {}).get("bvid"),
            (module_dynamic.get("dyn_archive") or {}).get("bvid"),
            (module_dynamic.get("dyn_ugc") or {}).get("bvid"),
            (major.get("ugc_season") or {}).get("bvid"),
        ]
        for candidate in bvid_candidates:
            if normalized := normalize_bvid(str(candidate or "")):
                return normalized

        url_candidates = [
            (item.get("basic") or {}).get("jump_url"),
            (major.get("archive") or {}).get("jump_url"),
            (major.get("ugc_season") or {}).get("jump_url"),
            (module_dynamic.get("dyn_archive") or {}).get("jump_url"),
            (module_dynamic.get("dyn_ugc") or {}).get("jump_url"),
        ]
        for candidate in url_candidates:
            if bvid := self._extract_bvid_from_text(candidate):
                return bvid

        if str(item.get("type") or "") == "DYNAMIC_TYPE_FORWARD":
            if original := self._extract_forward_original(item):
                return self._extract_dynamic_bvid(original)
        return None

    def _extract_bvid_from_text(self, value: Any) -> str | None:
        text = str(value or "")
        if not text:
            return None
        match = BVID_RE.search(text)
        return normalize_bvid(match.group(0)) if match else None

    def _extract_dynamic_jump_url(self, item: dict[str, Any], dynamic_id: str) -> str | None:
        basic = item.get("basic") or {}
        modules = self._dynamic_modules_map(item)
        module_dynamic = modules.get("module_dynamic") or {}
        major = module_dynamic.get("major") or {}
        author_more = (self._dynamic_author_info(item, "").get("more") or {})
        three_point_items = author_more.get("three_point_items") or []
        copy_links = [
            ((entry.get("params") or {}).get("link"))
            for entry in three_point_items
            if entry.get("type") == "THREE_POINT_COPY"
        ]
        dyn_archive = module_dynamic.get("dyn_archive") or {}
        candidates = [
            basic.get("jump_url"),
            *copy_links,
            (major.get("archive") or {}).get("jump_url"),
            (major.get("article") or {}).get("jump_url"),
            (major.get("common") or {}).get("jump_url"),
            (major.get("pgc") or {}).get("jump_url"),
            (major.get("courses") or {}).get("jump_url"),
            (major.get("music") or {}).get("jump_url"),
            (major.get("ugc_season") or {}).get("jump_url"),
            (major.get("upower_common") or {}).get("jump_url"),
            f"https://www.bilibili.com/video/{dyn_archive.get('bvid')}" if dyn_archive.get("bvid") else "",
            f"https://www.bilibili.com/opus/{dynamic_id}" if dynamic_id else "",
            f"https://t.bilibili.com/{dynamic_id}" if dynamic_id else "",
        ]
        for raw_url in candidates:
            url = self._normalize_jump_url(raw_url)
            if url:
                return url
        return None

    def _normalize_jump_url(self, raw_url: Any) -> str | None:
        url = str(raw_url or "").strip()
        if not url:
            return None
        if url.startswith("//"):
            return f"https:{url}"
        if url.startswith("/"):
            return f"https://www.bilibili.com{url}"
        if url.startswith("http://") or url.startswith("https://"):
            return url
        return f"https://{url}"

    def _join_summary_parts(self, *parts: Any) -> str:
        cleaned = [str(part).strip() for part in parts if str(part or "").strip()]
        return " | ".join(cleaned)

    def _truncate_text(self, text: str, max_len: int) -> str:
        normalized = " ".join(text.split())
        if len(normalized) <= max_len:
            return normalized
        return normalized[: max_len - 3] + "..."

    async def _fanout_message(self, text: str) -> None:
        group_targets = self.state.enabled_targets()
        user_targets = self.state.enabled_user_targets()
        if not group_targets and not user_targets:
            LOGGER.warning("detected watched Bilibili event but no enabled QQ targets")
            return

        for group_openid in group_targets:
            try:
                await self.qq.send_group_text(group_openid, text)
                LOGGER.info("sent alert to group_openid=%s", group_openid)
            except QQBotAPIError as exc:
                LOGGER.error(
                    "failed to send proactive QQ message to %s: status=%s payload=%s",
                    group_openid,
                    exc.status_code,
                    exc.payload,
                )
            except Exception:
                LOGGER.exception("failed to send proactive QQ message to %s", group_openid)

        for openid in user_targets:
            try:
                await self.qq.send_user_text(openid, text)
                LOGGER.info("sent alert to user_openid=%s", openid)
            except QQBotAPIError as exc:
                LOGGER.error(
                    "failed to send proactive QQ message to user %s: status=%s payload=%s",
                    openid,
                    exc.status_code,
                    exc.payload,
                )
            except Exception:
                LOGGER.exception("failed to send proactive QQ message to user %s", openid)


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    config = Config.from_env()
    state = StateStore(
        config.state_path,
        initial_bvids=config.initial_bvids,
        manual_targets=config.target_group_openids,
        manual_user_targets=config.target_user_openids,
        initial_commenter_mids=config.initial_commenter_mids,
        initial_commenter_names=config.initial_commenter_names,
        initial_dynamic_uids=config.initial_dynamic_uids,
        initial_dynamic_comment_ids=config.initial_dynamic_comment_ids,
    )
    qq = QQBotClient(config)
    monitor = BilibiliMonitor(config, qq, state)
    gateway = QQGatewayClient(config, qq, state)

    LOGGER.info(
        "starting monitor with %s video subscriptions, %s dynamic watches, and %s specific dynamic comment watches",
        len(state.subscriptions()),
        len(state.dynamic_uids()),
        len(state.dynamic_comment_ids()),
    )
    try:
        await asyncio.gather(
            gateway.run_forever(),
            monitor.run_forever(),
        )
    finally:
        await gateway.aclose()
        await monitor.aclose()
        await qq.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        LOGGER.info("shutdown requested by user")
