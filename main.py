from __future__ import annotations

import asyncio
import contextlib
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
BILI_SUB_REPLY_URL = "https://api.bilibili.com/x/v2/reply/reply"
BILI_ROOT_SORT = 0
BVID_RE = re.compile(r"BV[0-9A-Za-z]{10}", re.IGNORECASE)
USER_MID_RE = re.compile(r"\d{1,20}")
DEFAULT_HEADERS = {"User-Agent": "bili-qq-monitor/1.0"}
BILI_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.bilibili.com/",
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


class QQBotAPIError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, payload: Any = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload


@dataclass(slots=True)
class Config:
    qq_appid: str
    qq_app_secret: str
    target_group_openids: list[str]
    target_user_openids: list[str]
    initial_bvids: list[str]
    initial_commenter_mids: list[str]
    initial_commenter_names: list[str]
    bili_sessdata: str | None
    bili_poll_interval: int
    bili_max_pages: int
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
            bili_sessdata=normalize_sessdata(os.getenv("BILI_SESSDATA", "")),
            bili_poll_interval=max(10, int(os.getenv("BILI_POLL_INTERVAL", "30"))),
            bili_max_pages=max(1, int(os.getenv("BILI_MAX_PAGES", "10"))),
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
        state["seen_rpids"] = loaded.get("seen_rpids", {})
        state["handled_dispatch_ids"] = loaded.get("handled_dispatch_ids", [])
        return state

    def _default_state(self) -> dict[str, Any]:
        return {
            "targets": {},
            "user_targets": {},
            "subscriptions": [],
            "commenter_mids": [],
            "commenter_names": [],
            "seen_rpids": {},
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
                        "已记录该群可接收通知。使用 @机器人 /watch BV号 添加视频，或发送 /h 查看全部命令。",
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
                        "已记录你可接收提醒。直接发送 /watch BV号 添加视频，或发送 /h 查看全部命令。",
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
                "已接入。@我发送 /watch BV号 添加视频，/watchuser 昵称 添加评论用户监听，/h 查看全部命令。",
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
                "已接入。直接发送 /watch BV号 添加视频，/watchuser 昵称 添加评论用户监听，/h 查看全部命令。",
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
        response = self._run_command(command)
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
        response = self._run_command(command)
        await self._safe_user_reply(openid, response, msg_id=msg_id)

    def _run_command(self, command: str) -> str:
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
        if lowered.startswith("/unwatchuser "):
            return f"/unwatchuser {cleaned.split(maxsplit=1)[1]}"
        if lowered.startswith("/unwatchuid "):
            return f"/unwatchuid {cleaned.split(maxsplit=1)[1]}"
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
            "/status: 查看当前状态\n"
            "/h 或 /help: 查看本帮助"
        )

    def _build_status_text(self) -> str:
        bvids = self.state.subscriptions()
        commenter_mids = self.state.commenter_mids()
        commenter_names = self.state.commenter_names()
        group_targets = self.state.list_targets()
        user_targets = self.state.list_user_targets()
        active_group_targets = [group_openid for group_openid, meta in group_targets.items() if meta.get("enabled")]
        active_user_targets = [openid for openid, meta in user_targets.items() if meta.get("enabled")]
        return (
            "状态:\n"
            f"监控视频: {', '.join(bvids) if bvids else '无'}\n"
            f"指定评论UID: {', '.join(commenter_mids) if commenter_mids else '无'}\n"
            f"指定评论昵称: {', '.join(commenter_names) if commenter_names else '无'}\n"
            f"有效群目标: {len(active_group_targets)} 个\n"
            f"有效私聊目标: {len(active_user_targets)} 个\n"
            "注意: 官方文档已提示主动推送能力可能被拒绝，若告警发送失败请查看服务日志。"
        )

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
        self.failure_streaks: dict[str, int] = {}
        self.failure_notified: set[str] = set()

    async def aclose(self) -> None:
        await self.client.aclose()

    async def run_forever(self) -> None:
        while True:
            subscriptions = self.state.subscriptions()
            if not subscriptions:
                LOGGER.info("no BVID configured yet, waiting for /watch command")
            for bvid in subscriptions:
                try:
                    await self._scan_video(bvid)
                    self._record_scan_success(bvid)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    LOGGER.exception("Bilibili scan failed for bvid=%s", bvid)
                    await self._record_scan_failure(bvid, exc)
            await asyncio.sleep(self.config.bili_poll_interval)

    def _record_scan_success(self, bvid: str) -> None:
        if self.failure_streaks.get(bvid, 0) > 0:
            LOGGER.info("Bilibili scan recovered for bvid=%s after %s failures", bvid, self.failure_streaks[bvid])
        self.failure_streaks[bvid] = 0
        self.failure_notified.discard(bvid)

    async def _record_scan_failure(self, bvid: str, exc: Exception) -> None:
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

    async def _scan_video(self, bvid: str) -> None:
        meta = await self._get_video_meta(bvid)
        found: dict[str, dict[str, Any]] = {}

        for page_no in range(1, self.config.bili_max_pages + 1):
            root_payload = await self.client.get(
                BILI_REPLY_URL,
                params={"type": 1, "oid": meta["aid"], "pn": page_no, "sort": BILI_ROOT_SORT},
            )
            root_payload.raise_for_status()
            root_json = root_payload.json()
            if root_json.get("code") != 0:
                raise RuntimeError(f"B站评论接口失败: {root_json}")
            root_data = root_json.get("data") or {}
            replies = root_data.get("replies") or []
            if not replies:
                break

            for root in replies:
                root_rpid = str(root["rpid"])
                match = self._match_commenter(root, meta)
                if match:
                    found.setdefault(root_rpid, {"reply": root, "match": match})

                preview_sub_replies = root.get("replies") or []
                for sub in preview_sub_replies:
                    sub_rpid = str(sub["rpid"])
                    match = self._match_commenter(sub, meta)
                    if match:
                        found.setdefault(sub_rpid, {"reply": sub, "match": match})

                has_extra_sub_replies = (root.get("rcount") or 0) > len(preview_sub_replies)
                need_full_sub_scan = (
                    (root.get("reply_control") or {}).get("up_reply")
                    or (
                        has_extra_sub_replies
                        and (self.state.commenter_mids() or self.state.commenter_names())
                    )
                )
                if need_full_sub_scan:
                    async for sub in self._iter_sub_replies(meta["aid"], root["rpid"]):
                        sub_rpid = str(sub["rpid"])
                        match = self._match_commenter(sub, meta)
                        if match:
                            found.setdefault(sub_rpid, {"reply": sub, "match": match})

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

    def _match_commenter(self, reply: dict[str, Any], meta: dict[str, Any]) -> dict[str, str] | None:
        member = reply.get("member") or {}
        mid = str(member.get("mid") or "")
        uname = member.get("uname") or "未知用户"
        if not mid:
            return None

        if mid == meta["owner_mid"]:
            return {
                "mid": mid,
                "uname": uname,
                "kind": "UP主",
            }

        if mid in set(self.state.commenter_mids()):
            return {
                "mid": mid,
                "uname": uname,
                "kind": "指定UID",
            }

        if uname in set(self.state.commenter_names()):
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

    async def _fanout_message(self, text: str) -> None:
        group_targets = self.state.enabled_targets()
        user_targets = self.state.enabled_user_targets()
        if not group_targets and not user_targets:
            LOGGER.warning("detected UP comment but no enabled QQ targets")
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
    )
    qq = QQBotClient(config)
    monitor = BilibiliMonitor(config, qq, state)
    gateway = QQGatewayClient(config, qq, state)

    LOGGER.info("starting monitor with %s subscriptions", len(state.subscriptions()))
    try:
        await asyncio.gather(
            gateway.run_forever(),
            monitor.run_forever(),
        )
    finally:
        await monitor.aclose()
        await qq.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        LOGGER.info("shutdown requested by user")
