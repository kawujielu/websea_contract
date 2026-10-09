#!/usr/bin/env python3
"""订阅 HL userFills。用法: python sub/sub_user_fills.py"""

import json
import time

import websocket

WS_URL = "wss://api.hyperliquid.xyz/ws"
USERS = ["0xf5d81a135f756ca16544e53c20fc20643ec3ad53"]
RECONNECT_DELAY = 5


def subscribe_msg(user):
    return json.dumps({"method": "subscribe", "subscription": {"type": "userFills", "user": user}})


def run(users=None, on_fill=None, on_subscribe=None, ws_url=WS_URL, reconnect_delay=RECONNECT_DELAY):
    users = users or USERS
    on_fill = on_fill or (lambda u, f, s: print(u, f.get("coin"), f.get("px")))

    def on_open(ws):
        for user in users:
            ws.send(subscribe_msg(user))
        print(f"已连接，订阅 {len(users)} 个地址 userFills ...")

    def on_message(ws, raw):
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return
        channel, data = msg.get("channel"), msg.get("data")
        if channel == "subscriptionResponse":
            if on_subscribe:
                on_subscribe(data)
            return
        if channel != "userFills" and not (isinstance(data, dict) and "fills" in data):
            return
        user = ((data or {}).get("user") or "?").lower()
        snap = bool((data or {}).get("isSnapshot", False))
        for fill in (data or {}).get("fills") or []:
            on_fill(user, fill, snap)

    while True:
        try:
            ws = websocket.WebSocketApp(
                ws_url, on_open=on_open, on_message=on_message,
                on_error=lambda ws, e: print(f"WebSocket 错误: {e}"),
                on_close=lambda ws, c, r: print(f"连接关闭: {c} {r}"),
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print(f"连接异常: {e}")
        time.sleep(reconnect_delay)


if __name__ == "__main__":
    run()
