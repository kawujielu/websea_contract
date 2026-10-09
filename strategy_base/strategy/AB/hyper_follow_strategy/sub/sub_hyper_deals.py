#!/usr/bin/env python3
"""订阅 HL trades。用法: python sub/sub_hyper_deals.py"""

import json
import time

import websocket

WS_URL = "wss://api.hyperliquid.xyz/ws"
COINS = ["BTC"]
RECONNECT_DELAY = 5


def subscribe_msg(coin):
    return json.dumps({"method": "subscribe", "subscription": {"type": "trades", "coin": coin}})


def run(coins, on_trade=None, on_subscribe=None, ws_url=WS_URL, reconnect_delay=RECONNECT_DELAY):
    def on_open(ws):
        try:
            for coin in coins:
                ws.send(subscribe_msg(coin))
            print(f"已连接，订阅 {coins} trades ...")
        except Exception as e:
            print(f"订阅发送失败: {e}")

    def on_message(ws, raw):
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError as e:
            print(f"JSON 解析失败: {e}")
            return
        channel = msg.get("channel")
        if channel == "subscriptionResponse":
            if on_subscribe:
                on_subscribe(msg.get("data"))
            return
        if channel == "error":
            print(f"服务端错误: {msg.get('data')}")
            return
        if channel != "trades":
            return
        for trade in msg.get("data") or []:
            if on_trade:
                on_trade(trade)

    def on_error(ws, error):
        print(f"WebSocket 错误: {error}")

    def on_close(ws, code, reason):
        print(f"连接关闭: code={code}, reason={reason}")

    while True:
        try:
            ws = websocket.WebSocketApp(
                ws_url, on_open=on_open, on_message=on_message,
                on_error=on_error, on_close=on_close,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print(f"连接异常: {e}")
        print(f"{reconnect_delay}s 后重连 ...")
        time.sleep(reconnect_delay)


if __name__ == "__main__":
    run(COINS, on_trade=lambda t: print(t.get("coin"), t.get("time")))
