"""只读查询 MT 账户资金与成交，并监听实时成交推送。运行: py xau_account_monitor.py
依赖: pip install metaapi-cloud-sdk requests

成交推送依据 MetaAPI Streaming/WebSocket API：
https://metaapi.cloud/docs/client/websocket/synchronizing/deals/
平仓后附加：账户总收益率、最大回撤（相对历史最高净值）、当日盈亏总额。
支持多账户并行监控。
"""
import asyncio
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests
from metaapi_cloud_sdk import MetaApi, SynchronizationListener
from metaapi_cloud_sdk.logger import NativeLogger

# 屏蔽 MetaAPI NativeLogger 的 WARN（如 on_deals_synchronized 耗时提示）
NativeLogger.warning = lambda self, msg, *args, **kwargs: None

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ========== 配置 ==========
ACCOUNTS = [
    {
        "login": "10041327",
        "account_id": "eca68df4-1ad4-40b5-b705-9808dad93fde",
        "token": "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI0MDI2MzRhMzczY2IwMTk3OGM3NjBhMDU3MjI0MDRkZCIsImFjY2Vzc1J1bGVzIjpbeyJpZCI6InRyYWRpbmctYWNjb3VudC1tYW5hZ2VtZW50LWFwaSIsIm1ldGhvZHMiOlsidHJhZGluZy1hY2NvdW50LW1hbmFnZW1lbnQtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiXSwicmVzb3VyY2VzIjpbImFjY291bnQ6JFVTRVJfSUQkOmVjYTY4ZGY0LTFhZDQtNDBiNS1iNzA1LTk4MDhkYWQ5M2ZkZSJdfSx7ImlkIjoibWV0YWFwaS1yZXN0LWFwaSIsIm1ldGhvZHMiOlsibWV0YWFwaS1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciIsIndyaXRlciJdLCJyZXNvdXJjZXMiOlsiYWNjb3VudDokVVNFUl9JRCQ6ZWNhNjhkZjQtMWFkNC00MGI1LWI3MDUtOTgwOGRhZDkzZmRlIl19LHsiaWQiOiJtZXRhYXBpLXJwYy1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOndzOnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyJhY2NvdW50OiRVU0VSX0lEJDplY2E2OGRmNC0xYWQ0LTQwYjUtYjcwNS05ODA4ZGFkOTNmZGUiXX0seyJpZCI6Im1ldGFhcGktcmVhbC10aW1lLXN0cmVhbWluZy1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOndzOnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyJhY2NvdW50OiRVU0VSX0lEJDplY2E2OGRmNC0xYWQ0LTQwYjUtYjcwNS05ODA4ZGFkOTNmZGUiXX0seyJpZCI6Im1ldGFzdGF0cy1hcGkiLCJtZXRob2RzIjpbIm1ldGFzdGF0cy1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciJdLCJyZXNvdXJjZXMiOlsiYWNjb3VudDokVVNFUl9JRCQ6ZWNhNjhkZjQtMWFkNC00MGI1LWI3MDUtOTgwOGRhZDkzZmRlIl19LHsiaWQiOiJyaXNrLW1hbmFnZW1lbnQtYXBpIiwibWV0aG9kcyI6WyJyaXNrLW1hbmFnZW1lbnQtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiXSwicmVzb3VyY2VzIjpbImFjY291bnQ6JFVTRVJfSUQkOmVjYTY4ZGY0LTFhZDQtNDBiNS1iNzA1LTk4MDhkYWQ5M2ZkZSJdfV0sImlnbm9yZVJhdGVMaW1pdHMiOmZhbHNlLCJ0b2tlbklkIjoiMjAyMTAyMTMiLCJpbXBlcnNvbmF0ZWQiOmZhbHNlLCJyZWFsVXNlcklkIjoiNDAyNjM0YTM3M2NiMDE5NzhjNzYwYTA1NzIyNDA0ZGQiLCJpYXQiOjE3ODgyODAwNjgsImV4cCI6MTc5NjA1NjA2OH0.j3OxtUTTusS8P5Sap9LfFMf_rnXIlR4V7MMfo-Glt8_R62oDJHwGmf1M6JcLGqh8O6y3AkQxszPykbX2XkEL6jrGDrBkPnP6Go2wBmXdHJTxaeqF1GWF2NE1LSaAePLsGtwFi29yw3c7whP3sebF35WaW-bR5EQsKV98o2XRt1o4-ECos2VonOeuWcmU-U091pft70ax02sJOfJAMj6gKcx-6qSyufr3xLgnck8hjnH25N2Wi9UkD_6nWQT1l5WZjskiUdRxSOImsrb2TYeG1YCUzEaBIJL_RYTibGDfWBU8icTd25oy2vkuSIr42Zby0B547iqaSqNlsovd9qb47-6ZDdIIgnkprg9PEXWZ2HilzgRzntJf0KlwoeUFYsjD9kEHwUeFPhAmIJwZTo3HinJnKYz0rHYn7bH1CIhIpKO_xqrIzoWaRSs6hk7oEt__amUUgaA-QjY58atwGDWDfcAKCGTZKOqDsfzt1jxs27W4-6KbRIlDhi8Q3e1jpmS_x0kyDt4PDuAxZ3HBy-aJdIoEQ7IpFXcUmgsp3CHOiXTlwCy3lTgTDI5vCKZMIoeSYyyoMFynUWdaEF8KNb-XqHNxHb28OrfBdW9QZG0pQDlJvGK0fUd5vsol7QeLE3uWqOFEYFkcEFM1kMAXhl8xHwUuKvptZ4YuM8e_Vbh_SOw",
        "peak_equity_init": 20000.0,
    },
    {
        "login": "10041452",
        "account_id": "a14b8d72-41a9-4dab-b0ab-6888b5da1ff8",
        "token": "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI0MDI2MzRhMzczY2IwMTk3OGM3NjBhMDU3MjI0MDRkZCIsImFjY2Vzc1J1bGVzIjpbeyJpZCI6InRyYWRpbmctYWNjb3VudC1tYW5hZ2VtZW50LWFwaSIsIm1ldGhvZHMiOlsidHJhZGluZy1hY2NvdW50LW1hbmFnZW1lbnQtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiXSwicmVzb3VyY2VzIjpbImFjY291bnQ6JFVTRVJfSUQkOmExNGI4ZDcyLTQxYTktNGRhYi1iMGFiLTY4ODhiNWRhMWZmOCJdfSx7ImlkIjoibWV0YWFwaS1yZXN0LWFwaSIsIm1ldGhvZHMiOlsibWV0YWFwaS1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciIsIndyaXRlciJdLCJyZXNvdXJjZXMiOlsiYWNjb3VudDokVVNFUl9JRCQ6YTE0YjhkNzItNDFhOS00ZGFiLWIwYWItNjg4OGI1ZGExZmY4Il19LHsiaWQiOiJtZXRhYXBpLXJwYy1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOndzOnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyJhY2NvdW50OiRVU0VSX0lEJDphMTRiOGQ3Mi00MWE5LTRkYWItYjBhYi02ODg4YjVkYTFmZjgiXX0seyJpZCI6Im1ldGFhcGktcmVhbC10aW1lLXN0cmVhbWluZy1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOndzOnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyJhY2NvdW50OiRVU0VSX0lEJDphMTRiOGQ3Mi00MWE5LTRkYWItYjBhYi02ODg4YjVkYTFmZjgiXX0seyJpZCI6Im1ldGFzdGF0cy1hcGkiLCJtZXRob2RzIjpbIm1ldGFzdGF0cy1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciJdLCJyZXNvdXJjZXMiOlsiYWNjb3VudDokVVNFUl9JRCQ6YTE0YjhkNzItNDFhOS00ZGFiLWIwYWItNjg4OGI1ZGExZmY4Il19LHsiaWQiOiJyaXNrLW1hbmFnZW1lbnQtYXBpIiwibWV0aG9kcyI6WyJyaXNrLW1hbmFnZW1lbnQtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiXSwicmVzb3VyY2VzIjpbImFjY291bnQ6JFVTRVJfSUQkOmExNGI4ZDcyLTQxYTktNGRhYi1iMGFiLTY4ODhiNWRhMWZmOCJdfV0sImlnbm9yZVJhdGVMaW1pdHMiOmZhbHNlLCJ0b2tlbklkIjoiMjAyMTAyMTMiLCJpbXBlcnNvbmF0ZWQiOmZhbHNlLCJyZWFsVXNlcklkIjoiNDAyNjM0YTM3M2NiMDE5NzhjNzYwYTA1NzIyNDA0ZGQiLCJpYXQiOjE3ODgzNTEwNTgsImV4cCI6MTc5NjEyNzA1OH0.YllrAmGFyNyRJ2OAdd2kAIxSlI06vCEDipTVW_jboFrYSZeoqkMaz2shrMg_dKgddOXWspeUW22lQfzxEAccYsRZSy0vu6VQD__3U9_PZmIW_2LDTgcHyRb5xlN91Kv8dfUrsl-L6oAvoy2DYHoYbvRHyOSVgFiFzp5xUqD2Wk_SVsb_o71KzzMyHYvLbSTgmMk8ZfrzzeZ48AxV6Xh24Kk_-ffj4mMBviKQ32o3St4O6nskUr3UMKRO-7vbJ6C-OQ1b99hhPCRqtUF4bT4pWe5abiQJrb1x9rMURZiptlN5QnxWttCxdyB-wWCS0sjxXBV5g_wZ1XYAiUj5KlXe6rI-ywxn1ktpJS6peLXl3ng4vmOEX9kzYGElLDvLAJINERhMbXHtjSBjLY7ULFq0LIzgOWcvdYIvboT9LlooVjJbVPrZGlIng6Q840tYqFP7t8hKRHhGOLGxwE5tNl5CE2Vh60h8MC7ofZLSn3Z_l5nZ9c_Q65fKbJudmDTglcYnY048tLrxGESzIvZIKbuOq-JnbS239BsQEG0BG_sRgsIgurjrsxG4hvsC93C3A1rJHDZ5M3MYToSCpbX7UsGkQYAmWUQ7O932EsbWb9xnHIYJxY775WOS8JTIDW6EZnAVKI04CfjulQwv-Vc4NAU-8EM729KAxixcKQjUB3j6HqI",
        "peak_equity_init": 20000.0,
    },
    {
        "login": "10041453",
        "account_id": "284b4334-33ba-4c1b-ab2e-189326245440",
        "token": "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI0MDI2MzRhMzczY2IwMTk3OGM3NjBhMDU3MjI0MDRkZCIsImFjY2Vzc1J1bGVzIjpbeyJpZCI6InRyYWRpbmctYWNjb3VudC1tYW5hZ2VtZW50LWFwaSIsIm1ldGhvZHMiOlsidHJhZGluZy1hY2NvdW50LW1hbmFnZW1lbnQtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiXSwicmVzb3VyY2VzIjpbImFjY291bnQ6JFVTRVJfSUQkOjI4NGI0MzM0LTMzYmEtNGMxYi1hYjJlLTE4OTMyNjI0NTQ0MCJdfSx7ImlkIjoibWV0YWFwaS1yZXN0LWFwaSIsIm1ldGhvZHMiOlsibWV0YWFwaS1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciIsIndyaXRlciJdLCJyZXNvdXJjZXMiOlsiYWNjb3VudDokVVNFUl9JRCQ6Mjg0YjQzMzQtMzNiYS00YzFiLWFiMmUtMTg5MzI2MjQ1NDQwIl19LHsiaWQiOiJtZXRhYXBpLXJwYy1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOndzOnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyJhY2NvdW50OiRVU0VSX0lEJDoyODRiNDMzNC0zM2JhLTRjMWItYWIyZS0xODkzMjYyNDU0NDAiXX0seyJpZCI6Im1ldGFhcGktcmVhbC10aW1lLXN0cmVhbWluZy1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOndzOnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyJhY2NvdW50OiRVU0VSX0lEJDoyODRiNDMzNC0zM2JhLTRjMWItYWIyZS0xODkzMjYyNDU0NDAiXX0seyJpZCI6Im1ldGFzdGF0cy1hcGkiLCJtZXRob2RzIjpbIm1ldGFzdGF0cy1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciJdLCJyZXNvdXJjZXMiOlsiYWNjb3VudDokVVNFUl9JRCQ6Mjg0YjQzMzQtMzNiYS00YzFiLWFiMmUtMTg5MzI2MjQ1NDQwIl19LHsiaWQiOiJyaXNrLW1hbmFnZW1lbnQtYXBpIiwibWV0aG9kcyI6WyJyaXNrLW1hbmFnZW1lbnQtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiXSwicmVzb3VyY2VzIjpbImFjY291bnQ6JFVTRVJfSUQkOjI4NGI0MzM0LTMzYmEtNGMxYi1hYjJlLTE4OTMyNjI0NTQ0MCJdfV0sImlnbm9yZVJhdGVMaW1pdHMiOmZhbHNlLCJ0b2tlbklkIjoiMjAyMTAyMTMiLCJpbXBlcnNvbmF0ZWQiOmZhbHNlLCJyZWFsVXNlcklkIjoiNDAyNjM0YTM3M2NiMDE5NzhjNzYwYTA1NzIyNDA0ZGQiLCJpYXQiOjE3ODgzNTEzMzcsImV4cCI6MTc5NjEyNzMzN30.ZRbjpgx0az1vJW8LcumzsXai_Kr5zyi6-PPqc2Eoh8b5rj9NNAIPklPeChWCH4oE3RE1xaOv82aI2T6gyvqsBuUgYpkHK8hgNLo73XTEyBycNO1uw1-m04JmC-nbRU_IaWNl1A2bBn6oR-V0YQPybkIa3FFkfiyMmPGWCnImK2fvPtqVn0hxPeLNMY_9VfExYYAleW1iivtkNitRRQ7GK9RG2D6i18i2MZv0X4a4IXUEE5vTOF70R-b4I7hD5uSNGGRPLh1vZE8BG17UjYPhyUwo5HLRKC5TkxST74CzO1SSlUS_iyvNLcC0iTjq8Kuvemlofv_TRvrwxZrCHJqcsR5O8I-Vc8NU-yQkGvXbLLiXUywOUVNhmNKMmIv3CBt03MWFYF5z_m9n1kY_YmZF0kF6DPXS9d4nwl9uVezhy05z3Ce2FmOYXQwd0Fid2bCvu-tOM2ViBwfOiA_1y1oGngZeWBWZ0cW7DJvdwTK7rjLRHF2LQcpjG04B0L8noiz3F0Q2sGs0gkH2LLw5dxZ5h7gI1-c5CSwezorZq3QinhtSoPJwjKHC816145Wza8E05wmJ2LXpgFIYIkkqsGsUSrJvTQt6CkrBWyAf-0zb1JlsF9tfhexgo7AF4epFxCjEg9I6alMZIHfI5mxVQndMWJIENjOOFdKBieBDBtTsb-4",
        "peak_equity_init": 20000.0,
    },
    {
        "login": "10041454",
        "account_id": "2104c937-1cb6-4412-8333-885511826bbf",
        "token": "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI0MDI2MzRhMzczY2IwMTk3OGM3NjBhMDU3MjI0MDRkZCIsImFjY2Vzc1J1bGVzIjpbeyJpZCI6InRyYWRpbmctYWNjb3VudC1tYW5hZ2VtZW50LWFwaSIsIm1ldGhvZHMiOlsidHJhZGluZy1hY2NvdW50LW1hbmFnZW1lbnQtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiXSwicmVzb3VyY2VzIjpbImFjY291bnQ6JFVTRVJfSUQkOjIxMDRjOTM3LTFjYjYtNDQxMi04MzMzLTg4NTUxMTgyNmJiZiJdfSx7ImlkIjoibWV0YWFwaS1yZXN0LWFwaSIsIm1ldGhvZHMiOlsibWV0YWFwaS1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciIsIndyaXRlciJdLCJyZXNvdXJjZXMiOlsiYWNjb3VudDokVVNFUl9JRCQ6MjEwNGM5MzctMWNiNi00NDEyLTgzMzMtODg1NTExODI2YmJmIl19LHsiaWQiOiJtZXRhYXBpLXJwYy1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOndzOnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyJhY2NvdW50OiRVU0VSX0lEJDoyMTA0YzkzNy0xY2I2LTQ0MTItODMzMy04ODU1MTE4MjZiYmYiXX0seyJpZCI6Im1ldGFhcGktcmVhbC10aW1lLXN0cmVhbWluZy1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOndzOnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyJhY2NvdW50OiRVU0VSX0lEJDoyMTA0YzkzNy0xY2I2LTQ0MTItODMzMy04ODU1MTE4MjZiYmYiXX0seyJpZCI6Im1ldGFzdGF0cy1hcGkiLCJtZXRob2RzIjpbIm1ldGFzdGF0cy1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciJdLCJyZXNvdXJjZXMiOlsiYWNjb3VudDokVVNFUl9JRCQ6MjEwNGM5MzctMWNiNi00NDEyLTgzMzMtODg1NTExODI2YmJmIl19LHsiaWQiOiJyaXNrLW1hbmFnZW1lbnQtYXBpIiwibWV0aG9kcyI6WyJyaXNrLW1hbmFnZW1lbnQtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiXSwicmVzb3VyY2VzIjpbImFjY291bnQ6JFVTRVJfSUQkOjIxMDRjOTM3LTFjYjYtNDQxMi04MzMzLTg4NTUxMTgyNmJiZiJdfV0sImlnbm9yZVJhdGVMaW1pdHMiOmZhbHNlLCJ0b2tlbklkIjoiMjAyMTAyMTMiLCJpbXBlcnNvbmF0ZWQiOmZhbHNlLCJyZWFsVXNlcklkIjoiNDAyNjM0YTM3M2NiMDE5NzhjNzYwYTA1NzIyNDA0ZGQiLCJpYXQiOjE3ODgzNTE0MzIsImV4cCI6MTc5NjEyNzQzMn0.i_70AILHRGac9qWW4t25Si0KcsQAv2_Ck9Of0y_rzAYwOpxb0GyImXDNp_FiZeeY9oK8miw2y3smWOWaFN_EX5T4cZKMIINM2cvqv4NPxQ8qJMZRzd3XamQpRM9OUyRX_xRx8uNx0z3jjTjO8ixSmaklXnJ0muCRdhYXssJIX7DWoln1RrkCmRI9nLTMn3sXCHkBsc6IHneP8HYlKrDdwvdHTGh8XvL29tR30hVRmbfmzoFMd4o4db1zzmLt2_mnrxZBTkVI4zsaREevtKqTsw0g2ZJ51cBF9fSZucRysVcB1tvu3BtMAfn7cNhY56TMhb4RFhOUGobl2b5z1mwqoaOnMWT9HQ50sjU4IQoYOAXqOSIQvmdAs65dcvw44koyTpWXCR4i2Ei1JeMhMI4X7KlMbJzdE6gqldrYj0hl91dK4od8W2UHREZ6ZX1pW7KYBb1RbGKrAaFfF3dpUDbwbrWXKgA-EEUj5Wlk6blMNseXQz0N2kdAAFase40uQYZ6MAldc6Hqbzr2XTqa2QTUp_iyzgf3fFCKscZt4n2X3YNAdZnmnbwfM1D7KBTo1GQMapeXq60tTc7ofg0XXOcbU9i_QbSL_rcxGftp9gS1LFcm8cOi3bR_vevCKbg01-89k5uxToXAT3cSg4jeQWH7-KBbzKf6gW3aH0N25lWhHOk",
        "peak_equity_init": 20000.0,
    },
    {
        "login": "10041457",
        "account_id": "d122b133-fb97-4287-a55a-aaa8885bc1c9",
        "token": "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI0MDI2MzRhMzczY2IwMTk3OGM3NjBhMDU3MjI0MDRkZCIsImFjY2Vzc1J1bGVzIjpbeyJpZCI6InRyYWRpbmctYWNjb3VudC1tYW5hZ2VtZW50LWFwaSIsIm1ldGhvZHMiOlsidHJhZGluZy1hY2NvdW50LW1hbmFnZW1lbnQtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiXSwicmVzb3VyY2VzIjpbImFjY291bnQ6JFVTRVJfSUQkOmQxMjJiMTMzLWZiOTctNDI4Ny1hNTVhLWFhYTg4ODViYzFjOSJdfSx7ImlkIjoibWV0YWFwaS1yZXN0LWFwaSIsIm1ldGhvZHMiOlsibWV0YWFwaS1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciIsIndyaXRlciJdLCJyZXNvdXJjZXMiOlsiYWNjb3VudDokVVNFUl9JRCQ6ZDEyMmIxMzMtZmI5Ny00Mjg3LWE1NWEtYWFhODg4NWJjMWM5Il19LHsiaWQiOiJtZXRhYXBpLXJwYy1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOndzOnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyJhY2NvdW50OiRVU0VSX0lEJDpkMTIyYjEzMy1mYjk3LTQyODctYTU1YS1hYWE4ODg1YmMxYzkiXX0seyJpZCI6Im1ldGFhcGktcmVhbC10aW1lLXN0cmVhbWluZy1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOndzOnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyJhY2NvdW50OiRVU0VSX0lEJDpkMTIyYjEzMy1mYjk3LTQyODctYTU1YS1hYWE4ODg1YmMxYzkiXX0seyJpZCI6Im1ldGFzdGF0cy1hcGkiLCJtZXRob2RzIjpbIm1ldGFzdGF0cy1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciJdLCJyZXNvdXJjZXMiOlsiYWNjb3VudDokVVNFUl9JRCQ6ZDEyMmIxMzMtZmI5Ny00Mjg3LWE1NWEtYWFhODg4NWJjMWM5Il19LHsiaWQiOiJyaXNrLW1hbmFnZW1lbnQtYXBpIiwibWV0aG9kcyI6WyJyaXNrLW1hbmFnZW1lbnQtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiXSwicmVzb3VyY2VzIjpbImFjY291bnQ6JFVTRVJfSUQkOmQxMjJiMTMzLWZiOTctNDI4Ny1hNTVhLWFhYTg4ODViYzFjOSJdfV0sImlnbm9yZVJhdGVMaW1pdHMiOmZhbHNlLCJ0b2tlbklkIjoiMjAyMTAyMTMiLCJpbXBlcnNvbmF0ZWQiOmZhbHNlLCJyZWFsVXNlcklkIjoiNDAyNjM0YTM3M2NiMDE5NzhjNzYwYTA1NzIyNDA0ZGQiLCJpYXQiOjE3ODgzNTE1MjQsImV4cCI6MTc5NjEyNzUyNH0.YKld1WnbIAxTiSfpasINagd2qyDJlkH2vHagPXwNcrmvfpg-Ko_mQCi1VJWgX2wbRG8GYr4a_ZwzrRyP31BCcBliZlmrQkr6pTMuUJvZzKtnMouFILf7rYoydJbMT64L9Cw4ZNU5JdZSd4QqGbIAjwgj1yPgAzEzghmdmvHMjLw4sj4CGkyXccynsEMUuXu6ArDukggbZTzrs2U8-nJ9ywdPyUCEgulQLrN3EHiNg5BgxXmeX7qB3G4YJbePveJ8DI7zRa0dO6TnZpbPihS4XomRNHFRlB9ov1hVJfLkTHAkPDPm3hlKmOyBaCnPSIy0rhgJ41zRTGkiDc-b3ZB3_9SrfBmpNu7CfpLsQi04HzsT2NvKjQZZKRXnddV1_lXf9gy_XPzcAz7ulhhTYdjjLy1xFydWV21vhG-nOpeMLWCAnbyhgKU8OVclCBPgJG7Afq9dXjZCh-SNHzkpEsi-GYYC74fcg4WSnDiOFZcmUjp_4LHRr3WhNH1Ip0V3Ui4lncZLzXtuG6ufJnlhEJq8WdC5IY-ROogtMYWnTu1SEisSr3o24ZWr3zlB0rxhZfJZ5n3vlMRVVil69iz4AxY1tFF6HZK2ID9fuMy7CSKjh7-Zkthq0WA0qEeWoafrg86h6mDUBFPG3zP5NtmjgCcog3g2C8cTWjV9wPtWEBz12vs",
        "peak_equity_init": 20000.0,
    },
]
DAYS = 7
LISTEN = True
DAILY_SUMMARY = True
# 启动时历史最高净值 = max(期初资金(=全部入金合计), peak_equity_init)
# Telegram（与 CFD_deal_monitor 同群）
TG_BOT_TOKEN = "6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q"
TG_CHAT_ID = "-5008089826"
TG_ENABLED = True
# ==========================

BJ = timezone(timedelta(hours=8))
_TG_MAX = 3500

_TYPE_CN = {
    "DEAL_TYPE_BUY": "买入", "DEAL_TYPE_SELL": "卖出",
    "DEAL_ENTRY_IN": "开仓", "DEAL_ENTRY_OUT": "平仓",
}


def send_tg(text: str) -> None:
    if not TG_ENABLED or not text:
        return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    parts = [text[i:i + _TG_MAX] for i in range(0, len(text), _TG_MAX)] or [text]
    for part in parts:
        try:
            r = requests.post(url, data={"chat_id": TG_CHAT_ID, "text": part}, timeout=30)
            r.raise_for_status()
        except Exception as e:
            print(f"TG发送失败: {e}")


def emit(text: str) -> None:
    print(text)
    send_tg(text)


def _as_dict(deal):
    if isinstance(deal, dict):
        return deal
    return {k: getattr(deal, k) for k in (
        "id", "type", "entryType", "symbol", "time", "brokerTime",
        "price", "volume", "profit", "commission", "swap", "comment",
    ) if hasattr(deal, k)}


def _fmt_price(v):
    if v is None or v == "-":
        return "-"
    try:
        return f"{float(v):.2f}"
    except (TypeError, ValueError):
        return v


def _deal_pnl(d: dict) -> float:
    return float(d.get("profit") or 0) + float(d.get("commission") or 0) + float(d.get("swap") or 0)


def _fmt_push(deal, login: str, metrics: Optional[str] = None) -> str:
    d = _as_dict(deal)
    t = d.get("brokerTime") or d.get("time") or "-"
    prefix = f"[实时成交][{login}]"
    if d.get("type") == "DEAL_TYPE_BALANCE":
        amt = d.get("profit") or 0
        kind = "入金" if amt > 0 else ("出金" if amt < 0 else "余额调整")
        return f"{prefix} {t}  {kind}  {amt}"
    direction = _TYPE_CN.get(d.get("entryType") or d.get("type"), d.get("type", "-"))
    text = (f"{prefix} {t}  {direction}  {d.get('symbol', '-')}  "
            f"价格={_fmt_price(d.get('price'))}  数量={d.get('volume', '-')}  "
            f"盈亏={d.get('profit', '-')}")
    if metrics:
        text = f"{text}\n{metrics}"
    return text


def _parse_deal_time(deal) -> Optional[datetime]:
    d = _as_dict(deal)
    raw = d.get("time") or d.get("brokerTime")
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    s = str(raw).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(s[:26], fmt)
                break
            except ValueError:
                dt = None
        if dt is None:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _is_fresh_deal(deal, within_sec: int = 60) -> bool:
    """成交时间早于当前超过 within_sec 则视为历史回放，不推送。"""
    dt = _parse_deal_time(deal)
    if dt is None:
        return False
    return dt >= datetime.now(timezone.utc) - timedelta(seconds=within_sec)


def _is_close_deal(deal) -> bool:
    return _as_dict(deal).get("entryType") == "DEAL_ENTRY_OUT"


class DealPushListener(SynchronizationListener):
    def __init__(self, stream, account, login: str, peak_equity_init: float):
        super().__init__()
        self.ready = False
        self._init_started = False
        self.stream = stream
        self.account = account
        self.login = str(login)
        self.peak_equity_init = float(peak_equity_init)
        self.peak_equity = 0.0
        self.max_dd = 0.0            # 成交路径上出现过的最大回撤%
        self.basis = 0.0
        self.day_key = ""
        self.daily_pnl = 0.0

    def _equity(self) -> float:
        info = getattr(self.stream.terminal_state, "account_information", None) or {}
        if isinstance(info, dict):
            return float(info.get("equity") or 0)
        return float(getattr(info, "equity", 0) or 0)

    def _currency(self) -> str:
        info = getattr(self.stream.terminal_state, "account_information", None) or {}
        if isinstance(info, dict):
            return str(info.get("currency") or "")
        return str(getattr(info, "currency", "") or "")

    async def _init_stats(self) -> None:
        equity = self._equity()
        try:
            rpc = self.account.get_rpc_connection()
            await rpc.connect()
            await rpc.wait_synchronized()
            try:
                end = datetime.now(timezone.utc)
                start = end - timedelta(days=3650)
                result = await rpc.get_deals_by_time_range(start_time=start, end_time=end)
                deals = result.get("deals", result) if isinstance(result, dict) else result

                deposit_sum = sum(
                    float(d.get("profit") or 0)
                    for d in deals
                    if d.get("type") == "DEAL_TYPE_BALANCE" and float(d.get("profit") or 0) > 0
                )
                self.basis = deposit_sum if abs(deposit_sum) > 1e-8 else equity
                dd_info = _max_dd_shield_cashflow(deals, equity, self.peak_equity_init)
                self.peak_equity = float(dd_info["peak"])
                self.max_dd = float(dd_info["max_dd"])
                self.dd_info = dd_info

                today = datetime.now(BJ).date()
                self.day_key = today.isoformat()
                self.daily_pnl = 0.0
                for d in deals:
                    if d.get("type") == "DEAL_TYPE_BALANCE":
                        continue
                    dt = _parse_deal_time(d)
                    if dt is not None and dt.astimezone(BJ).date() == today:
                        self.daily_pnl += _deal_pnl(d)
            finally:
                await rpc.close()
        except Exception as e:
            print(f"[{self.login}] 初始化收益率/当日盈亏失败，改用当前权益: {e}")
            self.basis = equity or 1.0
            self.peak_equity = max(self.basis, self.peak_equity_init)
            self.max_dd = 0.0
            self.day_key = datetime.now(BJ).date().isoformat()
            self.daily_pnl = 0.0
        print(
            f"[{self.login}] 净值监控就绪 peak={self.peak_equity:.2f} max_dd={self.max_dd:.4f}% "
            f"期初={self.basis:.2f} 当日盈亏={self.daily_pnl:.2f}"
        )

    def _metrics_after_close(self, deal) -> Optional[str]:
        """平仓后更新状态；仅当最大回撤创新高时返回推送文案，否则返回 None。"""
        today = datetime.now(BJ).date().isoformat()
        if today != self.day_key:
            self.day_key = today
            self.daily_pnl = 0.0
        self.daily_pnl += _deal_pnl(_as_dict(deal))

        equity = self._equity()
        if equity > self.peak_equity:
            self.peak_equity = equity
            cur_dd = 0.0
        else:
            cur_dd = (
                (self.peak_equity - equity) / self.peak_equity * 100
                if self.peak_equity else 0.0
            )
        dd_broke = cur_dd > self.max_dd + 1e-12
        self.max_dd = max(self.max_dd, cur_dd)
        if not dd_broke:
            return None

        cur = self._currency()
        total_ret = (
            (equity - self.basis) / self.basis * 100
            if abs(self.basis) > 1e-8 else 0.0
        )
        return (
            f"账户总收益率: {total_ret:.4f}%  （净值={equity:.2f} / 期初资金={self.basis:.2f} {cur}）\n"
            f"最大回撤: {self.max_dd:.4f}%  （历史最高净值={self.peak_equity:.2f} {cur}；成交路径盘中最大）\n"
            f"当日盈亏总额: {self.daily_pnl:.2f} {cur}"
        )

    async def on_deals_synchronized(self, instance_index: str, synchronization_id: str):
        # 多实例会重复回调；耗时初始化放到后台，避免 SDK WARN
        if self._init_started:
            return
        self._init_started = True

        async def _bg_init():
            await self._init_stats()
            self.ready = True
            print(f"[{self.login}] 成交历史同步完成，开始接收实时推送…")

        asyncio.create_task(_bg_init())

    async def on_deal_added(self, instance_index: str, deal):
        # 仅平仓且最大回撤创新高时推送；普通成交/开仓/入金等一律不推
        if not self.ready:
            return
        if not _is_close_deal(deal):
            return
        if not _is_fresh_deal(deal, within_sec=60):
            return
        await asyncio.sleep(0.3)  # 等待净值刷新
        try:
            metrics = self._metrics_after_close(deal)
        except Exception as e:
            print(f"[{self.login}] 平仓指标计算失败: {e}")
            return
        if metrics:
            emit(_fmt_push(deal, self.login, metrics))


def _fmt_cn(info, deals, login: str) -> str:
    cur = info.get("currency", "")
    trades = [d for d in deals if d.get("type") != "DEAL_TYPE_BALANCE"]
    balances = [d for d in deals if d.get("type") == "DEAL_TYPE_BALANCE"]
    lines = [
        f"=== 账户资金 [{login}] ===",
        f"整体权益: {info.get('equity')} {cur}",
        f"可用: {info.get('freeMargin')} {cur}",
        f"杠杆倍数: 1:{info.get('leverage')}",
        f"资产名称: {cur}",
        "",
        f"=== 余额变动（共 {len(balances)} 笔）===",
    ]
    if not balances:
        lines.append("（无）")
    for d in balances:
        amt = d.get("profit") or 0
        kind = "入金" if amt > 0 else ("出金" if amt < 0 else "余额调整")
        t = d.get("brokerTime") or d.get("time")
        lines.append(f"{t}  {kind}  {amt} {cur}")

    lines += [
        "",
        f"=== 成交明细（近 {DAYS} 天，共 {len(trades)} 笔）===",
        f"{'时间':<22} {'方向':<6} {'标的':<12} {'价格':<10} {'数量':<8} {'盈亏':<8}",
    ]
    for d in trades:
        t = d.get("brokerTime") or d.get("time") or "-"
        direction = _TYPE_CN.get(d.get("entryType") or d.get("type"), d.get("type", "-"))
        lines.append(
            f"{str(t)[:22]:<22} {direction:<6} {d.get('symbol', '-'):<12} "
            f"{_fmt_price(d.get('price')):<10} {str(d.get('volume', '-')):<8} {d.get('profit', '-')}"
        )
    return "\n".join(lines)


def _bj_yesterday_range(now_bj=None):
    now_bj = now_bj or datetime.now(BJ)
    today0 = now_bj.replace(hour=0, minute=0, second=0, microsecond=0)
    return today0 - timedelta(days=1), today0


def _seconds_to_next_bj_midnight():
    now = datetime.now(BJ)
    nxt = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return max(1.0, (nxt - now).total_seconds())


def _deal_time_str(d: dict) -> str:
    return str(d.get("brokerTime") or d.get("time") or "-")


def _max_dd_shield_cashflow(all_deals, current_equity: float, peak_equity_init: float) -> dict:
    """按每笔成交重建净值路径，统计盘中最大回撤及对应区间交易。
    返回 dict: peak, max_dd, loss, peak_time, trough_time, peak_nav, trough_nav, trades
    """
    deals = sorted(
        all_deals or [],
        key=lambda d: str(d.get("time") or d.get("brokerTime") or ""),
    )
    nav = 0.0
    peak = 0.0
    max_dd = 0.0
    peak_idx = -1
    cur_peak_idx = -1
    trough_idx = -1
    peak_nav_at_dd = 0.0
    trough_nav_at_dd = 0.0

    for i, d in enumerate(deals):
        if d.get("type") == "DEAL_TYPE_BALANCE":
            p = float(d.get("profit") or 0)
            nav += p
            peak += p
            if peak < nav:
                peak = nav
            cur_peak_idx = i
        else:
            nav += _deal_pnl(d)
            if nav > peak:
                peak = nav
                cur_peak_idx = i
        if peak > 1e-8 and nav < peak:
            dd = (peak - nav) / peak * 100
            if dd > max_dd + 1e-12:
                max_dd = dd
                peak_idx = cur_peak_idx
                trough_idx = i
                peak_nav_at_dd = peak
                trough_nav_at_dd = nav

    eq = float(current_equity or 0)
    peak = max(peak, float(peak_equity_init))
    if eq > peak:
        peak = eq
    elif peak > 1e-8 and eq < peak:
        dd = (peak - eq) / peak * 100
        if dd > max_dd + 1e-12:
            max_dd = dd
            peak_nav_at_dd = peak
            trough_nav_at_dd = eq

    loss = max(0.0, peak_nav_at_dd - trough_nav_at_dd)
    trades = []
    peak_time = trough_time = "-"
    if 0 <= peak_idx <= trough_idx < len(deals):
        peak_time = _deal_time_str(deals[peak_idx])
        trough_time = _deal_time_str(deals[trough_idx])
        for d in deals[peak_idx + 1: trough_idx + 1]:
            if d.get("type") == "DEAL_TYPE_BALANCE":
                continue
            trades.append(d)

    return {
        "peak": peak,
        "max_dd": max_dd,
        "loss": loss,
        "peak_time": peak_time,
        "trough_time": trough_time,
        "peak_nav": peak_nav_at_dd,
        "trough_nav": trough_nav_at_dd,
        "trades": trades,
    }


def _fmt_dd_trades(trades, currency="") -> list:
    if not trades:
        return ["  （无对应交易明细）"]
    lines = []
    for d in trades:
        t = _deal_time_str(d)
        direction = _TYPE_CN.get(d.get("entryType") or d.get("type"), d.get("type", "-"))
        pnl = _deal_pnl(d)
        lines.append(
            "  {}  {}  {}  价格={}  数量={}  盈亏={:.2f} {}".format(
                str(t)[:22],
                direction,
                d.get("symbol", "-"),
                _fmt_price(d.get("price")),
                d.get("volume", "-"),
                pnl,
                currency,
            )
        )
    return lines


def _fmt_day_summary(title, deals, equity, deposit_sum, dd_info=None, currency="") -> str:
    """昨日汇总：总盈亏=当前权益-期初权益；昨日盈亏仅计交易；期初权益=历史入金合计。"""
    trades = [d for d in deals if d.get("type") != "DEAL_TYPE_BALANCE"]
    count = len(trades)
    amount = round(sum((d.get("volume") or 0) for d in trades), 4)
    trade_pnl = round(sum(_deal_pnl(d) for d in trades), 2)
    basis = float(deposit_sum or 0)
    eq = float(equity or 0)
    total_pnl = round(eq - basis, 2)
    ret = (trade_pnl / basis * 100) if abs(basis) > 1e-8 else 0.0
    total_ret = ((eq - basis) / basis * 100) if abs(basis) > 1e-8 else 0.0
    dd_info = dd_info or {}
    max_dd = float(dd_info.get("max_dd") or 0)
    peak_equity = float(dd_info.get("peak") or 0)
    loss = float(dd_info.get("loss") or 0)
    # dd_trades = dd_info.get("trades") or []
    lines = [
        title,
        f"期初权益: {round(basis, 2)} {currency}",
        f"当前权益: {round(eq, 2)} {currency}",
        f"总盈亏: {total_pnl} {currency}",
        f"总收益率: {total_ret:.4f}%",
        f"最大回撤: {max_dd:.4f}%",     #   （历史最高净值={round(peak_equity, 2)} {currency}）
        f"回撤亏损金额: {round(loss, 2)} {currency}  "
        f"（峰值净值={round(float(dd_info.get('peak_nav') or 0), 2)} → "
        f"谷值净值={round(float(dd_info.get('trough_nav') or 0), 2)}）",
        f"回撤发生时间: {dd_info.get('peak_time', '-')}  →  {dd_info.get('trough_time', '-')}",
        # f"回撤区间交易（共 {len(dd_trades)} 笔）:",
    ]
    # lines.extend(_fmt_dd_trades(dd_trades, currency))
    lines += [
        f"昨日成交笔数: {count}",
        f"金额(手数合计): {amount}",
        f"昨日盈亏: {trade_pnl} {currency}",
        f"昨日收益率: {ret:.4f}%",
    ]
    return "\n".join(lines)


async def _load_all_deals(rpc) -> list:
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=3650)
    result = await rpc.get_deals_by_time_range(start_time=start, end_time=end)
    return result.get("deals", result) if isinstance(result, dict) else (result or [])


def _sum_deposits(all_deals) -> float:
    return sum(
        float(d.get("profit") or 0)
        for d in all_deals
        if d.get("type") == "DEAL_TYPE_BALANCE" and float(d.get("profit") or 0) > 0
    )


async def fetch_and_emit_summary(account, login: str, peak_equity_init: float):
    start_bj, end_bj = _bj_yesterday_range()
    title = f"【昨日成交汇总】[{login}] 北京时间 {start_bj.strftime('%Y-%m-%d')}"
    rpc = account.get_rpc_connection()
    await rpc.connect()
    await rpc.wait_synchronized()
    try:
        info = await rpc.get_account_information()
        equity = info.get("equity")
        result = await rpc.get_deals_by_time_range(
            start_time=start_bj.astimezone(timezone.utc),
            end_time=end_bj.astimezone(timezone.utc),
        )
        deals = result.get("deals", result) if isinstance(result, dict) else result
        all_deals = await _load_all_deals(rpc)
        deposit_sum = _sum_deposits(all_deals)
        dd_info = _max_dd_shield_cashflow(all_deals, equity, peak_equity_init)
        emit(_fmt_day_summary(
            title, deals, equity, deposit_sum, dd_info, info.get("currency") or "",
        ))
    finally:
        await rpc.close()


async def daily_summary_loop(account, login: str, peak_equity_init: float):
    print(f"[{login}] 已启用每日汇总：下一个北京时间 0 点触发（约 {_seconds_to_next_bj_midnight()/3600:.1f} 小时后）")
    while True:
        await asyncio.sleep(_seconds_to_next_bj_midnight())
        try:
            await fetch_and_emit_summary(account, login, peak_equity_init)
        except Exception as e:
            print(f"[{login}] 昨日汇总失败: {e}")
        await asyncio.sleep(2)


async def run_account(cfg: dict):
    login = str(cfg["login"])
    account_id = cfg["account_id"]
    token = cfg["token"]
    peak_equity_init = float(cfg.get("peak_equity_init") or 20000.0)

    api = MetaApi(token)
    account = await api.metatrader_account_api.get_account(account_id)
    print(f"[{login}] MetaAPI 账户 ID: {account.id}  状态: {account.state}  login: {account.login}")

    if account.state != "DEPLOYED":
        print(f"[{login}] 部署中…")
        await account.deploy()
    await account.wait_connected()

    rpc = account.get_rpc_connection()
    await rpc.connect()
    await rpc.wait_synchronized()

    info = await rpc.get_account_information()
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=DAYS)
    result = await rpc.get_deals_by_time_range(start_time=start, end_time=end)
    deals = result.get("deals", result) if isinstance(result, dict) else result
    emit(_fmt_cn(info, deals, login))
    await rpc.close()

    try:
        await fetch_and_emit_summary(account, login, peak_equity_init)
    except Exception as e:
        print(f"[{login}] 启动日汇总失败: {e}")

    if not LISTEN and not DAILY_SUMMARY:
        return

    tasks = []
    stream = None
    listener = None

    if DAILY_SUMMARY:
        tasks.append(asyncio.create_task(
            daily_summary_loop(account, login, peak_equity_init),
            name=f"daily-{login}",
        ))

    if LISTEN:
        print(f"\n=== [{login}] 开启成交实时推送（Streaming API）===")
        stream = account.get_streaming_connection()
        listener = DealPushListener(stream, account, login, peak_equity_init)
        stream.add_synchronization_listener(listener)
        await stream.connect()
        await stream.wait_synchronized()
        tasks.append(asyncio.create_task(asyncio.sleep(10**9), name=f"listen-{login}"))

    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for t in tasks:
            t.cancel()
        if stream and listener:
            stream.remove_synchronization_listener(listener)
            await stream.close()


async def main():
    print(f"共监控 {len(ACCOUNTS)} 个账户: {[c['login'] for c in ACCOUNTS]}")
    results = await asyncio.gather(
        *(run_account(cfg) for cfg in ACCOUNTS),
        return_exceptions=True,
    )
    for cfg, result in zip(ACCOUNTS, results):
        if isinstance(result, Exception):
            print(f"[{cfg['login']}] 账户任务异常退出: {result}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n已停止监听")

