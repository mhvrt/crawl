from __future__ import annotations

from pathlib import Path

import httpx


async def telegram_send_message(token: str, chat_id: str, text: str) -> bool:
    if not token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    async with httpx.AsyncClient() as client:
        r = await client.post(url, data={"chat_id": chat_id, "text": text}, timeout=30)
        return r.is_success


async def telegram_send_document(token: str, chat_id: str, path: Path, caption: str = "") -> bool:
    if not token or not chat_id or not path.exists():
        return False
    url = f"https://api.telegram.org/bot{token}/sendDocument"
    async with httpx.AsyncClient() as client:
        with path.open("rb") as f:
            files = {"document": (path.name, f, "application/zip")}
            data = {"chat_id": chat_id, "caption": caption[:1024]}
            r = await client.post(url, data=data, files=files, timeout=120)
        return r.is_success
