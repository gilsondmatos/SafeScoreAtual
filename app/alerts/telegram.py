from __future__ import annotations
import os
import json
import requests  # type: ignore

class TelegramAlerter:
    def __init__(self, bot_token: str | None, chat_id: str | None):
        self.bot_token = bot_token
        self.chat_id = chat_id

    @classmethod
    def from_env(cls) -> "TelegramAlerter":
        return cls(
            bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
            chat_id=os.getenv("TELEGRAM_CHAT_ID"),
        )

    def send(self, message: str) -> bool:
        if not self.bot_token or not self.chat_id:
            # Alerta desativado por falta de credenciais.
            return False
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        try:
            resp = requests.post(url, json={"chat_id": self.chat_id, "text": message})
            return bool(resp.ok)
        except Exception:
            # Falla silenciosa (melhor esforço)
            return False
