import os
from typing import List


def _get_env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


class Settings:
    # Security
    PIN_CODE: str = _get_env("PIN_CODE", "1234")  # change in production
    SECRET_KEY: str = _get_env("SECRET_KEY", "change-this-secret-key")

    # Session
    TOKEN_TTL_SECONDS: int = int(_get_env("TOKEN_TTL_SECONDS", "10800"))  # 3 hours
    COOKIE_NAME: str = _get_env("COOKIE_NAME", "mbs_session")

    # CORS (important for dev: frontend 5173 talking to backend 8000 with cookies)
    # Example: "http://localhost:5173,http://127.0.0.1:5173"
    CORS_ORIGINS_RAW: str = _get_env("CORS_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173")

    # Cookie security flags
    # In production HTTPS set SECURE_COOKIES=1
    SECURE_COOKIES: bool = _get_env("SECURE_COOKIES", "0") in ("1", "true", "True", "yes", "YES")

    @property
    def CORS_ORIGINS(self) -> List[str]:
        parts = [p.strip() for p in self.CORS_ORIGINS_RAW.split(",") if p.strip()]
        return parts or ["*"]


settings = Settings()