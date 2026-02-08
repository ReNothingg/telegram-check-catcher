import os
from typing import Union

from dotenv import load_dotenv

load_dotenv()


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Required env var is missing: {name}")
    return value


def _require_int_env(name: str, minimum: int = 1) -> int:
    raw_value = _require_env(name)
    try:
        parsed = int(raw_value)
    except ValueError as error:
        raise RuntimeError(f"Env var {name} must be integer, got: {raw_value}") from error
    if parsed < minimum:
        raise RuntimeError(f"Env var {name} must be >= {minimum}, got: {parsed}")
    return parsed


def _env_bool(name: str, default: bool) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default

    normalized = raw_value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False

    raise RuntimeError(f"Env var {name} must be boolean (true/false/1/0), got: {raw_value}")


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    raw_value = os.getenv(name)
    if raw_value is None or not raw_value.strip():
        parsed = default
    else:
        try:
            parsed = int(raw_value)
        except ValueError as error:
            raise RuntimeError(f"Env var {name} must be integer, got: {raw_value}") from error

    if parsed < minimum:
        raise RuntimeError(f"Env var {name} must be >= {minimum}, got: {parsed}")
    return parsed


def _require_chat_target(name: str) -> Union[int, str]:
    value = _require_env(name)
    if value.lstrip("-").isdigit():
        return int(value)
    return value


api_id = _require_int_env("API_ID", minimum=1)
api_hash = _require_env("API_HASH")
channel = _require_chat_target("CHANNEL")

avto_vivod = _env_bool("AVTO_VIVOD", default=False)
anti_captcha = _env_bool("ANTI_CAPTCHA", default=True)

max_retries = _env_int("MAX_RETRIES", default=2, minimum=1)
retry_delay = _env_int("RETRY_DELAY", default=1, minimum=0)

max_cached_checks = _env_int("MAX_CACHED_CHECKS", default=50000, minimum=500)
captcha_cache_size = _env_int("CAPTCHA_CACHE_SIZE", default=500, minimum=50)
db_queue_maxsize = _env_int("DB_QUEUE_MAXSIZE", default=10000, minimum=100)
db_commit_batch = _env_int("DB_COMMIT_BATCH", default=100, minimum=1)

startup_channel = os.getenv("STARTUP_CHANNEL", "").strip()
tesseract_cmd = os.getenv("TESSERACT_CMD", "").strip() or None
