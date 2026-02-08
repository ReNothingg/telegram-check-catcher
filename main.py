import asyncio
import logging
import random
import re
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from io import BytesIO
from typing import Awaitable, Callable, Deque, Optional, Set, Tuple

import aiosqlite
import pytesseract
from PIL import Image
from telethon import TelegramClient, events
from telethon.errors import (
    ChannelPrivateError,
    ChatWriteForbiddenError,
    FloodWaitError,
    InviteHashExpiredError,
    InviteHashInvalidError,
    RPCError,
    UserAlreadyParticipantError,
)
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest

from config import (
    anti_captcha,
    api_hash,
    api_id,
    avto_vivod,
    captcha_cache_size,
    channel,
    db_commit_batch,
    db_queue_maxsize,
    max_cached_checks,
    max_retries,
    retry_delay,
    startup_channel,
    tesseract_cmd,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("check-catcher")

if tesseract_cmd:
    pytesseract.pytesseract.tesseract_cmd = tesseract_cmd

DB_PATH = "history.db"
DB_TABLES = {"checks", "wallet"}
SUBSCRIBE_BOT_IDS = (1985737506, 5014831088, 5794061503, 1559501630)
CRYPTO_BLACK_LIST = (1622808649, 1559501630, 1985737506, 5014831088, 6014729293, 5794061503)
CAPTCHA_CHAT_ID = 1559501630
SUCCESS_MARKERS = ("Вы получили", "Вы обналичили", "✅ Вы получили:", "💰 Вы получили")
CAPTCHA_TEXT_RE = re.compile(r"^[A-Za-z0-9]{4,12}$")

client = TelegramClient(
    session="session",
    api_id=api_id,
    api_hash=api_hash,
    system_version="4.16.30-vxSOSYNXA",
)

code_regex = re.compile(
    r"t\.me/(CryptoBot|send|tonRocketBot|CryptoTestnetBot|wallet|xrocket|xJetSwapBot)\?start="
    r"(CQ[A-Za-z0-9]{10}|C-[A-Za-z0-9]{10}|t_[A-Za-z0-9]{15}|mci_[A-Za-z0-9]{15}|c_[a-z0-9]{24})",
    re.IGNORECASE,
)

executor = ThreadPoolExecutor(max_workers=5)
seen_codes: Set[str] = set()
seen_codes_order: Deque[str] = deque()
captcha_answers: Deque[str] = deque()
captcha_answers_set: Set[str] = set()
session_checks_count = 0

db_conn: Optional[aiosqlite.Connection] = None
db_queue: asyncio.Queue = asyncio.Queue(maxsize=db_queue_maxsize)
db_writer_task: Optional[asyncio.Task] = None
background_tasks: Set[asyncio.Task] = set()


def create_background_task(coro: Awaitable[object], name: str) -> asyncio.Task:
    task = asyncio.create_task(coro, name=name)
    background_tasks.add(task)
    task.add_done_callback(_on_background_task_done)
    return task


def _on_background_task_done(task: asyncio.Task) -> None:
    background_tasks.discard(task)
    with suppress(asyncio.CancelledError):
        error = task.exception()
        if error:
            logger.exception("Background task failed: %s", task.get_name(), exc_info=error)


def remember_code(code: str) -> bool:
    if code in seen_codes:
        return False

    seen_codes.add(code)
    seen_codes_order.append(code)
    while len(seen_codes_order) > max_cached_checks:
        oldest = seen_codes_order.popleft()
        seen_codes.discard(oldest)
    return True


def remember_captcha_answer(answer: str) -> bool:
    if answer in captcha_answers_set:
        return False

    captcha_answers.append(answer)
    captcha_answers_set.add(answer)
    while len(captcha_answers) > captcha_cache_size:
        oldest = captcha_answers.popleft()
        captcha_answers_set.discard(oldest)
    return True


async def init_db() -> None:
    global db_conn, db_writer_task

    db_conn = await aiosqlite.connect(DB_PATH)
    await db_conn.execute("PRAGMA journal_mode=WAL")
    await db_conn.execute("PRAGMA synchronous=NORMAL")
    await db_conn.execute("PRAGMA temp_store=MEMORY")

    await db_conn.execute(
        """
        CREATE TABLE IF NOT EXISTS checks (
            code TEXT PRIMARY KEY,
            created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        )
        """
    )
    await db_conn.execute(
        """
        CREATE TABLE IF NOT EXISTS wallet (
            code TEXT PRIMARY KEY,
            created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        )
        """
    )
    await db_conn.commit()

    await load_recent_codes("checks")
    await load_recent_codes("wallet")
    logger.info("Кэш загружен: %s кодов в памяти.", len(seen_codes))

    db_writer_task = create_background_task(db_writer_loop(), "db-writer")


async def load_recent_codes(table: str) -> None:
    if db_conn is None or table not in DB_TABLES:
        return

    query = f"SELECT code FROM {table} ORDER BY rowid DESC LIMIT ?"
    async with db_conn.execute(query, (max_cached_checks,)) as cursor:
        rows = await cursor.fetchall()

    for row in reversed(rows):
        remember_code(row[0])


async def db_writer_loop() -> None:
    if db_conn is None:
        raise RuntimeError("DB connection is not initialized")

    pending = 0
    while True:
        item = await db_queue.get()
        if item is None:
            db_queue.task_done()
            break

        code, table = item
        try:
            if table not in DB_TABLES:
                raise ValueError(f"Invalid table name: {table}")

            await db_conn.execute(f"INSERT OR IGNORE INTO {table} (code) VALUES (?)", (code,))
            pending += 1

            if pending >= db_commit_batch or db_queue.empty():
                await db_conn.commit()
                pending = 0
        except Exception:
            logger.exception("Ошибка записи в БД для table=%s", table)
        finally:
            db_queue.task_done()

    if pending:
        await db_conn.commit()


def db_add_check(code: str, table: str = "checks") -> None:
    if table not in DB_TABLES:
        logger.error("Попытка записи в неизвестную таблицу: %s", table)
        return

    try:
        db_queue.put_nowait((code, table))
    except asyncio.QueueFull:
        logger.warning("Очередь БД заполнена, пропускаю запись кода: %s", code[:8])


async def db_get_total_count() -> int:
    if db_conn is None:
        return 0

    async with db_conn.execute("SELECT COUNT(*) FROM checks") as cursor:
        result = await cursor.fetchone()
    return result[0] if result else 0


async def shutdown_db() -> None:
    global db_conn, db_writer_task

    if db_writer_task is not None:
        await db_queue.put(None)
        await db_queue.join()
        await db_writer_task
        db_writer_task = None

    if db_conn is not None:
        await db_conn.close()
        db_conn = None


def local_ocr_sync(file_bytes: bytes) -> Optional[str]:
    try:
        image = Image.open(BytesIO(file_bytes)).convert("L")
        text = pytesseract.image_to_string(image, lang="eng", config="--oem 3 --psm 7")
        return re.sub(r"\s+", "", text)
    except Exception:
        return None


async def solve_captcha_local(file_bytes: bytes) -> Optional[str]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, local_ocr_sync, file_bytes)


async def safe_execution(
    action_name: str,
    action_factory: Callable[[], Awaitable[object]],
    attempts: int = 2,
) -> bool:
    for attempt in range(1, attempts + 1):
        try:
            await action_factory()
            return True
        except FloodWaitError as error:
            if attempt >= attempts:
                logger.warning("%s: FloodWait %s сек (без ретрая)", action_name, error.seconds)
                return False
            wait_seconds = error.seconds + random.uniform(0.2, 0.8)
            logger.warning("%s: FloodWait %s сек, жду %.1f сек...", action_name, error.seconds, wait_seconds)
            await asyncio.sleep(wait_seconds)
        except (
            ChannelPrivateError,
            ChatWriteForbiddenError,
            UserAlreadyParticipantError,
            InviteHashExpiredError,
            InviteHashInvalidError,
        ) as error:
            logger.debug("%s: пропуск (%s)", action_name, error.__class__.__name__)
            return False
        except RPCError as error:
            if attempt >= attempts:
                logger.warning("%s: RPC ошибка (%s)", action_name, error)
                return False
            await asyncio.sleep(retry_delay)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("%s: непредвиденная ошибка", action_name)
            return False
    return False


async def fast_activate(bot_username: str, code: str) -> None:
    action_name = f"activate:{bot_username}:{code[:8]}"
    await safe_execution(
        action_name,
        lambda: client.send_message(bot_username, f"/start {code}"),
        attempts=max_retries,
    )


def schedule_code_activation(bot_name: str, code: str) -> None:
    if not remember_code(code):
        return

    db_add_check(code)
    create_background_task(
        fast_activate(bot_name, code),
        f"activate:{bot_name}:{code[:8]}",
    )


def parse_telegram_url(url: str) -> Tuple[Optional[str], Optional[str]]:
    if "t.me/+" in url:
        invite_hash = url.split("t.me/+")[1].split("?")[0].strip("/")
        if invite_hash:
            return "invite", invite_hash
    if "t.me/" in url:
        username = url.split("t.me/")[1].split("?")[0].strip("/")
        if username:
            return "public", username
    return None, None


@client.on(events.NewMessage(outgoing=False))
async def main_handler(event) -> None:
    text = event.raw_text or ""
    if "t.me" not in text and "start=" not in text:
        return

    for bot_name, code in code_regex.findall(text):
        schedule_code_activation(bot_name, code)

    reply_markup = getattr(event.message, "reply_markup", None)
    if not reply_markup:
        return

    for row in reply_markup.rows:
        for button in row.buttons:
            button_url = getattr(button, "url", None)
            if not button_url:
                continue

            match = code_regex.search(button_url)
            if match:
                schedule_code_activation(match.group(1), match.group(2))


@client.on(events.NewMessage(chats=SUBSCRIBE_BOT_IDS))
async def subscribe_handler(event) -> None:
    reply_markup = getattr(event.message, "reply_markup", None)
    if not reply_markup:
        return

    text = event.raw_text or ""
    if "Вы не можете активировать" in text or "Для активации" in text:
        await safe_execution("click:Check", lambda: event.message.click(data=b"Check"))
        await safe_execution("click:check-subscribe", lambda: event.message.click(data=b"check-subscribe"))

    for row in reply_markup.rows:
        for button in row.buttons:
            button_url = getattr(button, "url", None)
            if not button_url:
                continue

            link_type, link_value = parse_telegram_url(button_url)
            if link_type == "invite":
                create_background_task(
                    safe_execution(
                        f"join:invite:{link_value[:8]}",
                        lambda invite=link_value: client(ImportChatInviteRequest(invite)),
                        attempts=max_retries,
                    ),
                    f"join-invite:{link_value[:8]}",
                )
            elif link_type == "public":
                create_background_task(
                    safe_execution(
                        f"join:public:{link_value}",
                        lambda username=link_value: client(JoinChannelRequest(username)),
                        attempts=max_retries,
                    ),
                    f"join-public:{link_value}",
                )

            match = code_regex.search(button_url)
            if match:
                schedule_code_activation(match.group(1), match.group(2))

    for row in reply_markup.rows:
        for button in row.buttons:
            button_data = getattr(button, "data", None)
            if not button_data:
                continue

            data_text = button_data.decode("utf-8", errors="ignore")
            if data_text.startswith(("showCheque_", "activateCheque_")):
                create_background_task(
                    safe_execution(
                        f"click:{data_text[:32]}",
                        lambda payload=button_data: event.message.click(data=payload),
                    ),
                    f"click:{data_text[:20]}",
                )


@client.on(events.NewMessage(outgoing=True, pattern=r"^\.stats$"))
async def stats_handler(event) -> None:
    total_db = await db_get_total_count()
    msg = (
        "<b>📊 Статистика (Turbo Mode):</b>\n\n"
        f"🔹 Активировано за сессию: <b>{session_checks_count}</b>\n"
        f"🔹 Всего в базе: <b>{total_db}</b>\n"
        f"🔹 Кэш в памяти: <b>{len(seen_codes)}</b>\n"
        f"🔹 Очередь записи: <b>{db_queue.qsize()}</b>"
    )
    await event.edit(msg, parse_mode="HTML")


def filter_success(event) -> bool:
    text = event.raw_text or ""
    return any(marker in text for marker in SUCCESS_MARKERS)


@client.on(events.NewMessage(chats=CRYPTO_BLACK_LIST, func=filter_success))
async def success_handler(event) -> None:
    global session_checks_count
    session_checks_count += 1

    summary = (event.raw_text or "").split("\n", 1)[0]
    msg = f"✅ <b>УСПЕХ!</b>\n{summary}\nВсего за сессию: {session_checks_count}"

    delivered = await safe_execution(
        "notify:channel",
        lambda: client.send_message(channel, msg, parse_mode="HTML"),
    )
    if not delivered and channel != "me":
        await safe_execution(
            "notify:me-fallback",
            lambda: client.send_message("me", msg, parse_mode="HTML"),
        )


if anti_captcha:
    @client.on(events.NewMessage(chats=(CAPTCHA_CHAT_ID,), func=lambda e: bool(e.photo)))
    async def captcha_handler(event) -> None:
        photo_bytes = await event.download_media(bytes)
        if not photo_bytes:
            return

        recognized_text = await solve_captcha_local(photo_bytes)
        if not recognized_text:
            return

        recognized_text = recognized_text.strip()
        if not CAPTCHA_TEXT_RE.fullmatch(recognized_text):
            return
        if not remember_captcha_answer(recognized_text):
            return

        await asyncio.sleep(random.uniform(0.4, 1.0))
        await safe_execution(
            "captcha:answer",
            lambda: client.send_message("CryptoBot", message=recognized_text),
            attempts=max_retries,
        )


async def main() -> None:
    try:
        await init_db()
        await client.start()

        if startup_channel:
            await safe_execution(
                f"startup:join:{startup_channel}",
                lambda: client(JoinChannelRequest(startup_channel)),
            )

        logger.info("Ловец чеков запущен.")
        if avto_vivod:
            logger.warning("AVTO_VIVOD включен, но модуль вывода пока не реализован.")

        await client.run_until_disconnected()
    except Exception:
        logger.exception("Critical error")
    finally:
        pending = list(background_tasks)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

        await shutdown_db()
        executor.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    asyncio.run(main())
