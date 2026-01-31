import asyncio
from io import BytesIO
import regex as re
import pytesseract
from PIL import Image
from telethon import TelegramClient, events
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.errors import FloodWaitError, ChannelPrivateError, ChatWriteForbiddenError
from concurrent.futures import ThreadPoolExecutor
import aiosqlite
from config import *

# pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

client = TelegramClient(session='session', api_id=int(api_id), api_hash=api_hash, system_version="4.16.30-vxSOSYNXA ")

code_regex = re.compile(r"t\.me/(CryptoBot|send|tonRocketBot|CryptoTestnetBot|wallet|xrocket|xJetSwapBot)\?start=(CQ[A-Za-z0-9]{10}|C-[A-Za-z0-9]{10}|t_[A-Za-z0-9]{15}|mci_[A-Za-z0-9]{15}|c_[a-z0-9]{24})", re.IGNORECASE)
url_regex = re.compile(r"https:\/\/t\.me\/\+(\w{12,})")
public_regex = re.compile(r"https:\/\/t\.me\/(\w{4,})")

replace_chars = ''' @#&+()*"'…;,!№•—–·±<{>}†★‡„“”«»‚‘’‹›¡¿‽~`|√π÷×§∆\\°^%©®™✓₤$₼€₸₾₶฿₳₥₦₫₿¤₲₩₮¥₽₻₷₱₧£₨¢₠₣₢₺₵₡₹₴₯₰₪'''
translation = str.maketrans('', '', replace_chars)

executor = ThreadPoolExecutor(max_workers=5)
crypto_black_list = [1622808649, 1559501630, 1985737506, 5014831088, 6014729293, 5794061503]

seen_codes = set()
session_checks_count = 0
captches = []

async def init_db():
    async with aiosqlite.connect('history.db') as db:
        await db.execute("CREATE TABLE IF NOT EXISTS checks (code TEXT PRIMARY KEY)")
        await db.execute("CREATE TABLE IF NOT EXISTS wallet (code TEXT PRIMARY KEY)")
        await db.commit()

        async with db.execute("SELECT code FROM checks") as cursor:
            async for row in cursor:
                seen_codes.add(row[0])
        async with db.execute("SELECT code FROM wallet") as cursor:
            async for row in cursor:
                seen_codes.add(row[0])
    print(f"💲 Кэш загружен: {len(seen_codes)} чеков в памяти.")

async def db_add_check(code, table='checks'):
    try:
        async with aiosqlite.connect('history.db') as db:
            await db.execute(f"INSERT OR IGNORE INTO {table} (code) VALUES (?)", (code,))
            await db.commit()
    except Exception as e:
        print(f"⚠️ Ошибка записи в БД: {e}")

async def db_get_total_count():
    async with aiosqlite.connect('history.db') as db:
        async with db.execute("SELECT COUNT(*) FROM checks") as cursor:
            result = await cursor.fetchone()
            return result[0] if result else 0

def local_ocr_sync(file_bytes: bytes):
    try:
        image = Image.open(BytesIO(file_bytes))
        text = pytesseract.image_to_string(image, lang='eng')
        return text.replace(" ", "").strip()
    except Exception:
        return None

async def solve_captcha_local(file: bytes):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, local_ocr_sync, file)

async def fast_activate(bot_username, code):
    try:
        await client.send_message(bot_username, f'/start {code}')
    except FloodWaitError as e:
        print(f"⚠️ FloodWait на {e.seconds} сек. Спим...")
        await asyncio.sleep(e.seconds)
        await client.send_message(bot_username, f'/start {code}')
    except Exception as e:
        print(f"⚠️ Ошибка активации: {e}")

async def safe_execution(coroutine):
    try:
        await coroutine
    except FloodWaitError as e:
        print(f"⚠️ FloodWait: Ждем {e.seconds} секунд...")
        await asyncio.sleep(e.seconds)
    except (ChannelPrivateError, ChatWriteForbiddenError):
        pass
    except Exception as e:
        print(f"⚠️ Ошибка выполнения: {e}")

@client.on(events.NewMessage(outgoing=False))
async def main_handler(event):
    text = event.raw_text

    if 't.me' not in text and 'start=' not in text:
        return

    codes = code_regex.findall(text)
    if codes:
        for bot_name, code in codes:
            if code in seen_codes:
                continue

            seen_codes.add(code)
            asyncio.create_task(fast_activate(bot_name, code))
            asyncio.create_task(db_add_check(code))

    try:
        if event.message.reply_markup:
            for row in event.message.reply_markup.rows:
                for button in row.buttons:
                    if hasattr(button, 'url') and button.url:
                        match = code_regex.search(button.url)
                        if match:
                            bot_name = match.group(1)
                            code = match.group(2)
                            if code not in seen_codes:
                                seen_codes.add(code)
                                asyncio.create_task(fast_activate(bot_name, code))
                                asyncio.create_task(db_add_check(code))
    except AttributeError:
        pass

@client.on(events.NewMessage(chats=[1985737506, 5014831088, 5794061503, 1559501630]))
async def subscribe_handler(event):
    """Обработка условий подписки и нажатия кнопок"""
    try:
        if not event.message.reply_markup: return

        if "Вы не можете активировать" in event.raw_text or "Для активации" in event.raw_text:
             await safe_execution(event.message.click(data=b'Check'))
             await safe_execution(event.message.click(data=b'check-subscribe'))

        for row in event.message.reply_markup.rows:
            for button in row.buttons:
                if hasattr(button, 'url') and button.url:
                    if 't.me/+' in button.url:
                        try:
                            hash_arg = button.url.split('+')[1]
                            asyncio.create_task(client(ImportChatInviteRequest(hash_arg)))
                        except: pass
                    elif 't.me/' in button.url:
                        try:
                            chan = button.url.split('t.me/')[1]
                            asyncio.create_task(client(JoinChannelRequest(chan)))
                        except: pass

        if event.message.reply_markup:
             for row in event.message.reply_markup.rows:
                for button in row.buttons:
                    if hasattr(button, 'data'):
                        try:
                            data = button.data.decode()
                            if data.startswith(('showCheque_', 'activateCheque_')):
                                asyncio.create_task(event.message.click(data=button.data))
                        except: pass
    except Exception:
        pass

@client.on(events.NewMessage(outgoing=True, pattern='.stats'))
async def stats_handler(event):
    total_db = await db_get_total_count()
    msg = (
        f"<b>📊 Статистика (Turbo Mode):</b>\n\n"
        f"🔹 Активировано за сессию: <b>{session_checks_count}</b>\n"
        f"🔹 Всего в базе: <b>{total_db}</b>\n"
        f"🔹 Кэш в памяти: <b>{len(seen_codes)}</b>"
    )
    await event.edit(msg, parse_mode='HTML')

async def filter_success(event):
    text = event.message.text
    return any(x in text for x in ['Вы получили', 'Вы обналичили', '✅ Вы получили:', '💰 Вы получили'])

@client.on(events.NewMessage(chats=crypto_black_list, func=filter_success))
async def success_handler(event):
    global session_checks_count
    session_checks_count += 1

    try:
        summ = event.raw_text.split('\n')[0]
        await client.send_message('me', f'✅ <b>УСПЕХ!</b>\n{summ}\nВсего за сессию: {session_checks_count}', parse_mode='HTML')
    except Exception as e:
        print(f"Пойман чек, но ошибка логгирования: {e}")

if anti_captcha:
    @client.on(events.NewMessage(chats=[1559501630], func=lambda e: e.photo))
    async def captcha_handler(event):
        photo_bytes = await event.download_media(bytes)
        recognized_text = await solve_captcha_local(photo_bytes)
        if recognized_text and recognized_text not in captches:
            await asyncio.sleep(random.uniform(0.5, 1.5))
            await client.send_message('CryptoBot', message=recognized_text)
            captches.append(recognized_text)

async def pay_out():
    while True:
        await asyncio.sleep(3600)
        try:
            pass
        except Exception:
            pass

async def main():
    import random
    try:
        await init_db()
        await client.start()

        try:
            await client(JoinChannelRequest('ReNothinggTube'))
        except: pass

        print(f'💲 Turbo-Ловец запущен!')

        if avto_vivod:
            asyncio.create_task(pay_out())

        await client.run_until_disconnected()
    except Exception as e:
        print(f'⚠️ Critical Error: {e}')

if __name__ == '__main__':
    asyncio.run(main())