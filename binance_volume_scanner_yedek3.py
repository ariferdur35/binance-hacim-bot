import os
import json
import asyncio
import logging
import time
from datetime import datetime
from dotenv import load_dotenv

import aiohttp
from aiohttp import ClientSession, ClientTimeout

load_dotenv()

# ============================================================
#  AYARLAR
# ============================================================
TELEGRAM_BOT_TOKEN = os.getenv("BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    raise ValueError("BOT_TOKEN bulunamadı!")

INTERVAL          = "5m"
SCAN_PERIOD_MIN   = 1
LOOKBACK_BARS     = 20
VOLUME_MULTIPLIER = 4
MIN_VOLUME_USDT   = 20000
MIN_BAR_CHANGE    = 2.5

SUBSCRIBERS_FILE  = "subscribers.json"
MAX_LEN           = 4000
SENT_BARS_MAX     = 5000  # memory safe duplicate kontrolü

BINANCE_BASE = "https://api.binance.com"
TELEGRAM_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Global olarak gönderilmiş barları tutacağız
sent_bars = {}  # {bar_id: timestamp}
IGNORE_PERIOD = 60 * 60  # 1 saat (saniye cinsinden)

# ----------------------------------------------------------
# Abone yönetimi
# ----------------------------------------------------------
def load_subscribers() -> set:
    if os.path.exists(SUBSCRIBERS_FILE):
        with open(SUBSCRIBERS_FILE, "r") as f:
            return set(json.load(f))
    return set()

def save_subscribers(subscribers: set) -> None:
    with open(SUBSCRIBERS_FILE, "w") as f:
        json.dump(list(subscribers), f)

def add_subscriber(chat_id: int, subscribers: set) -> bool:
    if chat_id not in subscribers:
        subscribers.add(chat_id)
        save_subscribers(subscribers)
        return True
    return False

# ----------------------------------------------------------
# Telegram fonksiyonları (async)
# ----------------------------------------------------------
async def send_message(session: ClientSession, chat_id: int, text: str) -> None:
    url = f"{TELEGRAM_BASE}/sendMessage"
    lines = text.split("\n")
    chunk = ""

    try:
        for line in lines:
            if len(chunk) + len(line) + 1 > MAX_LEN:
                payload = {"chat_id": chat_id, "text": chunk, "parse_mode": "HTML"}
                async with session.post(url, json=payload, timeout=10) as resp:
                    await resp.text()
                chunk = ""
            chunk += line + "\n"

        if chunk:
            payload = {"chat_id": chat_id, "text": chunk, "parse_mode": "HTML"}
            async with session.post(url, json=payload, timeout=10) as resp:
                await resp.text()
    except Exception as e:
        log.error(f"Mesaj gönderilemedi (chat_id={chat_id}): {e}")

async def send_to_all(session: ClientSession, subscribers: set, message: str) -> None:
    if not subscribers:
        log.warning("Abone listesi boş, mesaj gönderilmedi.")
        return
    log.info(f"{len(subscribers)} aboneye mesaj gönderiliyor...")
    tasks = [send_message(session, chat_id, message) for chat_id in subscribers]
    await asyncio.gather(*tasks)

# ----------------------------------------------------------
# Binance fonksiyonları (async)
# ----------------------------------------------------------
async def get_all_usdt_symbols(session: ClientSession) -> list:
    url = f"{BINANCE_BASE}/api/v3/exchangeInfo"
    async with session.get(url, timeout=10) as resp:
        data = await resp.json()
    return [
        s["symbol"]
        for s in data["symbols"]
        if s["quoteAsset"] == "USDT" and s["status"] == "TRADING" and s["isSpotTradingAllowed"]
    ]

async def get_klines(session: ClientSession, symbol: str, interval: str, limit: int) -> list:
    url = f"{BINANCE_BASE}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    async with session.get(url, params=params, timeout=10) as resp:
        return await resp.json()

async def check_volume_spike(session: ClientSession, symbol: str) -> dict | None:
    global sent_bars
    try:
        klines = await get_klines(session, symbol, INTERVAL, LOOKBACK_BARS)
        if len(klines) < LOOKBACK_BARS:
            return None

        closed = klines[-2]
        previous = klines[:-2]

        last_vol = float(closed[5])
        avg_vol = sum(float(k[5]) for k in previous) / len(previous)
        close_price = float(closed[4])
        open_price = float(closed[1])
        price_change = ((close_price - open_price) / open_price) * 100
        ratio = last_vol / avg_vol

        if avg_vol == 0 or last_vol * close_price < MIN_VOLUME_USDT:
            return None
        if abs(price_change) < MIN_BAR_CHANGE:
            return None
        if ratio < VOLUME_MULTIPLIER:
            return None

        bar_id = f"{symbol}-{closed[0]}"
        now_ts = time.time()

        # 1 saat ignore kontrolü
        if bar_id in sent_bars and now_ts - sent_bars[bar_id] < IGNORE_PERIOD:
            return None

        sent_bars[bar_id] = now_ts

        # Memory safe: eski kayıtları temizle
        sent_bars = {k: v for k, v in sent_bars.items() if now_ts - v < IGNORE_PERIOD}

        bar_time = datetime.utcfromtimestamp(closed[0] / 1000).strftime("%H:%M UTC")
        return {
            "symbol": symbol,
            "last_vol": last_vol,
            "avg_vol": avg_vol,
            "ratio": ratio,
            "close_price": close_price,
            "price_change": price_change,
            "bar_time": bar_time,
        }
    except Exception:
        return None

# ----------------------------------------------------------
# Mesaj formatlama
# ----------------------------------------------------------
def _fmt(val: float) -> str:
    if val >= 1_000_000:
        return f"{val/1_000_000:.2f}M"
    if val >= 1_000:
        return f"{val/1_000:.1f}K"
    return f"{val:.2f}"

def build_message(results: list, scan_time: str) -> str:
    lines = [
        f"🔥 <b>HACİM TARAMA SONUÇLARI</b>",
        f"🕐 Tarama: <b>{scan_time} UTC</b>",
        f"📊 Zaman dilimi: <b>{INTERVAL}</b> | Eşik: <b>%{int((VOLUME_MULTIPLIER-1)*100)} artış</b>",
        f"🎯 Bulunan coin: <b>{len(results)}</b>",
        "─" * 30,
    ]
    for i, r in enumerate(results, 1):
        direction = "🟢" if r["price_change"] >= 0 else "🔴"
        lines.append(
            f"\n{i}. {direction} <b>{r['symbol']}</b>\n"
            f"   💰 Fiyat: <code>{r['close_price']:.6f}</code> USDT\n"
            f"   📈 Bar değişimi: <b>{r['price_change']:+.2f}%</b>\n"
            f"   📦 Hacim artışı: <b>x{r['ratio']:.2f}</b> "
            f"({_fmt(r['last_vol'])} / ort. {_fmt(r['avg_vol'])})\n"
            f"   ⏱ Bar saati: {r['bar_time']}"
        )
    lines.append("\n" + "─" * 30)
    lines.append("⚡ <i>Bu bir sinyal değil, hacim anomali bildirimidir.</i>")
    return "\n".join(lines)

# ----------------------------------------------------------
# Ana tarama task
# ----------------------------------------------------------
async def run_scan(session: ClientSession, subscribers: set) -> None:
    log.info("Tarama başlıyor...")
    scan_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    symbols = await get_all_usdt_symbols(session)
    log.info(f"Toplam {len(symbols)} USDT paritesi bulundu.")

    # Async olarak tüm coinleri kontrol et
    tasks = [check_volume_spike(session, symbol) for symbol in symbols]
    results = [res for res in await asyncio.gather(*tasks) if res]

    log.info(f"Tarama tamamlandı. {len(results)} coin eşiği aştı.")
    if results:
        results.sort(key=lambda x: x["ratio"], reverse=True)
        message = build_message(results, scan_time)
        await send_to_all(session, subscribers, message)

# ----------------------------------------------------------
# Telegram listener task
# ----------------------------------------------------------
async def get_updates(session: ClientSession, offset: int = 0) -> list:
    url = f"{TELEGRAM_BASE}/getUpdates"
    params = {"timeout": 10, "offset": offset}
    try:
        async with session.get(url, params=params, timeout=15) as resp:
            data = await resp.json()
        return data.get("result", [])
    except Exception as e:
        log.error(f"getUpdates hatası: {e}")
        return []

async def process_updates(session: ClientSession, subscribers: set, last_update_id: int) -> int:
    updates = await get_updates(session, last_update_id + 1)
    for update in updates:
        last_update_id = update["update_id"]
        message = update.get("message", {})
        text = message.get("text", "")
        chat = message.get("chat", {})
        chat_id = chat.get("id")
        username = chat.get("username", "")
        first_name = chat.get("first_name", "Kullanıcı")

        if not chat_id:
            continue

        if text == "/start":
            log.info(f"✅ /start komutu — Chat ID: {chat_id} | @{username} | {first_name}")
            is_new = add_subscriber(chat_id, subscribers)
            if is_new:
                await send_message(session, chat_id,
                    f"👋 Merhaba <b>{first_name}</b>!\n✅ Başarıyla abone oldun.\n📊 Tarama: {INTERVAL}\n⚡ Sinyaller gelmeye başlayacak.\n❌ Durdurmak için: /stop")
            else:
                await send_message(session, chat_id, f"ℹ️ Zaten abonesin!")

        elif text == "/stop":
            if chat_id in subscribers:
                subscribers.discard(chat_id)
                save_subscribers(subscribers)
                await send_message(session, chat_id, "❌ Aboneliğin iptal edildi.")
            else:
                await send_message(session, chat_id, "ℹ️ Zaten abone değilsin.")

        elif text == "/liste":
            await send_message(session, chat_id, f"👥 Toplam abone sayısı: <b>{len(subscribers)}</b>")

    return last_update_id

# ----------------------------------------------------------
# Main
# ----------------------------------------------------------
async def main():
    log.info("="*50)
    log.info("  Binance Hacim Tarayıcı Botu Başlatıldı (Async)")
    log.info(f"  Zaman dilimi  : {INTERVAL}")
    log.info(f"  Tarama aralığı: Her {SCAN_PERIOD_MIN} dakika")
    log.info(f"  Hacim eşiği   : %{int((VOLUME_MULTIPLIER-1)*100)} artış")
    log.info("="*50)

    subscribers = load_subscribers()
    log.info(f"Mevcut abone sayısı: {len(subscribers)}")

    last_update_id = 0
    next_scan_time = asyncio.get_event_loop().time()

    async with aiohttp.ClientSession(timeout=ClientTimeout(total=15)) as session:
        while True:
            last_update_id = await process_updates(session, subscribers, last_update_id)

            if asyncio.get_event_loop().time() >= next_scan_time:
                try:
                    await run_scan(session, subscribers)
                except Exception as e:
                    log.error(f"Tarama hatası: {e}")
                next_scan_time = asyncio.get_event_loop().time() + (SCAN_PERIOD_MIN * 60)

            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())