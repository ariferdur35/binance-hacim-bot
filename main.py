import os
import asyncio
import logging
import time
import json
from datetime import datetime
from dotenv import load_dotenv
import aiohttp
from aiohttp import ClientSession, ClientTimeout

load_dotenv()

# =========================
# AYARLAR
# =========================
TELEGRAM_BOT_TOKEN = os.getenv("BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    raise ValueError("BOT_TOKEN bulunamadı!")

INTERVAL          = "5m"
SCAN_PERIOD_MIN   = 1
LOOKBACK_BARS     = 20
VOLUME_MULTIPLIER = 4
MIN_VOLUME_USDT   = 20000
MIN_BAR_CHANGE    = 2.5
COOLDOWN_SECONDS  = 60 * 60  # coin bazlı 1 saat ignore (hacim)
MA_COOLDOWN       = 24*60*60 # coin bazlı 24 saat ignore (MA233)
CHUNK_SIZE        = 50        # async gather chunk
MAX_TELEGRAM_CONC = 10        # aynı anda gönderilecek mesaj sayısı

SUBSCRIBERS_FILE  = "subscribers.json"
MAX_LEN           = 4000

BINANCE_BASE      = "https://api.binance.com"
TELEGRAM_BASE     = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# =========================
# Global
# =========================
sent_coins = {}  # hacim cooldown
ma_sent    = {}  # MA233 cooldown
ema_time = "4h"

# =========================
# Abone yönetimi
# =========================
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

# =========================
# Telegram async
# =========================
semaphore = asyncio.Semaphore(MAX_TELEGRAM_CONC)

async def send_message(session: ClientSession, chat_id: int, text: str):
    async with semaphore:
        url = f"{TELEGRAM_BASE}/sendMessage"
        lines = text.split("\n")
        chunk = ""
        for line in lines:
            if len(chunk) + len(line) + 1 > MAX_LEN:
                payload = {"chat_id": chat_id, "text": chunk, "parse_mode": "HTML"}
                try:
                    async with session.post(url, json=payload) as resp:
                        await resp.text()
                except Exception as e:
                    log.error(f"Mesaj gönderilemedi {chat_id}: {e}")
                chunk = ""
            chunk += line + "\n"
        if chunk:
            payload = {"chat_id": chat_id, "text": chunk, "parse_mode": "HTML"}
            try:
                async with session.post(url, json=payload) as resp:
                    await resp.text()
            except Exception as e:
                log.error(f"Mesaj gönderilemedi {chat_id}: {e}")

async def send_to_all(session: ClientSession, subscribers: set, message: str):
    if not subscribers:
        return
    tasks = [send_message(session, cid, message) for cid in subscribers]
    await asyncio.gather(*tasks)

# =========================
# Binance async
# =========================
async def get_all_usdt_symbols(session: ClientSession) -> list:
    url = f"{BINANCE_BASE}/api/v3/exchangeInfo"
    async with session.get(url) as resp:
        data = await resp.json()
    return [
        s["symbol"] for s in data["symbols"]
        if s["quoteAsset"] == "USDT" and s["status"] == "TRADING" and s["isSpotTradingAllowed"]
    ]

async def get_klines(session: ClientSession, symbol: str, interval: str, limit: int):
    url = f"{BINANCE_BASE}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    async with session.get(url, params=params) as resp:
        return await resp.json()

def calculate_ema(values, alpha=0.3):
    ema = values[0]
    for v in values[1:]:
        ema = alpha * v + (1 - alpha) * ema
    return ema

# =========================
# Hacim kontrol
# =========================
async def check_volume_spike(session: ClientSession, symbol: str):
    global sent_coins
    try:
        klines = await get_klines(session, symbol, INTERVAL, LOOKBACK_BARS)
        if len(klines) < LOOKBACK_BARS:
            return None

        closed = klines[-2]
        previous = klines[:-2]
        last_vol = float(closed[5])
        close_price = float(closed[4])
        open_price = float(closed[1])
        price_change = ((close_price - open_price) / open_price) * 100
        avg_vol = calculate_ema([float(k[5]) for k in previous])
        ratio = last_vol / avg_vol

        # filtreler
        if avg_vol == 0 or last_vol * close_price < MIN_VOLUME_USDT:
            return None
        if abs(price_change) < MIN_BAR_CHANGE:
            return None
        if ratio < VOLUME_MULTIPLIER:
            return None

        # coin bazlı cooldown
        now_ts = time.time()
        if symbol in sent_coins and now_ts - sent_coins[symbol] < COOLDOWN_SECONDS:
            return None
        sent_coins[symbol] = now_ts

        bar_time = datetime.utcfromtimestamp(closed[0]/1000).strftime("%H:%M UTC")
        return {
            "symbol": symbol,
            "last_vol": last_vol,
            "avg_vol": avg_vol,
            "ratio": ratio,
            "close_price": close_price,
            "price_change": price_change,
            "bar_time": bar_time,
        }
    except Exception as e:
        log.debug(f"{symbol} hata: {e}")
        return None

# =========================
# EMA233 kırılım - 4 saatlik
# =========================
async def check_ema233_breakout(session: ClientSession, symbol: str, interval="4h"):
    """
    233 periyotluk EMA kırılımını kontrol eder
    """
    global ma_sent
    try:
        klines = await get_klines(session, symbol, interval, 500)
        if len(klines) < 233:
            return None

        closes = [float(k[4]) for k in klines]

        # EMA alpha hesaplama
        N = 233
        alpha = 2 / (N + 1)

        # EMA hesapla
        ema233 = calculate_ema(closes[-233:], alpha=alpha)
        prev_ema233 = calculate_ema(closes[-234:-1], alpha=alpha)

        last_close = closes[-1]
        prev_close = closes[-2]

        # cooldown
        now = time.time()
        if symbol in ma_sent and now - ma_sent[symbol] < MA_COOLDOWN:
            return None

        # Kırılım kontrolü
        if prev_close < prev_ema233 and last_close > ema233:
            ma_sent[symbol] = now
            return {"symbol": symbol, "price": last_close, "ma": ema233, "interval": ema_time}

        return None

    except Exception as e:
        log.debug(f"EMA233 hata {symbol}: {e}")
        return None

# =========================
# Mesaj formatlama
# =========================
def _fmt(val: float) -> str:
    if val >= 1_000_000:
        return f"{val/1_000_000:.2f}M"
    if val >= 1_000:
        return f"{val/1_000:.1f}K"
    return f"{val:.2f}"

def build_message(results, scan_time):
    lines = [
        f"🔥 <b>HACİM TARAMA SONUÇLARI</b>",
        f"🕐 Tarama: <b>{scan_time} UTC</b>",
        f"📊 Zaman dilimi: <b>{INTERVAL}</b> | Eşik: <b>%{int((VOLUME_MULTIPLIER-1)*100)} artış</b>",
        f"🎯 Bulunan coin: <b>{len(results)}</b>",
        "─"*25
    ]
    for i, r in enumerate(results,1):
        direction = "🟢" if r["price_change"]>=0 else "🔴"
        lines.append(
            f"\n{i}. {direction} <b>{r['symbol']}</b>\n"
            f"   💰 Fiyat: <code>{r['close_price']:.6f}</code> USDT\n"
            f"   📈 Bar değişimi: <b>{r['price_change']:+.2f}%</b>\n"
            f"   📦 Hacim artışı: <b>x{r['ratio']:.2f}</b> "
            f"({_fmt(r['last_vol'])}/ort. {_fmt(r['avg_vol'])})\n"
            f"   ⏱ Bar saati: {r['bar_time']}"
        )
    lines.append("\n" + "─"*30)
    lines.append("⚡ <i>Bu bir sinyal değil, hacim anomali bildirimidir.</i>")
    return "\n".join(lines)

# =========================
# EMA mesaj formatlama
# =========================
def build_ema_message(results):
    if not results:
        return ""

    interval = results[0].get("interval", ema_time)  # mesaj için interval
    lines = [
        "🚀 <b>EMA233 KIRILIM</b>",
        f"📊 Zaman dilimi: {interval}",
        "─"*25
    ]
    for i, r in enumerate(results,1):
        lines.append(
            f"\n{i}. <b>{r['symbol']}</b>\n"
            f"   💰 Fiyat: <code>{r['price']:.6f}</code>\n"
            f"   📈 EMA233: <code>{r['ma']:.6f}</code>"
        )
    lines.append("\n⚡ <i>Yeni kırılım sinyali</i>")
    return "\n".join(lines)

# =========================
# Ana tarama
# =========================
async def run_scan(session, subscribers):
    log.info("Tarama başlıyor...")
    scan_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    symbols = await get_all_usdt_symbols(session)
    log.info(f"Toplam {len(symbols)} USDT paritesi bulundu.")

    volume_results = []
    ma_results = []

    for i in range(0, len(symbols), CHUNK_SIZE):
        chunk = symbols[i:i+CHUNK_SIZE]

        volume_tasks = [check_volume_spike(session, s) for s in chunk]
        ema_tasks = [check_ema233_breakout(session, s, ema_time) for s in chunk]

        vol_res = await asyncio.gather(*volume_tasks)
        ema_res = await asyncio.gather(*ema_tasks)

        volume_results.extend([r for r in vol_res if r])
        ma_results.extend([r for r in ma_res if r])

    if volume_results:
        volume_results.sort(key=lambda x:x["ratio"], reverse=True)
        message = build_message(volume_results, scan_time)
        await send_to_all(session, subscribers, message)

    if ma_results:
        message = build_ema_message(ma_results)
        await send_to_all(session, subscribers, message)

    log.info(f"Hacim: {len(volume_results)} | MA233: {len(ma_results)}")

# =========================
# Telegram listener
# =========================
async def get_updates(session, offset=0):
    url = f"{TELEGRAM_BASE}/getUpdates"
    params = {"timeout":10, "offset":offset}
    try:
        async with session.get(url, params=params) as resp:
            data = await resp.json()
        return data.get("result",[])
    except Exception as e:
        log.error(f"getUpdates hatası: {e}")
        return []

async def process_updates(session, subscribers, last_update_id):
    updates = await get_updates(session, last_update_id+1)
    for u in updates:
        last_update_id = u["update_id"]
        msg = u.get("message",{})
        text = msg.get("text","")
        chat = msg.get("chat",{})
        chat_id = chat.get("id")
        username = chat.get("username","")
        first_name = chat.get("first_name","Kullanıcı")

        if not chat_id:
            continue
        if text=="/start":
            is_new = add_subscriber(chat_id, subscribers)
            if is_new:
                await send_message(session,chat_id,f"👋 Merhaba <b>{first_name}</b>!\n✅ Başarıyla abone oldun.\n📊 Tarama: {INTERVAL}\n⚡ Sinyaller gelmeye başlayacak.\n❌ Durdurmak için: /stop")
            else:
                await send_message(session,chat_id,f"ℹ️ Zaten abonesin!")
        elif text=="/stop":
            if chat_id in subscribers:
                subscribers.discard(chat_id)
                save_subscribers(subscribers)
                await send_message(session,chat_id,"❌ Aboneliğin iptal edildi.")
            else:
                await send_message(session,chat_id,"ℹ️ Zaten abone değilsin.")
        elif text=="/liste":
            await send_message(session,chat_id,f"👥 Toplam abone sayısı: <b>{len(subscribers)}</b>")
    return last_update_id

# =========================
# Main
# =========================
async def main():
    log.info("="*50)
    log.info("  Binance Hacim + MA233 Botu Başlatıldı (Async Profi)")
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
                next_scan_time = asyncio.get_event_loop().time() + (SCAN_PERIOD_MIN*60)
            await asyncio.sleep(1)

if __name__=="__main__":
    asyncio.run(main())