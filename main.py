import os
import asyncio
import logging
import time
import json
from datetime import datetime
from dotenv import load_dotenv
import aiohttp
from aiohttp import ClientSession, ClientTimeout
import pandas as pd

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
ema_sent    = {}  # MA233 cooldown
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
        avg_klines = klines[:-2]  # açık bar ve son kapanmış bar hariç
        last_vol = float(closed[5])
        close_price = float(closed[4])
        open_price = float(closed[1])
        price_change = ((close_price - open_price) / open_price) * 100
        # EMA hacim ortalaması
        avg_vol = pd.Series([float(k[5]) for k in avg_klines]).ewm(span=14, adjust=False).mean().iloc[-1]
        ratio = last_vol / avg_vol

        # coin bazlı cooldown
        now_ts = time.time()
        if symbol in sent_coins and now_ts - sent_coins[symbol] < COOLDOWN_SECONDS:
            return None
       
        # filtreler
        if avg_vol == 0 or last_vol * close_price < MIN_VOLUME_USDT:
            return None
        if abs(price_change) < MIN_BAR_CHANGE:
            return None
        if ratio < VOLUME_MULTIPLIER:
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
# EMA233 kırılım batch
# =========================
async def check_ema233_breakout_batch(session: ClientSession, symbols: list):
    global ema_sent
    results = []

    # tüm coinler için klines çek
    tasks = [get_klines(session, s, ema_time, 500) for s in symbols]
    all_klines = await asyncio.gather(*tasks)

    now = time.time()
    for symbol, klines in zip(symbols, all_klines):
        try:
            if len(klines) < 300:
                continue

            closes_for_ema = [float(k[4]) for k in klines[:-1]]
            series = pd.Series(closes_for_ema)
            ema233 = series.ewm(span=233, adjust=False).mean()

            last_close = float(klines[-1][4])  # anlık (live) fiyat
            prev_close = closes_for_ema[-1]    # son kapanmış bar
            last_ema = ema233.iloc[-1]
            prev_ema = ema233.iloc[-2]

            if symbol in ema_sent and now - ema_sent[symbol] < MA_COOLDOWN:
                continue

            if prev_close < prev_ema and last_close > last_ema:
                current_vol = float(klines[-1][5])
                prev_vol    = float(klines[-2][5])
                if prev_vol == 0 or current_vol < prev_vol * 1.10:
                    continue
                vol_increase = (current_vol / prev_vol - 1) * 100
                ema_sent[symbol] = now
                results.append({"symbol": symbol, "price": last_close, "ma": last_ema, "interval": ema_time, "vol_increase": vol_increase})
        except Exception as e:
            log.debug(f"EMA233 batch hata {symbol}: {e}")

    return results

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

def build_ema_message(results):
    if not results:
        return ""
    interval = results[0].get("interval", ema_time)
    lines = [
        "🚀 <b>EMA233 KIRILIM</b>",
        f"📊 Zaman dilimi: {interval}",
        "─"*25
    ]
    for i, r in enumerate(results,1):
        lines.append(
            f"\n{i}. <b>{r['symbol']}</b>\n"
            f"   💰 Fiyat: <code>{r['price']:.6f}</code>\n"
            f"   📈 EMA233: <code>{r['ma']:.6f}</code>\n"
            f"   {'🟢' if r['vol_increase'] >= 0 else '🔴'} Hacim: <b>{'+' if r['vol_increase'] >= 0 else ''}{r['vol_increase']:.1f}%</b>"
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
    ema_results = []

    # chunk bazlı işle
    for i in range(0, len(symbols), CHUNK_SIZE):
        chunk = symbols[i:i+CHUNK_SIZE]
        # hacim kontrolü
        volume_tasks = [check_volume_spike(session, s) for s in chunk]
        vol_res = await asyncio.gather(*volume_tasks)
        volume_results.extend([r for r in vol_res if r])

        # EMA233 batch kontrol
        ema_res = await check_ema233_breakout_batch(session, chunk)
        ema_results.extend(ema_res)

    if volume_results:
        volume_results.sort(key=lambda x:x["ratio"], reverse=True)
        message = build_message(volume_results, scan_time)
        await send_to_all(session, subscribers, message)

    if ema_results:
        message = build_ema_message(ema_results)
        await send_to_all(session, subscribers, message)

    log.info(f"Hacim: {len(volume_results)} | EMA233: {len(ema_results)}")

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
        # username = chat.get("username","")
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