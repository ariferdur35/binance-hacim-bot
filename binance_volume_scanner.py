import requests
import time
import json
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ============================================================
#  AYARLAR - Sadece token'ı gir, Chat ID otomatik alınır
# ============================================================
TELEGRAM_BOT_TOKEN = os.getenv("BOT_TOKEN")

INTERVAL          = "15m"
SCAN_PERIOD_MIN   = 15
LOOKBACK_BARS     = 50
VOLUME_MULTIPLIER = 3.1
MIN_VOLUME_USDT = 10000  # minimum hacim 10k USDT
MIN_BAR_CHANGE = 5  # yüzde

SUBSCRIBERS_FILE  = "subscribers.json"   # Chat ID'ler burada saklanır
MAX_LEN = 4000
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BINANCE_BASE = "https://api.binance.com"
TELEGRAM_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"


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
# Telegram yardımcı fonksiyonlar
# ----------------------------------------------------------

def send_message(chat_id: int, text: str) -> None:
    url = f"{TELEGRAM_BASE}/sendMessage"

    try:
        lines = text.split("\n")
        chunk = ""

        for line in lines:
            if len(chunk) + len(line) + 1 > MAX_LEN:
                payload = {
                    "chat_id": chat_id,
                    "text": chunk,
                    "parse_mode": "HTML"
                }

                resp = requests.post(url, json=payload, timeout=10)
                resp.raise_for_status()
                chunk = ""

            chunk += line + "\n"

        if chunk:
            payload = {
                "chat_id": chat_id,
                "text": chunk,
                "parse_mode": "HTML"
            }

            resp = requests.post(url, json=payload, timeout=10)
            resp.raise_for_status()

    except Exception as e:
        log.error(f"Mesaj gönderilemedi (chat_id={chat_id}): {e}")


def send_to_all(subscribers: set, message: str) -> None:
    if not subscribers:
        log.warning("Abone listesi boş, mesaj gönderilmedi.")
        return
    log.info(f"{len(subscribers)} aboneye mesaj gönderiliyor...")
    for chat_id in subscribers:
        send_message(chat_id, message)
        time.sleep(0.05)


def get_updates(offset: int = 0) -> list:
    url = f"{TELEGRAM_BASE}/getUpdates"
    params = {"timeout": 10, "offset": offset}
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json().get("result", [])
    except Exception as e:
        log.error(f"getUpdates hatası: {e}")
        return []


# ----------------------------------------------------------
# /start komutunu dinle
# ----------------------------------------------------------

def process_updates(subscribers: set, last_update_id: int) -> int:
    updates = get_updates(offset=last_update_id + 1)
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
            print(f"\n{'='*50}")
            print(f"  YENİ ABONE!")
            print(f"  Chat ID   : {chat_id}")
            print(f"  İsim      : {first_name}")
            print(f"  Kullanıcı : @{username}")
            print(f"{'='*50}\n")

            is_new = add_subscriber(chat_id, subscribers)
            if is_new:
                send_message(chat_id, (
                    f"👋 Merhaba <b>{first_name}</b>!\n\n"
                    f"✅ Başarıyla abone oldun.\n\n"
                    f"🔍 Her <b>{SCAN_PERIOD_MIN} dakikada</b> bir Binance Spot USDT "
                    f"paritelerini tarayıp hacim anomalilerini sana bildireceğim.\n\n"
                    f"📊 Zaman dilimi: <b>{INTERVAL}</b>\n"
                    f"📦 Eşik: Son 20 barın ortalamasının <b>%{int((VOLUME_MULTIPLIER-1)*100)} üzeri</b>\n\n"
                    f"⚡ Sinyaller gelmeye başlayacak, hazır ol!\n\n"
                    f"❌ Durdurmak için: /stop"
                ))
            else:
                send_message(chat_id, f"ℹ️ <b>{first_name}</b>, zaten abonesin! Sinyaller gelmeye devam edecek.")

        elif text == "/stop":
            if chat_id in subscribers:
                subscribers.discard(chat_id)
                save_subscribers(subscribers)
                send_message(chat_id, "❌ Aboneliğin iptal edildi. Tekrar başlamak için /start yaz.")
                log.info(f"Abone çıkarıldı: {chat_id} (@{username})")
            else:
                send_message(chat_id, "ℹ️ Zaten abone değilsin.")

        elif text == "/liste":
            send_message(chat_id, f"👥 Toplam abone sayısı: <b>{len(subscribers)}</b>")

    return last_update_id


# ----------------------------------------------------------
# Binance fonksiyonlar
# ----------------------------------------------------------

def get_all_usdt_symbols() -> list:
    url = f"{BINANCE_BASE}/api/v3/exchangeInfo"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return [
        s["symbol"]
        for s in data["symbols"]
        if s["quoteAsset"] == "USDT"
        and s["status"] == "TRADING"
        and s["isSpotTradingAllowed"]
    ]


def get_klines(symbol: str, interval: str, limit: int) -> list:
    url = f"{BINANCE_BASE}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def check_volume_spike(symbol: str) -> dict | None:
    try:
        klines = get_klines(symbol, INTERVAL, LOOKBACK_BARS)
        if len(klines) < LOOKBACK_BARS:
            return None

        closed   = klines[-2]
        previous = klines[:-2]

        last_vol = float(closed[5])
        avg_vol  = sum(float(k[5]) for k in previous) / len(previous)

        if avg_vol == 0:
            return None

        ratio = last_vol / avg_vol

        close_price  = float(closed[4])
        open_price   = float(closed[1])
        price_change = ((close_price - open_price) / open_price) * 100
        bar_time     = datetime.utcfromtimestamp(closed[0] / 1000).strftime("%H:%M UTC")

        # 🚀 Yeni filtreler
        if last_vol * close_price < MIN_VOLUME_USDT:  # USDT hacmi kontrolü
            return None
        if price_change < MIN_BAR_CHANGE:       # minimum bar değişimi kontrolü
            return None
        if ratio < VOLUME_MULTIPLIER:               # hacim artış oranı kontrolü
            return None

        # Tüm filtreleri geçen coin
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
        log.debug(f"{symbol} atlandı: {e}")
    return None


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
# Ana döngü
# ----------------------------------------------------------

def run_scan(subscribers: set) -> None:
    log.info("Tarama başlıyor...")
    scan_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M")

    symbols = get_all_usdt_symbols()
    log.info(f"Toplam {len(symbols)} USDT paritesi bulundu.")

    results = []
    for idx, symbol in enumerate(symbols):
        result = check_volume_spike(symbol)
        if result:
            results.append(result)
            log.info(f"✅ {symbol} — x{result['ratio']:.2f} hacim artışı")
        if (idx + 1) % 10 == 0:
            time.sleep(0.2)

    log.info(f"Tarama tamamlandı. {len(results)} coin eşiği aştı.")

    if results:
        results.sort(key=lambda x: x["ratio"], reverse=True)
        message = build_message(results, scan_time)
        # log.info(f"Telegram mesajı =>  {message}")
        send_to_all(subscribers, message)
    else:
        log.info("Eşiği aşan coin bulunamadı.")


def main() -> None:
    log.info("=" * 50)
    log.info("  Binance Hacim Tarayıcı Botu Başlatıldı")
    log.info(f"  Zaman dilimi  : {INTERVAL}")
    log.info(f"  Tarama aralığı: Her {SCAN_PERIOD_MIN} dakika")
    log.info(f"  Hacim eşiği   : %{int((VOLUME_MULTIPLIER-1)*100)} artış")
    log.info("=" * 50)
    log.info("📢 Bota /start yazarak abone olunabilir!")

    subscribers = load_subscribers()
    log.info(f"Mevcut abone sayısı: {len(subscribers)}")

    last_update_id = 0
    next_scan_time = time.time()

    while True:
        # /start komutlarını sürekli dinle
        last_update_id = process_updates(subscribers, last_update_id)

        # Tarama zamanı geldiyse çalıştır
        if time.time() >= next_scan_time:
            try:
                run_scan(subscribers)
            except Exception as e:
                log.error(f"Tarama hatası: {e}")
            next_scan_time = time.time() + (SCAN_PERIOD_MIN * 60)
            log.info(f"Sonraki tarama {SCAN_PERIOD_MIN} dakika sonra...")

        time.sleep(3)


if __name__ == "__main__":
    main()