# main.py — Купон-бот: Admitad + CityAds + HTML
# Python 3.13.4 ; aiogram 3.22.0 ; SQLite + APScheduler
import os
import re
import json
import html
import time
import asyncio
import logging
import sqlite3
import threading
import datetime
from typing import Optional, List, Dict, Any

import requests
import feedparser
from bs4 import BeautifulSoup

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ---------- ЛОГИ ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------- КОНФИГ ----------
def _sanitize_token(raw: str) -> str:
    if not raw:
        return ""
    # убрать BOM/невидимые и пробелы по краям/кавычки
    to_strip = {
        "\u200b": None, "\u200c": None, "\u200d": None, "\u200e": None, "\u200f": None,
        "\ufeff": None
    }
    raw = raw.translate(to_strip).strip()
    # если завернули в кавычки — снимем
    if (raw.startswith('"') and raw.endswith('"')) or (raw.startswith("'") and raw.endswith("'")):
        raw = raw[1:-1].strip()
    return raw

def _valid_token(t: str) -> bool:
    return bool(re.fullmatch(r"\d+:[A-Za-z0-9_-]{30,}", t or ""))

BOT_TOKEN = _sanitize_token(os.environ.get("BOT_TOKEN", ""))

TIMEZONE = os.environ.get("TIMEZONE", "Europe/Moscow")
DB_PATH = os.environ.get("DB_PATH", "/data/halyava.db")
TRIAL_DAYS = int(os.environ.get("TRIAL_DAYS", "3"))
MONTHLY_PRICE_RUB = int(os.environ.get("MONTHLY_PRICE_RUB", "249"))

# Admitad
ADMITAD_ACCESS_TOKEN = os.environ.get("ADMITAD_ACCESS_TOKEN", "") or ""
ADMITAD_CLIENT_ID = os.environ.get("ADMITAD_CLIENT_ID", "") or ""
ADMITAD_CLIENT_SECRET = os.environ.get("ADMITAD_CLIENT_SECRET", "") or ""
ADMITAD_WEBSITE_ID = os.environ.get("ADMITAD_WEBSITE_ID", "") or ""

# CityAds
CITYADS_COUPONS_URL = os.environ.get("CITYADS_COUPONS_URL", "") or ""

# Официальные промо-страницы магазинов (через запятую в ENV)
OFFICIAL_PROMO_PAGES = [
    u.strip() for u in (os.environ.get("OFFICIAL_PROMO_PAGES", "") or "").split(",") if u.strip()
]

# Популярные магазины РФ + маркеты + еда (алиасы → slug)
STORE_ALIASES: Dict[str, str] = {
    # marketplaces
    "ozon": "ozon", "озон": "ozon",
    "wildberries": "wildberries", "вб": "wildberries", "wb": "wildberries",
    "яндекс маркет": "yandex_market", "я маркет": "yandex_market", "ym": "yandex_market",
    "aliexpress": "aliexpress", "алиэкспресс": "aliexpress",
    # электроника
    "mvideo": "mvideo", "мвидео": "mvideo",
    "eldorado": "eldorado", "эльдорадо": "eldorado",
    "dns": "dns_shop", "днс": "dns_shop",
    "citilink": "citilink", "ситилинк": "citilink",
    "svyaznoy": "svyaznoy", "связной": "svyaznoy",
    "technopark": "technopark", "технопарк": "technopark",
    # одежда
    "lamoda": "lamoda", "ла мода": "lamoda",
    "asos": "asos",
    "hm": "hm", "h&m": "hm",
    # еда/доставка
    "яндекс еда": "yandex_eats", "yandex eats": "yandex_eats",
    "delivery club": "delivery_club", "деливери клаб": "delivery_club",
    "самокат": "samokat", "samokat": "samokat",
    "vkusvill": "vkusvill", "вкусвилл": "vkusvill",
    "lenta": "lenta", "лента": "lenta",
    "perekrestok": "perekrestok", "перекресток": "perekrestok",
}
POPULAR_STORES = sorted(set(STORE_ALIASES.values()))

# ---------- БД ----------
_DB_CONN: Optional[sqlite3.Connection] = None
_DB_LOCK = threading.RLock()

def db() -> sqlite3.Connection:
    global _DB_CONN
    if _DB_CONN is None:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        _DB_CONN = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10.0)
        _DB_CONN.row_factory = sqlite3.Row
        with _DB_CONN:
            _DB_CONN.execute("PRAGMA journal_mode=WAL;")
            _DB_CONN.execute("PRAGMA busy_timeout=5000;")
            _DB_CONN.execute("PRAGMA synchronous=NORMAL;")
    return _DB_CONN

def init_db():
    schema = """
    CREATE TABLE IF NOT EXISTS users(
      user_id INTEGER PRIMARY KEY,
      username TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS subscriptions(
      user_id INTEGER PRIMARY KEY,
      status TEXT,   -- trial|active|expired
      until TEXT,
      plan TEXT,
      updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS deals(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      store_slug TEXT,
      title TEXT,
      description TEXT,
      url TEXT,
      coupon_code TEXT,
      price_old REAL,
      price_new REAL,
      cashback REAL,
      start_at TEXT,
      end_at TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      source TEXT,
      score REAL DEFAULT 0,
      hash TEXT UNIQUE
    );
    CREATE INDEX IF NOT EXISTS idx_deals_store ON deals(store_slug);
    CREATE INDEX IF NOT EXISTS idx_deals_end ON deals(end_at);
    """
    with _DB_LOCK:
        conn = db()
        conn.executescript(schema)
        conn.commit()

def _now_iso_naive_utc() -> str:
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None, microsecond=0).isoformat()

def upsert_user(user_id:int, username:str=""):
    with _DB_LOCK:
        conn = db()
        conn.execute("INSERT OR IGNORE INTO users(user_id, username) VALUES(?,?)", (user_id, username or ""))
        conn.commit()

def get_sub(user_id:int) -> Optional[dict]:
    with _DB_LOCK:
        conn = db()
        r = conn.execute("SELECT status, until FROM subscriptions WHERE user_id=?", (user_id,)).fetchone()
        return dict(r) if r else None

def set_sub(user_id:int, status:str, until_iso:str, plan:str="monthly"):
    with _DB_LOCK:
        conn = db()
        conn.execute("""
            INSERT INTO subscriptions(user_id,status,until,plan,updated_at)
            VALUES(?,?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
              status=excluded.status,
              until=excluded.until,
              plan=excluded.plan,
              updated_at=excluded.updated_at
        """, (user_id, status, until_iso, plan, _now_iso_naive_utc()))
        conn.commit()

def sub_active(user_id:int) -> bool:
    sub = get_sub(user_id)
    if not sub:
        return False
    try:
        return datetime.datetime.fromisoformat(sub["until"]) >= datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    except Exception:
        return False

def grant_trial(user_id:int, days:int=TRIAL_DAYS) -> str:
    until = (datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) + datetime.timedelta(days=days)).replace(microsecond=0).isoformat()
    set_sub(user_id, "trial", until)
    return until

def grant_month(user_id:int, months:int=1) -> str:
    until = (datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) + datetime.timedelta(days=30*months)).replace(microsecond=0).isoformat()
    set_sub(user_id, "active", until)
    return until

def _hash_deal(url:str, title:str, code:str) -> str:
    import hashlib
    return hashlib.sha256((url + "|" + title + "|" + (code or "")).encode("utf-8")).hexdigest()

def put_deals_bulk(deals:List[Dict[str,Any]]) -> int:
    if not deals:
        return 0
    keys = ["store_slug","title","description","url","coupon_code","price_old","price_new",
            "cashback","start_at","end_at","source","score","hash"]
    qmarks = ",".join(["?"]*len(keys))
    rows = []
    for d in deals:
        d["hash"] = _hash_deal(d.get("url",""), d.get("title",""), d.get("coupon_code",""))
        rows.append([d.get(k) for k in keys])
    inserted = 0
    with _DB_LOCK:
        conn = db()
        for row in rows:
            try:
                conn.execute(f"INSERT INTO deals({','.join(keys)}) VALUES({qmarks})", row)
                inserted += 1
            except sqlite3.IntegrityError:
                pass
        conn.commit()
    return inserted

def search_deals(store_slug:str, limit:int=8) -> List[dict]:
    with _DB_LOCK:
        conn = db()
        cur = conn.execute(
            """
            SELECT * FROM deals
            WHERE store_slug=?
              AND (end_at IS NULL OR end_at >= ?)
            ORDER BY
                score DESC,
                CASE WHEN end_at IS NULL THEN 1 ELSE 0 END,
                end_at ASC,
                created_at DESC
            LIMIT ?
            """,
            (store_slug, _now_iso_naive_utc(), limit)
        )
        return [dict(r) for r in cur.fetchall()

        ]

def cleanup_old(days:int=60):
    threshold = (datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(days=days)).isoformat()
    with _DB_LOCK:
        conn = db()
        conn.execute("DELETE FROM deals WHERE (end_at IS NOT NULL AND end_at < ?) OR created_at < ?", (threshold, threshold))
        conn.commit()

# ---------- УТИЛИТЫ ----------
UA = {"User-Agent": "Mozilla/5.0 (compatible; HalyavaBot/1.2)"}

def slug_for_query(q:str) -> Optional[str]:
    q = q.strip().lower()
    return STORE_ALIASES.get(q) or (q if q in POPULAR_STORES else None)

def esc(s:str) -> str:
    return html.escape(s or "")

# ---------- ИСТОЧНИКИ ----------
# 1) Admitad Coupons
_admitad_cached_token: Dict[str, Any] = {"value": ADMITAD_ACCESS_TOKEN.strip(), "exp": 0}

def admitad_get_token() -> Optional[str]:
    if _admitad_cached_token["value"] and _admitad_cached_token["exp"] > time.time():
        return _admitad_cached_token["value"]
    if ADMITAD_ACCESS_TOKEN.strip():
        _admitad_cached_token["value"] = ADMITAD_ACCESS_TOKEN.strip()
        _admitad_cached_token["exp"] = time.time() + 3600
        return _admitad_cached_token["value"]
    if not (ADMITAD_CLIENT_ID and ADMITAD_CLIENT_SECRET):
        return None
    try:
        resp = requests.post(
            "https://api.admitad.com/token/",
            data={"grant_type": "client_credentials", "scope": "coupons"},
            auth=(ADMITAD_CLIENT_ID, ADMITAD_CLIENT_SECRET),
            timeout=20
        )
        resp.raise_for_status()
        data = resp.json()
        token = data.get("access_token")
        ttl = data.get("expires_in", 3600)
        if token:
            _admitad_cached_token["value"] = token
            _admitad_cached_token["exp"] = time.time() + max(300, int(ttl) - 60)
            return token
    except Exception as e:
        log.warning("[ADMITAD] token error: %s", e)
    return None

def pull_admitad() -> int:
    if not ADMITAD_WEBSITE_ID:
        return 0
    token = admitad_get_token()
    if not token:
        return 0
    url = f"https://api.admitad.com/coupons/website/{ADMITAD_WEBSITE_ID}/"
    params = {"limit": 500, "language": "ru", "region": "RU", "status": "active", "ordering": "-date_end"}
    headers = {"Authorization": f"Bearer {token}"}
    added = 0
    try:
        log.info("[SRC][ADMITAD] %s", url)
        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        js = r.json()
        results = js.get("results") or []
        out = []
        for it in results:
            campaign = (it.get("campaign") or {}).get("name") or ""
            code = it.get("promocode") or ""
            title = it.get("short_name") or it.get("code") or campaign
            desc = it.get("description") or ""
            start_at = it.get("date_start")
            end_at = it.get("date_end")
            link = it.get("tracked_link") or it.get("goto_link") or it.get("link") or ""

            store_slug = None
            c = campaign.lower()
            for k, v in STORE_ALIASES.items():
                if k in c:
                    store_slug = v
                    break
            if not store_slug:
                store_slug = re.sub(r"[^a-z0-9]+", "_", campaign.lower()).strip("_") or "unknown"

            score = 1.0 + (0.5 if code else 0) + (0.2 if end_at else 0)
            out.append(dict(
                store_slug=store_slug, title=title, description=desc, url=link, coupon_code=code,
                price_old=None, price_new=None, cashback=None,
                start_at=start_at, end_at=end_at, source="admitad", score=score
            ))
        added += put_deals_bulk(out)
    except Exception as e:
        log.error("[ADMITAD] error: %s", e)
    return added

# 2) CityAds feed (JSON/XML)
def pull_cityads() -> int:
    if not CITYADS_COUPONS_URL:
        return 0
    try:
        log.info("[SRC][CITYADS] %s", CITYADS_COUPONS_URL)
        r = requests.get(CITYADS_COUPONS_URL, timeout=30, headers=UA)
        r.raise_for_status()
        added = 0
        out: List[Dict[str,Any]] = []
        content_type = r.headers.get("Content-Type","").lower()
        if "json" in content_type or CITYADS_COUPONS_URL.endswith(".json"):
            data = r.json()
            items = data.get("coupons") or data.get("items") or data
            for it in items if isinstance(items, list) else []:
                store = (it.get("campaign") or it.get("advertiser") or {}).get("name") or it.get("shop") or ""
                code = it.get("code") or it.get("coupon") or ""
                title = it.get("title") or it.get("name") or store
                desc = it.get("description") or ""
                link = it.get("url") or it.get("link") or ""
                start_at = it.get("start_date") or it.get("start") or None
                end_at = it.get("end_date") or it.get("end") or None
                store_slug = None
                s = (store or "").lower()
                for k, v in STORE_ALIASES.items():
                    if k in s:
                        store_slug = v
                        break
                if not store_slug:
                    store_slug = re.sub(r"[^a-z0-9]+","_", s).strip("_") or "unknown"
                out.append(dict(
                    store_slug=store_slug, title=title, description=desc, url=link, coupon_code=code,
                    price_old=None, price_new=None, cashback=None,
                    start_at=start_at, end_at=end_at, source="cityads", score=0.9 + (0.4 if code else 0)
                ))
        else:
            feed = feedparser.parse(r.text)
            for e in feed.entries:
                title = (e.get("title") or "").strip()
                link = e.get("link") or ""
                summary = (e.get("summary") or "").strip()
                code_match = re.search(r"(?:promo|код|code)\s*[:\- ]\s*([A-Z0-9\-]{4,16})", summary, re.IGNORECASE)
                code = code_match.group(1) if code_match else ""
                store_slug = "unknown"
                for k, v in STORE_ALIASES.items():
                    if k in (title + " " + summary).lower():
                        store_slug = v
                        break
                out.append(dict(
                    store_slug=store_slug, title=title, description=summary, url=link, coupon_code=code,
                    price_old=None, price_new=None, cashback=None,
                    start_at=None, end_at=None, source="cityads_rss", score=0.7 + (0.3 if code else 0)
                ))
        added += put_deals_bulk(out)
        return added
    except Exception as e:
        log.error("[CITYADS] error: %s", e)
        return 0

# 3) Официальные промо-страницы
def pull_official_pages() -> int:
    if not OFFICIAL_PROMO_PAGES:
        return 0
    out: List[Dict[str,Any]] = []
    for url in OFFICIAL_PROMO_PAGES:
        try:
            log.info("[SRC][PROMO_PAGE] %s", url)
            r = requests.get(url, timeout=20, headers=UA)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            texts = soup.get_text(" ", strip=True)
            for m in re.finditer(r"(?:промокод|код)\s*[:\- ]\s*([A-Z0-9\-]{4,16})", texts, re.IGNORECASE):
                code = m.group(1)
                title = "Промокод"
                desc = "Официальная промо-страница"
                host = re.sub(r"^https?://", "", url).split("/")[0].lower()
                store_slug = "unknown"
                for k, v in STORE_ALIASES.items():
                    if k in host or k.replace(" ", "") in host:
                        store_slug = v
                        break
                out.append(dict(
                    store_slug=store_slug, title=title, description=desc, url=url, coupon_code=code,
                    price_old=None, price_new=None, cashback=None,
                    start_at=None, end_at=None, source="official_page", score=0.6
                ))
        except Exception as e:
            log.warning("[PROMO_PAGE] %s error: %s", url, e)
    return put_deals_bulk(out)

# Сбор всех источников
def run_all_sources() -> int:
    total = 0
    total += pull_admitad()
    total += pull_cityads()
    total += pull_official_pages()
    log.info("[SCRAPE] total added: %s", total)
    return total

# ---------- ФОРМАТИРОВАНИЕ ----------
def fmt_deal(d:dict) -> str:
    lines = []
    lines.append(f"🛍 {esc(d.get('store_slug') or 'магазин')} — {esc(d.get('title') or '')}")
    code = d.get("coupon_code")
    if code:
        lines.append(f"Промокод: <code>{esc(code)}</code>")
    if d.get("price_new"):
        if d.get("price_old"):
            lines.append(f"Цена: {d['price_new']} (было {d['price_old']})")
        else:
            lines.append(f"Цена: {d['price_new']}")
    if d.get("cashback"):
        lines.append(f"Кэшбэк: {d['cashback']}")
    if d.get("end_at"):
        lines.append(f"Действует до: {esc(d['end_at'])}")
    if d.get("description"):
        desc = (d["description"] or "")
        if len(desc) > 160:
            desc = desc[:157] + "…"
        lines.append(esc(desc))
    if d.get("url"):
        lines.append(f"🔗 {esc(d['url'])}")
    return "\n".join(lines)

# ---------- БОТ ----------
router = Router()

@router.message(Command("start"))
async def cmd_start(m: Message):
    log.info("[START] from=%s @%s", m.from_user.id, m.from_user.username)
    upsert_user(m.from_user.id, m.from_user.username or "")
    sub = get_sub(m.from_user.id)
    if not sub:
        till = grant_trial(m.from_user.id, TRIAL_DAYS)
        await m.answer(
            f"Привет! Включил бесплатный триал до {esc(till)}.\n"
            f"Команды: /search <магазин>, /stores, /profile, /buy, /redeem <код>, /help",
            disable_web_page_preview=True
        )
    else:
        await m.answer("Снова здесь! Попробуй: /search ozon", disable_web_page_preview=True)

@router.message(Command("help"))
async def cmd_help(m: Message):
    await m.answer(
        "Команды:\n"
        "• /search <магазин> — найти актуальные промо\n"
        "• /stores — список магазинов\n"
        "• /profile — статус подписки\n"
        f"• /buy — оформить подписку ({MONTHLY_PRICE_RUB}₽/мес)\n"
        "• /redeem <код> — активировать промокод подписки\n"
        "• /update — обновить источники вручную\n",
        disable_web_page_preview=True
    )

@router.message(Command("stores"))
async def cmd_stores(m: Message):
    await m.answer("Популярные магазины:\n" + "\n".join("• " + s for s in POPULAR_STORES))

@router.message(Command("profile"))
async def cmd_profile(m: Message):
    sub = get_sub(m.from_user.id)
    if not sub:
        return await m.answer("Статус: нет подписки. /buy — оформить (249₽/мес).")
    await m.answer(f"Статус: {esc(sub['status'])} до {esc(sub['until'])}")

@router.message(Command("buy"))
async def cmd_buy(m: Message):
    await m.answer(
        f"Подписка {MONTHLY_PRICE_RUB}₽/мес.\n"
        "На MVP доступна активация через промокод администратора: /redeem КОД.\n"
        "Позже прикрутим оплату.",
        disable_web_page_preview=True
    )

@router.message(Command("redeem"))
async def cmd_redeem(m: Message):
    parts = (m.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        return await m.answer("Формат: /redeem КОД")
    # mvp: активируем месяц без проверки кода
    until = grant_month(m.from_user.id, 1)
    await m.answer(f"Подписка активна до {esc(until)}. /profile — проверить")

@router.message(Command("update"))
async def cmd_update(m: Message):
    await m.answer("Собираю источники…")
    added = run_all_sources()
    await m.answer(f"Готово. Добавлено: {added}")

@router.message(Command("search"))
async def cmd_search(m: Message):
    log.info("[SEARCH] from=%s text=%r", m.from_user.id, m.text)
    args = (m.text or "").split()[1:]
    if not args:
        return await m.answer("Формат: /search магазин\nПример: /search ozon")

    if not sub_active(m.from_user.id):
        return await m.answer("Нужна активная подписка. /buy — оформить (в /start есть триал)")

    store_slug = slug_for_query(" ".join(args))
    if not store_slug:
        return await m.answer("Не узнал магазин. Посмотри /stores и попробуй ещё раз.")

    results = search_deals(store_slug, limit=8)
    if not results:
        await m.answer("По этому магазину пока пусто. Запрашиваю источники…")
        run_all_sources()
        results = search_deals(store_slug, limit=8)
        if not results:
            return await m.answer("Пока ничего не нашли. Загляни позже или попробуй другой магазин.")

    for d in results:
        try:
            await m.answer(fmt_deal(d), disable_web_page_preview=True)
        except Exception:
            await m.answer(re.sub(r"<.*?>", "", fmt_deal(d)), disable_web_page_preview=True)

# ---------- ПЛАНИРОВЩИК ----------
scheduler: Optional[AsyncIOScheduler] = None

async def scrape_job():
    try:
        added = run_all_sources()
        log.info("[SCRAPER] added: %s", added)
    except Exception as e:
        log.error("[SCRAPER] error: %s", e)

async def cleanup_job():
    try:
        cleanup_old(60)
        log.info("[CLEANUP] done")
    except Exception as e:
        log.error("[CLEANUP] error: %s", e)

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("Set BOT_TOKEN env var")
    if not _valid_token(BOT_TOKEN):
        # Поясняем, что именно не так
        masked = (BOT_TOKEN[:4] + "…" + BOT_TOKEN[-6:]) if BOT_TOKEN else "(empty)"
        raise RuntimeError(
            "BOT_TOKEN выглядит некорректно. Дай токен целиком в формате <id:secret>, "
            "например 8284074356:AAE… (без кавычек/пробелов). Сейчас: " + masked
        )

    init_db()

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)

    global scheduler
    scheduler = AsyncIOScheduler(timezone=TIMEZONE)
    scheduler.add_job(scrape_job, "interval", minutes=30, id="scrape")
    scheduler.add_job(cleanup_job, "interval", hours=12, id="cleanup")
    scheduler.start()

    loop = asyncio.get_event_loop()
    loop.call_later(10, lambda: asyncio.create_task(scrape_job()))

    log.info("Start polling")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
