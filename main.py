# main.py — AI Halyava Bot (Py 3.13.4, aiogram 3.22)
# One-file, long-polling; SQLite + APScheduler; автопарсер промо без lxml
import os, sqlite3, datetime, hashlib, json, asyncio, logging, re
from typing import Optional, List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import feedparser
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message, LinkPreviewOptions
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ======== LOGGING ========
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("halyava")

# ======== CONFIG ========
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
TRIAL_DAYS = int(os.environ.get("TRIAL_DAYS", "3"))
MONTHLY_PRICE_RUB = int(os.environ.get("MONTHLY_PRICE_RUB", "249"))
DB_PATH = os.environ.get("DB_PATH", "/data/halyava.db")
TIMEZONE = os.environ.get("TIMEZONE", "Europe/Moscow")

# Если в ENV нет STORES_JSON — используем дефолт ниже (валидный JSON без комментариев)
STORES_JSON = os.environ.get("STORES_JSON")

DEFAULT_STORES_JSON = """
{
  "stores": [
    { "type":"auto","store":"ozon","category":"акции","url":"https://www.ozon.ru/info/actions/" },
    { "type":"auto","store":"wb","category":"акции","url":"https://www.wildberries.ru/promotions" },
    { "type":"auto","store":"yandexmarket","category":"акции","url":"https://market.yandex.ru/specials" },
    { "type":"auto","store":"sbermegamarket","category":"акции","url":"https://sbermegamarket.ru/actions/" },

    { "type":"auto","store":"mvideo","category":"акции","url":"https://www.mvideo.ru/promo" },
    { "type":"auto","store":"eldorado","category":"акции","url":"https://www.eldorado.ru/promo/" },
    { "type":"auto","store":"dns","category":"акции","url":"https://www.dns-shop.ru/actions/" },
    { "type":"auto","store":"citilink","category":"акции","url":"https://www.citilink.ru/promo/" },
    { "type":"auto","store":"technopark","category":"акции","url":"https://www.technopark.ru/promo/" },

    { "type":"auto","store":"lamoda","category":"акции","url":"https://www.lamoda.ru/promo/" },
    { "type":"auto","store":"sportmaster","category":"акции","url":"https://www.sportmaster.ru/actions/" },

    { "type":"auto","store":"letual","category":"акции","url":"https://www.letu.ru/promo" },
    { "type":"auto","store":"rivegauche","category":"акции","url":"https://www.rivegauche.ru/promo" },

    { "type":"auto","store":"apteka","category":"акции","url":"https://apteka.ru/discounts" },
    { "type":"auto","store":"rigla","category":"акции","url":"https://www.rigla.ru/actions/" },
    { "type":"auto","store":"aptekamos","category":"акции","url":"https://www.apteka-mos.ru/actions/" },

    { "type":"auto","store":"vkusvill","category":"акции","url":"https://vkusvill.ru/akcii/" },
    { "type":"auto","store":"perekrestok","category":"акции","url":"https://www.perekrestok.ru/cat/akcii" },
    { "type":"auto","store":"magnit","category":"акции","url":"https://magnit.ru/promo/" },
    { "type":"auto","store":"lenta","category":"акции","url":"https://lenta.com/promo/" },
    { "type":"auto","store":"auchan","category":"акции","url":"https://www.auchan.ru/promo/" },
    { "type":"auto","store":"okey","category":"акции","url":"https://www.okmarket.ru/actions/" },
    { "type":"auto","store":"metro","category":"акции","url":"https://www.metro-cc.ru/promo" },
    { "type":"auto","store":"sbermarket","category":"акции","url":"https://sbermarket.ru/actions" },

    { "type":"auto","store":"deliveryclub","category":"акции","url":"https://delivery-club.ru/special" }
  ]
}
"""

# Промокоды для MVP: ENV PROMO_CODES="VIP,TEST1"
PROMO_CODES = {c.strip() for c in os.environ.get("PROMO_CODES", "").split(",") if c.strip()}

# ======== DB LAYER ========
def connect() -> sqlite3.Connection:
    """Новое соединение с безопасными PRAGMA и таймаутом. Используй: with connect() as conn: ..."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=60.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=60000;")
    return conn

def init_db():
    with connect() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS users(
          user_id INTEGER PRIMARY KEY,
          username TEXT,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS subscriptions(
          user_id INTEGER PRIMARY KEY,
          status TEXT,
          until TEXT,
          plan TEXT,
          updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS deals(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          store_slug TEXT,
          category TEXT,
          title TEXT,
          description TEXT,
          url TEXT,
          price_old REAL,
          price_new REAL,
          cashback REAL,
          coupon_code TEXT,
          start_at TEXT,
          end_at TEXT,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP,
          source TEXT,
          hash TEXT UNIQUE,
          score REAL DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_deals_store ON deals(store_slug);
        CREATE INDEX IF NOT EXISTS idx_deals_cat ON deals(category);
        CREATE INDEX IF NOT EXISTS idx_deals_end ON deals(end_at);
        """)

def now_utc_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None, microsecond=0).isoformat()

def upsert_user(user_id:int, username:str=""):
    with connect() as conn:
        conn.execute("INSERT OR IGNORE INTO users(user_id, username) VALUES(?,?)", (user_id, username or ""))

def get_sub(user_id:int) -> Optional[dict]:
    with connect() as conn:
        r = conn.execute("SELECT status, until FROM subscriptions WHERE user_id=?", (user_id,)).fetchone()
    return dict(r) if r else None

def set_sub(user_id:int, status:str, until_iso:str, plan:str="monthly"):
    with connect() as conn:
        conn.execute("""
            INSERT INTO subscriptions(user_id,status,until,plan,updated_at)
            VALUES(?,?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE
            SET status=excluded.status, until=excluded.until, updated_at=excluded.updated_at
        """, (user_id, status, until_iso, plan, now_utc_iso()))

def sub_active(user_id:int) -> bool:
    sub = get_sub(user_id)
    if not sub: return False
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

def put_deal(d:dict) -> bool:
    h = hashlib.sha256((d.get("url","") + d.get("title","")).encode("utf-8")).hexdigest()
    d["hash"] = h
    keys = ["store_slug","category","title","description","url","price_old","price_new","cashback","coupon_code","start_at","end_at","source","score","hash"]
    vals = [d.get(k) for k in keys]
    try:
        with connect() as conn:
            conn.execute(f"INSERT INTO deals({','.join(keys)}) VALUES({','.join(['?']*len(keys))})", vals)
        return True
    except sqlite3.IntegrityError:
        return False

def search_deals(store:Optional[str], category:Optional[str], limit:int=5) -> List[dict]:
    q = "SELECT * FROM deals WHERE 1=1"
    args = []
    if store:
        q += " AND store_slug=?"
        args.append(store)
    if category:
        q += " AND (category=? OR title LIKE ? OR description LIKE ?)"
        args.extend([category, f"%{category}%", f"%{category}%"])
    q += " AND (end_at IS NULL OR end_at>=?)"
    args.append(now_utc_iso())
    q += " ORDER BY score DESC, (end_at IS NULL) ASC, end_at ASC, created_at DESC LIMIT ?"
    args.append(limit)
    with connect() as conn:
        rows = conn.execute(q, tuple(args)).fetchall()
    return [dict(r) for r in rows]

def cleanup_old(ttl_days: int = 14):
    now = now_utc_iso()
    older_than = (datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(days=ttl_days)).replace(microsecond=0).isoformat()
    with connect() as conn:
        deleted = conn.execute(
            "DELETE FROM deals WHERE (end_at IS NOT NULL AND end_at < ?) OR (created_at < ?)",
            (now, older_than),
        ).rowcount
    log.info(f"[CLEANUP] deleted={deleted}")

# ======== SCRAPERS ========
HEADERS = {"User-Agent":"Mozilla/5.0 (compatible; HalyavaBot/1.0)"}
KEYWORDS = ["акци", "скид", "купон", "промо", "распрод", "sale", "%", "выгод", "бонус"]

def scrape_auto(store, category, url) -> int:
    log.info(f"[SCRAPE][AUTO] {store} {url}")
    try:
        r = requests.get(url, timeout=20, headers=HEADERS); r.raise_for_status()
    except Exception as e:
        log.warning(f"[SCRAPE][AUTO] fetch fail {store}: {e}"); return 0
    soup = BeautifulSoup(r.text, "html.parser")
    anchors = soup.find_all("a", href=True)
    added, seen = 0, set()
    for a in anchors[:2000]:
        text = " ".join((a.get_text() or "").split())
        href = urljoin(url, a["href"])
        if not text or href in seen: continue
        if href.startswith("javascript:") or href.startswith("#"): continue
        low = text.lower()
        if not any(k in low for k in KEYWORDS): continue
        if re.search(r"(login|signin|account|lk|cart|support|faq)", href, re.I): continue
        d = dict(store_slug=store, category=category, title=text, description="", url=href,
                 source=url, score=0.8, start_at=None, end_at=None, price_old=None, price_new=None,
                 cashback=None, coupon_code=None)
        if put_deal(d): added += 1
        seen.add(href)
    log.info(f"[SCRAPE][AUTO] added: {added}")
    return added

def scrape_rss(store, category, url) -> int:
    log.info(f"[SCRAPE][RSS] {store} {url}")
    try:
        feed = feedparser.parse(url)
    except Exception as e:
        log.warning(f"[SCRAPE][RSS] parse fail {store}: {e}"); return 0
    added = 0
    for e in feed.entries[:200]:
        title = (e.get("title") or "").strip(); link = e.get("link") or ""; summary = (e.get("summary") or "").strip()
        if not title or not link: continue
        d = dict(store_slug=store, category=category, title=title, description=summary, url=link,
                 source=url, score=0.7, start_at=None, end_at=None, price_old=None, price_new=None,
                 cashback=None, coupon_code=None)
        if put_deal(d): added += 1
    log.info(f"[SCRAPE][RSS] added: {added}")
    return added

def scrape_html_css(store, category, url, item_sel, title_sel, link_sel, desc_sel=None) -> int:
    log.info(f"[SCRAPE][HTML] {store} {url}")
    try:
        r = requests.get(url, timeout=20, headers=HEADERS); r.raise_for_status()
    except Exception as e:
        log.warning(f"[SCRAPE][HTML] fetch fail {store}: {e}"); return 0
    soup = BeautifulSoup(r.text, "html.parser")
    items = soup.select(item_sel)[:200]; added = 0
    for it in items:
        te = it.select_one(title_sel); le = it.select_one(link_sel)
        if not te or not le: continue
        title = " ".join(te.get_text().split()); link = urljoin(url, le.get("href") or "")
        desc = ""
        if desc_sel:
            de = it.select_one(desc_sel)
            if de: desc = " ".join(de.get_text().split())
        d = dict(store_slug=store, category=category, title=title, description=desc, url=link,
                 source=url, score=0.9, start_at=None, end_at=None, price_old=None, price_new=None,
                 cashback=None, coupon_code=None)
        if put_deal(d): added += 1
    log.info(f"[SCRAPE][HTML] added: {added}")
    return added

def run_all_sources() -> int:
    raw = STORES_JSON or DEFAULT_STORES_JSON
    try:
        conf = json.loads(raw)
    except Exception as e:
        log.error(f"[SCRAPE] bad STORES_JSON: {e}"); conf = {"stores":[]}
    total = 0
    for s in conf.get("stores", []):
        t = s.get("type")
        try:
            if t == "rss":
                total += scrape_rss(s["store"], s.get("category","другое"), s["url"])
            elif t == "html_css":
                total += scrape_html_css(
                    s["store"], s.get("category","другое"),
                    s["url"], s["item_selector"], s["title_selector"], s["link_selector"],
                    s.get("desc_selector")
                )
            else:  # auto / auto_html / None
                total += scrape_auto(s["store"], s.get("category","другое"), s["url"])
        except Exception:
            log.exception(f"[SCRAPE] error store={s}")
    log.info(f"[SCRAPE] total added: {total}")
    return total

# ======== BOT ========
router = Router()

def fmt_deal(d:dict) -> str:
    price = ""
    if d.get("price_new"):
        price = f"Цена: {d['price_new']}\n" if not d.get("price_old") else f"Цена: {d['price_new']} (было {d['price_old']})\n"
    cb = f"Кэшбэк: {d['cashback']}\n" if d.get("cashback") else ""
    coup = f"Промокод: <code>{d['coupon_code']}</code>\n" if d.get("coupon_code") else ""
    deadline = f"Дедлайн: {d['end_at']}\n" if d.get("end_at") else ""
    return (
        f"🛒 {d['store_slug']} • {d.get('category') or 'без категории'}\n"
        f"🧩 {d['title']}\n"
        f"{price}{cb}{coup}{deadline}"
        f"🔗 {d['url']}"
    )

@router.message(Command("ping"))
async def cmd_ping(m: Message):
    log.info(f"[PING] from={m.from_user.id}")
    await m.answer("pong")

@router.message(Command("reload"))
async def cmd_reload(m: Message):
    log.info(f"[RELOAD] from={m.from_user.id}")
    cnt = run_all_sources()
    await m.answer(f"Обновил источники. Новых позиций: {cnt}")

@router.message(Command("start"))
async def cmd_start(m: Message):
    log.info(f"[START] from={m.from_user.id} @{m.from_user.username}")
    upsert_user(m.from_user.id, m.from_user.username or "")
    sub = get_sub(m.from_user.id)
    if not sub:
        till = grant_trial(m.from_user.id, TRIAL_DAYS)
        await m.answer(
            "Привет! Включил бесплатный триал до {till}.\n"
            "Команды: /search, /buy, /profile, /stores, /categories, /redeem КОД, /help".format(till=till)
        )
    else:
        await m.answer("Снова ты! Пробуй: /search ozon акции")

@router.message(Command("help"))
async def cmd_help(m: Message):
    log.info(f"[HELP] from={m.from_user.id}")
    await m.answer(
        "Команды:\n"
        "/search &lt;магазин&gt; [категория]\n"
        "/stores\n/categories\n/profile\n"
        "/buy — подписка\n/redeem &lt;код&gt; — промокод\n"
        "/reload — обновить источники\n/ping — проверка связи"
    )

@router.message(Command("profile"))
async def cmd_profile(m: Message):
    log.info(f"[PROFILE] from={m.from_user.id}")
    sub = get_sub(m.from_user.id)
    if not sub:
        await m.answer("Статус: нет подписки. /buy — оформить (249₽/мес)")
    else:
        await m.answer(f"Статус: {sub['status']} до {sub['until']}")

@router.message(Command("buy"))
async def cmd_buy(m: Message):
    log.info(f"[BUY] from={m.from_user.id}")
    await m.answer(
        f"Подписка {MONTHLY_PRICE_RUB}₽/мес.\n"
        f"На MVP — промокод от админа: /redeem КОД\n"
        f"(Позже подключим оплату через Stars/CryptoBot)."
    )

@router.message(Command("redeem"))
async def cmd_redeem(m: Message):
    log.info(f"[REDEEM] from={m.from_user.id} text={m.text!r}")
    parts = m.text.split(maxsplit=1)
    if len(parts) < 2:
        return await m.answer("Формат: /redeem &lt;код&gt;")
    code = parts[1].strip()
    if not code:
        return await m.answer("Пустой код.")
    if PROMO_CODES and code not in PROMO_CODES:
        return await m.answer("Код неверный.")
    until = grant_month(m.from_user.id, 1)
    await m.answer(f"Подписка активна до {until}. /profile — проверить")

@router.message(Command("stores"))
async def cmd_stores(m: Message):
    log.info(f"[STORES] from={m.from_user.id}")
    with connect() as conn:
        r = conn.execute("SELECT DISTINCT store_slug FROM deals ORDER BY store_slug").fetchall()
    if not r:
        return await m.answer("Пока пусто. Нажми /reload, затем /search.")
    await m.answer("Магазины:\n" + "\n".join("• "+x["store_slug"] for x in r))

@router.message(Command("categories"))
async def cmd_categories(m: Message):
    log.info(f"[CATS] from={m.from_user.id}")
    with connect() as conn:
        r = conn.execute("SELECT DISTINCT COALESCE(category,'—') c FROM deals ORDER BY c").fetchall()
    if not r:
        return await m.answer("Пока пусто. Нажми /reload, затем /search.")
    await m.answer("Категории:\n" + "\n".join("• "+x["c"] for x in r))

@router.message(Command("search"))
async def cmd_search(m: Message):
    log.info(f"[SEARCH] from={m.from_user.id} text={m.text!r}")
    try:
        args = m.text.split()[1:]
        if not args:
            return await m.answer("Формат: /search &lt;магазин&gt; [категория]\nНапример: /search ozon акции")
        store = args[0].lower()
        category = args[1].lower() if len(args) > 1 else None

        if not sub_active(m.from_user.id):
            return await m.answer("Нужна активная подписка. /buy — оформить (есть триал в /start)")

        results = search_deals(store, category, limit=5)
        if not results:
            return await m.answer("Пока пусто. Нажми /reload, подожди 5–10 сек и попробуй снова.")

        for d in results:
            await m.answer(fmt_deal(d), link_preview_options=LinkPreviewOptions(is_disabled=True))
    except Exception as e:
        log.exception("[SEARCH] handler error")
        await m.answer(f"Ошибка поиска: {e!s}")

# ======== SCHEDULER ========
scheduler: Optional[AsyncIOScheduler] = None

async def scrape_job():
    try:
        cnt = run_all_sources()
        log.info(f"[SCRAPER] added: {cnt}")
    except Exception:
        log.exception("[SCRAPER] error")

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("Set BOT_TOKEN env var")
    init_db()
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)
    global scheduler
    scheduler = AsyncIOScheduler(timezone=ZoneInfo(TIMEZONE))
    scheduler.add_job(scrape_job, "interval", minutes=30, id="scrape")
    scheduler.add_job(cleanup_old, "cron", hour=3, minute=0, id="cleanup")
    scheduler.start()
    loop = asyncio.get_running_loop()
    loop.call_later(5, lambda: asyncio.create_task(scrape_job()))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
  
