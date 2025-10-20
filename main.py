# main.py — AI Halyava Bot (one-file)
# Python 3.10+ ; aiogram v3 ; long-polling ; SQLite + APScheduler
import os, sqlite3, datetime, hashlib, json, asyncio
from typing import Optional, List
import requests
from bs4 import BeautifulSoup
import feedparser
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ======== CONFIG ========
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
TRIAL_DAYS = int(os.environ.get("TRIAL_DAYS", "3"))
MONTHLY_PRICE_RUB = int(os.environ.get("MONTHLY_PRICE_RUB", "249"))
DB_PATH = os.environ.get("DB_PATH", "/data/halyava.db")
TIMEZONE = os.environ.get("TIMEZONE", "Europe/Moscow")

# JSON строка с источниками. Пример ниже.
STORES_JSON = os.environ.get("STORES_JSON", """
{
  "stores": [
    {
      "type": "rss",
      "store": "example",
      "category": "подписки",
      "url": "https://planetpython.org/rss20.xml"
    }
  ]
}
""")

# Промокоды для MVP (через ENV, список через запятую). Пример: "VIP,TEST1,OCT"
PROMO_CODES = set([c.strip() for c in os.environ.get("PROMO_CODES","").split(",") if c.strip()])

# Админы (ID через пробел) для будущих улучшений
ADMINS = {int(x) for x in os.environ.get("ADMINS","").split() if x.isdigit()}

# ======== DB LAYER ========
def db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = db()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS users(
      user_id INTEGER PRIMARY KEY,
      username TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS subscriptions(
      user_id INTEGER PRIMARY KEY,
      status TEXT,   -- trial|active|expired
      until TEXT,    -- ISO date
      plan TEXT,     -- monthly
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
    conn.commit()

def now_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat()

def upsert_user(user_id:int, username:str=""):
    conn = db()
    conn.execute("INSERT OR IGNORE INTO users(user_id, username) VALUES(?,?)", (user_id, username or ""))
    conn.commit()

def get_sub(user_id:int) -> Optional[dict]:
    conn = db()
    r = conn.execute("SELECT status, until FROM subscriptions WHERE user_id=?", (user_id,)).fetchone()
    return dict(r) if r else None

def set_sub(user_id:int, status:str, until_iso:str, plan:str="monthly"):
    conn = db()
    conn.execute("""
        INSERT INTO subscriptions(user_id,status,until,plan,updated_at)
        VALUES(?,?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET status=excluded.status, until=excluded.until, updated_at=excluded.updated_at
    """, (user_id, status, until_iso, plan, now_iso()))
    conn.commit()

def sub_active(user_id:int) -> bool:
    sub = get_sub(user_id)
    if not sub: return False
    try:
        return datetime.datetime.fromisoformat(sub["until"]) >= datetime.datetime.utcnow()
    except: return False

def grant_trial(user_id:int, days:int=TRIAL_DAYS) -> str:
    until = (datetime.datetime.utcnow() + datetime.timedelta(days=days)).replace(microsecond=0).isoformat()
    set_sub(user_id, "trial", until)
    return until

def grant_month(user_id:int, months:int=1) -> str:
    until = (datetime.datetime.utcnow() + datetime.timedelta(days=30*months)).replace(microsecond=0).isoformat()
    set_sub(user_id, "active", until)
    return until

def put_deal(d:dict) -> bool:
    conn = db()
    h = hashlib.sha256((d.get("url","") + d.get("title","")).encode("utf-8")).hexdigest()
    d["hash"] = h
    keys = ["store_slug","category","title","description","url","price_old","price_new","cashback","coupon_code","start_at","end_at","source","score","hash"]
    vals = [d.get(k) for k in keys]
    try:
        conn.execute(f"INSERT INTO deals({','.join(keys)}) VALUES({','.join(['?']*len(keys))})", vals)
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

def search_deals(store:Optional[str], category:Optional[str], limit:int=5) -> List[dict]:
    conn = db()
    q = "SELECT * FROM deals WHERE 1=1"
    args = []
    if store:
        q += " AND store_slug=?"
        args.append(store)
    if category:
        q += " AND (category=? OR title LIKE ? OR description LIKE ?)"
        args.extend([category, f"%{category}%", f"%{category}%"])
    q += " AND (end_at IS NULL OR end_at>=?)"
    args.append(now_iso())
    q += " ORDER BY score DESC, (end_at IS NULL) ASC, end_at ASC, created_at DESC LIMIT ?"
    args.append(limit)
    cur = conn.execute(q, tuple(args))
    return [dict(r) for r in cur.fetchall()]

# ======== SCRAPERS ========
HEADERS = {"User-Agent":"Mozilla/5.0 (compatible; HalyavaBot/1.0)"}

def scrape_rss(store, category, url) -> int:
    feed = feedparser.parse(url)
    added = 0
    for e in feed.entries[:100]:
        title = (e.get("title") or "").strip()
        link  = e.get("link") or ""
        summary = (e.get("summary") or "").strip()
        if not title or not link: 
            continue
        d = dict(store_slug=store, category=category, title=title, description=summary, url=link, source=url, score=1.0, start_at=None, end_at=None, price_old=None, price_new=None, cashback=None, coupon_code=None)
        if put_deal(d):
            added += 1
    return added

def scrape_html_css(store, category, url, item_sel, title_sel, link_sel, desc_sel=None) -> int:
    r = requests.get(url, timeout=20, headers=HEADERS)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")  # без lxml
    items = soup.select(item_sel)[:100]
    added = 0
    for it in items:
        te = it.select_one(title_sel)
        le = it.select_one(link_sel)
        if not te or not le: 
            continue
        title = " ".join(te.get_text().split())
        link  = le.get("href") or ""
        desc  = ""
        if desc_sel:
            de = it.select_one(desc_sel)
            if de: desc = " ".join(de.get_text().split())
        d = dict(store_slug=store, category=category, title=title, description=desc, url=link, source=url, score=0.8, start_at=None, end_at=None, price_old=None, price_new=None, cashback=None, coupon_code=None)
        if put_deal(d):
            added += 1
    return added

def run_all_sources() -> int:
    try:
        conf = json.loads(STORES_JSON)
    except Exception:
        conf = {"stores":[]}
    total = 0
    for s in conf.get("stores", []):
        t = s.get("type")
        if t == "rss":
            total += scrape_rss(s["store"], s.get("category","другое"), s["url"])
        elif t == "html_css":
            total += scrape_html_css(
                s["store"], s.get("category","другое"),
                s["url"], s["item_selector"], s["title_selector"], s["link_selector"],
                s.get("desc_selector")
            )
    return total

# ======== BOT ========
router = Router()

def fmt_deal(d:dict) -> str:
    price = ""
    if d.get("price_new"):
        if d.get("price_old"):
            price = f"Цена: {d['price_new']} (было {d['price_old']})\n"
        else:
            price = f"Цена: {d['price_new']}\n"
    cb = f"Кэшбэк: {d['cashback']}\n" if d.get("cashback") else ""
    coup = f"Промокод: `{d['coupon_code']}`\n" if d.get("coupon_code") else ""
    deadline = f"Дедлайн: {d['end_at']}\n" if d.get("end_at") else ""
    return (f"🛒 {d['store_slug']} • {d.get('category') or 'без категории'}\n"
            f"🧩 {d['title']}\n"
            f"{price}{cb}{coup}{deadline}"
            f"🔗 {d['url']}")

@router.message(Command("start"))
async def cmd_start(m: Message):
    upsert_user(m.from_user.id, m.from_user.username or "")
    sub = get_sub(m.from_user.id)
    if not sub:
        till = grant_trial(m.from_user.id, TRIAL_DAYS)
        await m.answer(f"Привет! Включил бесплатный триал до {till}.\nКоманды: /search, /buy, /profile, /stores, /categories, /redeem КОД, /help")
    else:
        await m.answer("Снова ты! Пробуй: /search example подписки")

@router.message(Command("help"))
async def cmd_help(m: Message):
    await m.answer("Команды:\n/search <магазин> [категория]\n/stores\n/categories\n/profile\n/buy — подписка\n/redeem <код> — промокод")

@router.message(Command("profile"))
async def cmd_profile(m: Message):
    sub = get_sub(m.from_user.id)
    if not sub:
        await m.answer("Статус: нет подписки. /buy — оформить (249₽/мес)")
    else:
        await m.answer(f"Статус: {sub['status']} до {sub['until']}")

@router.message(Command("buy"))
async def cmd_buy(m: Message):
    await m.answer(f"Подписка {MONTHLY_PRICE_RUB}₽/мес.\nНа MVP — промокод от админа: /redeem КОД\n(Позже подключим оплату через Stars/CryptoBot).")

@router.message(Command("redeem"))
async def cmd_redeem(m: Message):
    parts = m.text.split(maxsplit=1)
    if len(parts) < 2:
        return await m.answer("Формат: /redeem КОД")
    code = parts[1].strip()
    if not code:
        return await m.answer("Пустой код.")
    if PROMO_CODES and code not in PROMO_CODES:
        return await m.answer("Код неверный.")
    until = grant_month(m.from_user.id, 1)
    await m.answer(f"Подписка активна до {until}. /profile — проверить")

@router.message(Command("stores"))
async def cmd_stores(m: Message):
    conn = db()
    r = conn.execute("SELECT DISTINCT store_slug FROM deals ORDER BY store_slug").fetchall()
    if not r:
        return await m.answer("Пока пусто. Источники подтянем в фоне за 1–2 часа.")
    await m.answer("Магазины:\n" + "\n".join("• "+x["store_slug"] for x in r))

@router.message(Command("categories"))
async def cmd_categories(m: Message):
    conn = db()
    r = conn.execute("SELECT DISTINCT COALESCE(category,'—') c FROM deals ORDER BY c").fetchall()
    if not r:
        return await m.answer("Пока пусто.")
    await m.answer("Категории:\n" + "\n".join("• "+x["c"] for x in r))

@router.message(Command("search"))
async def cmd_search(m: Message):
    args = m.text.split()[1:]
    if not args:
        return await m.answer("Формат: /search <магазин> [категория]")
    store = args[0].lower()
    category = args[1].lower() if len(args)>1 else None
    if not sub_active(m.from_user.id):
        return await m.answer("Нужна активная подписка. /buy — оформить (есть триал в /start)")
    results = search_deals(store, category, limit=5)
    if not results:
        return await m.answer("Пока пусто. Попробуй другой магазин/категорию.")
    for d in results:
        await m.answer(fmt_deal(d), disable_web_page_preview=True)

# ======== SCHEDULER ========
scheduler: Optional[AsyncIOScheduler] = None

async def scrape_job():
    try:
        cnt = run_all_sources()
        print(f"[SCRAPER] added: {cnt}")
    except Exception as e:
        print("[SCRAPER] error:", e)

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("Set BOT_TOKEN env var")
    init_db()
    bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = ispatcher()
    dp.include_router(router)
    global scheduler
    scheduler = AsyncIOScheduler(timezone=TIMEZONE)
    scheduler.add_job(scrape_job, "interval", hours=2, id="scrape")
    scheduler.start()
    # Первый запуск сбора через минуту, чтобы база не была пустой
    asyncio.get_event_loop().call_later(60, lambda: asyncio.create_task(scrape_job()))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
  
