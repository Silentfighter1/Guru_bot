# guru_bot.py
import sqlite3
import logging
import re
import html
import time
from typing import Optional, Tuple, Dict, Any

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

# ================= CONFIG =================
BOT_TOKEN = "8404853617:AAE1cD_XrZffQPSiALHw6SSIuLUgaKY919c"  # your token
OWNER_ID = 6389122186
LOG_CHANNEL = -1003043723777   # your log channel
# ==========================================

# ======== Logging ========
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ======== Database (SQLite) ========
DB_FILE = "guru_data.db"
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
cur = conn.cursor()

cur.executescript("""
CREATE TABLE IF NOT EXISTS deals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER,
    message_id INTEGER,
    form_text TEXT,
    buyer_key TEXT,
    buyer_display TEXT,
    seller_key TEXT,
    seller_display TEXT,
    amount INTEGER,
    status TEXT,
    closed_by_key TEXT,
    closed_by_display TEXT,
    ts INTEGER
);

CREATE TABLE IF NOT EXISTS users (
    user_key TEXT PRIMARY KEY,   -- 'id:12345' or 'user:username'
    display TEXT,
    deals INTEGER DEFAULT 0,
    amount INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS admins (
    user_id INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS totals (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    total_deals INTEGER DEFAULT 0,
    total_amount INTEGER DEFAULT 0
);
""")
conn.commit()

# ensure totals row
cur.execute("SELECT count(*) FROM totals")
if cur.fetchone()[0] == 0:
    cur.execute("INSERT INTO totals (id, total_deals, total_amount) VALUES (1, 0, 0)")
    conn.commit()

# helper to load admins into memory (owner implicit)
def load_admins() -> set:
    cur.execute("SELECT user_id FROM admins")
    rows = cur.fetchall()
    return set(r[0] for r in rows)

bot_admins = load_admins()

# ======== Utility helpers ========
def key_from_user_id(uid: int) -> str:
    return f"id:{uid}"

def key_from_username(uname: str) -> str:
    uname_clean = uname.lstrip("@").lower()
    return f"user:{uname_clean}"

def store_or_update_user(user_key: str, display: str, add_deals: int = 0, add_amount: int = 0):
    cur.execute("SELECT deals, amount FROM users WHERE user_key=?", (user_key,))
    r = cur.fetchone()
    if r:
        new_deals = r[0] + add_deals
        new_amt = r[1] + add_amount
        cur.execute("UPDATE users SET display=?, deals=?, amount=? WHERE user_key=?", (display, new_deals, new_amt, user_key))
    else:
        cur.execute("INSERT INTO users (user_key, display, deals, amount) VALUES (?, ?, ?, ?)", (user_key, display, add_deals, add_amount))
    conn.commit()

def get_user_record_by_key(user_key: str) -> Optional[Dict[str, Any]]:
    cur.execute("SELECT user_key, display, deals, amount FROM users WHERE user_key=?", (user_key,))
    r = cur.fetchone()
    if not r:
        return None
    return {"user_key": r[0], "display": r[1], "deals": r[2], "amount": r[3]}

def increment_totals(add_deals: int, add_amount: int):
    cur.execute("UPDATE totals SET total_deals = total_deals + ?, total_amount = total_amount + ? WHERE id=1", (add_deals, add_amount))
    conn.commit()

def get_totals() -> Tuple[int, int]:
    cur.execute("SELECT total_deals, total_amount FROM totals WHERE id=1")
    r = cur.fetchone()
    return (r[0], r[1]) if r else (0, 0)

def escape_html(s: str) -> str:
    return html.escape(s) if s else s

async def try_resolve_username(context: ContextTypes.DEFAULT_TYPE, uname: str) -> Optional[Tuple[int,str]]:
    """
    Try to resolve @username to (user_id, display). Returns None if not resolvable.
    """
    uname = uname if uname.startswith('@') else ('@' + uname)
    try:
        chat = await context.bot.get_chat(uname)
        display = f"@{chat.username}" if getattr(chat, "username", None) else (chat.full_name if getattr(chat, "full_name", None) else str(chat.id))
        return (chat.id, display)
    except Exception as e:
        logger.debug(f"resolve failed for {uname}: {e}")
        return None

def numeric_from_text(s: str) -> Optional[int]:
    if not s:
        return None
    # pick all digit groups, choose the largest (helps ignore small numbers like time)
    nums = re.findall(r"\d[\d,]*", s)
    if not nums:
        return None
    try:
        ints = [int(n.replace(",", "")) for n in nums]
    except:
        return None
    return max(ints) if ints else None

# ======== Robust parser ========
async def parse_form(text: str, form_author, context: ContextTypes.DEFAULT_TYPE) -> Dict[str, Any]:
    """
    Parse ANY form-like text and return keys/displays/amount.
    Strategy:
     - check labeled lines (Buyer, Seller, Amount variants)
     - collect @mentions
     - fallback to largest number for amount
     - 'me' resolves to form_author
     - try to resolve @username via API when possible
    """
    buyer_key = seller_key = None
    buyer_display = seller_display = None
    amount = None
    details = None

    # 1) find mentions quickly
    mentions = re.findall(r'@[\w\d_]+', text)
    # 2) labeled lines
    lines = [ln.strip() for ln in text.splitlines() if ln.strip() != ""]
    for ln in lines:
        if ':' in ln:
            left, right = ln.split(':', 1)
            key = re.sub(r'[^A-Za-z]', '', left).lower()
            val = right.strip()
            # buyer
            if 'buyer' in key or 'byr' in key or 'khare' in key:
                if val.lower() == 'me':
                    buyer_key = key_from_user_id(form_author.id)
                    buyer_display = f"{'@'+form_author.username if form_author.username else form_author.full_name}"
                elif val.startswith('@'):
                    res = await try_resolve_username(context, val)
                    if res:
                        buyer_key = key_from_user_id(res[0])
                        buyer_display = res[1]
                    else:
                        buyer_key = key_from_username(val)
                        buyer_display = val
                elif re.fullmatch(r'\d+', val):
                    buyer_key = key_from_user_id(int(val))
                    buyer_display = val
                else:
                    # try resolve by adding @
                    res = await try_resolve_username(context, '@'+val)
                    if res:
                        buyer_key = key_from_user_id(res[0])
                        buyer_display = res[1]
                    else:
                        buyer_key = key_from_username(val)
                        buyer_display = val
            # seller
            elif 'seller' in key or 'sllr' in key or 'sell' in key or 'bech' in key:
                if val.lower() == 'me':
                    seller_key = key_from_user_id(form_author.id)
                    seller_display = f"{'@'+form_author.username if form_author.username else form_author.full_name}"
                elif val.startswith('@'):
                    res = await try_resolve_username(context, val)
                    if res:
                        seller_key = key_from_user_id(res[0])
                        seller_display = res[1]
                    else:
                        seller_key = key_from_username(val)
                        seller_display = val
                elif re.fullmatch(r'\d+', val):
                    seller_key = key_from_user_id(int(val))
                    seller_display = val
                else:
                    res = await try_resolve_username(context, '@'+val)
                    if res:
                        seller_key = key_from_user_id(res[0])
                        seller_display = res[1]
                    else:
                        seller_key = key_from_username(val)
                        seller_display = val
            # amount
            elif 'amount' in key or 'amt' in key or 'price' in key or '₹' in left or 'rs' in key:
                if amount is None:
                    amount = numeric_from_text(val)
            # details / deal -
            elif 'detail' in key or 'deal' in key or 'desc' in key:
                if not details:
                    details = val

    # 3) if buyer/seller still empty -> use mentions
    if not buyer_key or not seller_key:
        if len(mentions) >= 1 and not buyer_key:
            m = mentions[0]
            res = await try_resolve_username(context, m)
            if res:
                buyer_key = key_from_user_id(res[0]); buyer_display = res[1]
            else:
                buyer_key = key_from_username(m); buyer_display = m
        if len(mentions) >= 2 and not seller_key:
            m = mentions[1]
            res = await try_resolve_username(context, m)
            if res:
                seller_key = key_from_user_id(res[0]); seller_display = res[1]
            else:
                seller_key = key_from_username(m); seller_display = m

    # 4) fallback: if buyer missing -> assume form author (common case when user posts form for themselves)
    if not buyer_key:
        # try to detect "buyer: me" earlier; if still no buyer but form_author != command author, assume form_author is buyer
        buyer_key = key_from_user_id(form_author.id)
        buyer_display = f"{'@'+form_author.username if form_author.username else form_author.full_name}"

    if not seller_key:
        # don't overwrite if we think it's unknown; keep 'Unknown' as literal
        seller_key = None
        seller_display = None

    # 5) amount fallback: search anywhere for largest number
    if amount is None:
        # look for sequences like ₹50, 50 mem, 50
        nums = re.findall(r"\d[\d,]*", text)
        if nums:
            try:
                nums_ints = [int(n.replace(",", "")) for n in nums]
                amount = max(nums_ints)
            except:
                amount = None

    if amount is None:
        amount = 0

    return {
        "buyer_key": buyer_key,
        "buyer_display": buyer_display,
        "seller_key": seller_key,
        "seller_display": seller_display,
        "amount": int(amount),
        "details": details or ""
    }

# ======== Core command handlers ========
async def cmd_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Close a deal (Completed).
    Must be used as a reply to the form message.
    Prevents duplicate processing (by chat_id + message_id).
    """
    user = update.effective_user
    if not (user.id == OWNER_ID or user.id in bot_admins):
        await update.message.reply_text("⛔ Permission denied. Only bot admins can use /done.")
        return

    if not update.message.reply_to_message or not update.message.reply_to_message.text:
        await update.message.reply_text("⚠️ Reply to the form message and send /done.")
        return

    reply = update.message.reply_to_message
    chat_id = reply.chat.id
    message_id = reply.message_id

    # duplicate check
    cur.execute("SELECT id, status FROM deals WHERE chat_id=? AND message_id=?", (chat_id, message_id))
    r = cur.fetchone()
    if r:
        deal_id, status = r
        await update.message.reply_text(f"ℹ️ This form was already processed as Deal #{deal_id} (status: {status}).")
        return

    # parse
    parsed = await parse_form(reply.text, reply.from_user, context)

    # store deal
    ts = int(time.time())
    cur.execute("""
        INSERT INTO deals (
            chat_id, message_id, form_text,
            buyer_key, buyer_display, seller_key, seller_display,
            amount, status, closed_by_key, closed_by_display, ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        chat_id, message_id, reply.text,
        parsed["buyer_key"], parsed["buyer_display"], parsed["seller_key"], parsed["seller_display"],
        parsed["amount"], "completed",
        key_from_user_id(user.id), (f"@{user.username}" if user.username else user.full_name),
        ts
    ))
    deal_id = cur.lastrowid
    conn.commit()

    # update totals (only once per completed deal)
    increment_totals(1, parsed["amount"])

    # update user stats: credit buyer and seller (if present)
    if parsed["buyer_key"]:
        store_or_update_user(parsed["buyer_key"], parsed["buyer_display"] or parsed["buyer_key"], add_deals=1, add_amount=parsed["amount"])
    if parsed["seller_key"]:
        store_or_update_user(parsed["seller_key"], parsed["seller_display"] or parsed["seller_key"], add_deals=1, add_amount=parsed["amount"])

    # prepare mention HTML
    def mention_html_for_key(key: Optional[str], display: Optional[str]) -> str:
        if not key:
            return html.escape(display or "Unknown")
        if key.startswith("id:"):
            uid = int(key.split(":",1)[1])
            return f'<a href="tg://user?id={uid}">{escape_display(display)}</a>'
        if key.startswith("user:"):
            uname = key.split(":",1)[1]
            return f'<a href="https://t.me/{html.escape(uname)}">@{html.escape(uname)}</a>'
        return html.escape(display or str(key))

    def escape_display(s):
        return html.escape(s) if s else ""

    buyer_html = mention_html_for_key(parsed["buyer_key"], parsed["buyer_display"])
    seller_html = mention_html_for_key(parsed["seller_key"], parsed["seller_display"])
    closer_html = f'<a href="tg://user?id={user.id}">{html.escape(user.username or user.full_name)}</a>'

    msg_html = (
        f"✅ <b>Deal Completed</b>\n\n"
        f"🧾 <b>Deal No:</b> #{deal_id}\n"
        f"👤 <b>Buyer:</b> {buyer_html}\n"
        f"👤 <b>Seller:</b> {seller_html}\n"
        f"💰 <b>Amount:</b> ₹{parsed['amount']}\n"
        f"🛡 <b>Closed by:</b> {closer_html}\n\n"
        f"🔸 <i>Powered By Team Guru</i>"
    )

    # reply in group
    sent = await update.message.reply_html(msg_html)
    # try pi
