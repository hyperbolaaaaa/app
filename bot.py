import os
import queue
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from contextlib import contextmanager
from typing import Optional

import psycopg2
import telebot
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup


BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
FIRST_ADMIN_ID = os.getenv("ADMIN_ID")
REQUIRED_MEDIA = int(os.getenv("REQUIRED_MEDIA", "12"))
INACTIVITY_LIMIT = int(os.getenv("INACTIVITY_LIMIT_SECONDS", str(6 * 60 * 60)))
MESSAGE_MAP_MODE = os.getenv("MESSAGE_MAP_MODE", "full").strip().lower()
try:
    MESSAGE_MAP_SAMPLE_RATE = float(os.getenv("MESSAGE_MAP_SAMPLE_RATE", "1.0"))
except ValueError:
    MESSAGE_MAP_SAMPLE_RATE = 1.0
try:
    SEND_MAX_WORKERS = int(os.getenv("SEND_MAX_WORKERS", "16"))
except ValueError:
    SEND_MAX_WORKERS = 16
try:
    SEND_RETRIES = int(os.getenv("SEND_RETRIES", "2"))
except ValueError:
    SEND_RETRIES = 2

if not BOT_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
broadcast_queue: queue.Queue = queue.Queue()
media_groups = defaultdict(list)
album_timers: dict[str, bool] = {}


ADMIN_COMMANDS_TEXT = """
<b>Admin Commands</b>
/stats /info /ban /unban /whitelist /unwhitelist
/addadmin /removeadmin /openjoin /closejoin /clearmap
/dupon /dupoff /dupstatus /del /purge /setwelcome
/addword /removeword /words /panel /adminmenu
""".strip()
USER_COMMANDS_TEXT = "<b>User Commands</b>\n/start /help /chatid"


@contextmanager
def get_connection():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "CREATE TABLE IF NOT EXISTS users ("
                "user_id BIGINT PRIMARY KEY, username TEXT UNIQUE, "
                "banned BOOLEAN DEFAULT FALSE, auto_banned BOOLEAN DEFAULT FALSE, "
                "whitelisted BOOLEAN DEFAULT FALSE, activation_media_count INTEGER DEFAULT 0, "
                "total_media_sent INTEGER DEFAULT 0, last_activation_time BIGINT)"
            )
            c.execute("CREATE TABLE IF NOT EXISTS admins (user_id BIGINT PRIMARY KEY)")
            c.execute(
                "CREATE TABLE IF NOT EXISTS message_map ("
                "bot_message_id BIGINT, original_user_id BIGINT, receiver_id BIGINT, created_at BIGINT)"
            )
            c.execute("CREATE TABLE IF NOT EXISTS banned_words (word TEXT PRIMARY KEY)")
            c.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
            c.execute(
                "CREATE TABLE IF NOT EXISTS media_duplicates ("
                "file_id TEXT PRIMARY KEY, first_sender BIGINT, duplicate_count INTEGER DEFAULT 0)"
            )
            for key, value in (
                ("join_open", "true"),
                ("welcome_message", "Welcome! Please send your username."),
                ("duplicate_filter", "false"),
            ):
                c.execute(
                    "INSERT INTO settings(key, value) VALUES(%s, %s) ON CONFLICT DO NOTHING",
                    (key, value),
                )
            if FIRST_ADMIN_ID:
                c.execute(
                    "INSERT INTO admins(user_id) VALUES(%s) ON CONFLICT DO NOTHING",
                    (int(FIRST_ADMIN_ID),),
                )


def q1(sql: str, params=()) -> Optional[tuple]:
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(sql, params)
            return c.fetchone()


def exec_sql(sql: str, params=()) -> None:
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(sql, params)


def exec_many(sql: str, rows: list[tuple]) -> None:
    if not rows:
        return
    with get_connection() as conn:
        with conn.cursor() as c:
            c.executemany(sql, rows)


def is_admin(user_id: int) -> bool:
    return bool(q1("SELECT 1 FROM admins WHERE user_id=%s", (user_id,)))


def user_exists(user_id: int) -> bool:
    return bool(q1("SELECT 1 FROM users WHERE user_id=%s", (user_id,)))


def get_username(user_id: int) -> Optional[str]:
    row = q1("SELECT username FROM users WHERE user_id=%s", (user_id,))
    return row[0] if row else None


def get_setting(key: str, default: str = "") -> str:
    row = q1("SELECT value FROM settings WHERE key=%s", (key,))
    return row[0] if row else default


def set_setting(key: str, value: str) -> None:
    exec_sql(
        "INSERT INTO settings(key, value) VALUES(%s, %s) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value",
        (key, value),
    )


def start_keyboard(user_id: int) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton("User Commands", callback_data="show_user_commands")]]
    if is_admin(user_id):
        rows.insert(0, [InlineKeyboardButton("Show Admin Commands", callback_data="show_admin_commands")])
    return InlineKeyboardMarkup(rows)


def get_state(user_id: int) -> str:
    if is_admin(user_id):
        return "ADMIN"
    row = q1(
        "SELECT banned, auto_banned, whitelisted, username, last_activation_time FROM users WHERE user_id=%s",
        (user_id,),
    )
    if not row:
        return "NO_USER"
    banned, auto_banned, whitelisted, username, last_activation = row
    if banned:
        return "BANNED"
    if whitelisted:
        return "ACTIVE"
    if not username:
        return "NO_USERNAME"
    if auto_banned:
        return "INACTIVE"
    if last_activation is None:
        return "JOINING"
    return "ACTIVE"


def active_receivers() -> list[int]:
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                """
                SELECT u.user_id FROM users u
                LEFT JOIN admins a ON u.user_id = a.user_id
                WHERE u.banned=FALSE AND u.username IS NOT NULL AND
                (a.user_id IS NOT NULL OR u.whitelisted=TRUE OR (u.auto_banned=FALSE AND u.last_activation_time IS NOT NULL))
                """
            )
            return [r[0] for r in c.fetchall()]


def admin_guard(message) -> bool:
    if not is_admin(message.chat.id):
        bot.send_message(message.chat.id, "Not admin.")
        return False
    return True


def should_store_mapping(sender_id: int) -> bool:
    mode = MESSAGE_MAP_MODE
    if mode == "off":
        return False
    if mode == "admin_only":
        return is_admin(sender_id)
    if mode == "sample":
        return random.random() < max(0.0, min(1.0, MESSAGE_MAP_SAMPLE_RATE))
    return True


def _retry_after_seconds(error: Exception) -> Optional[float]:
    wait = getattr(error, "retry_after", None)
    if wait is not None:
        try:
            return float(wait)
        except (TypeError, ValueError):
            return None
    result = getattr(error, "result_json", None)
    if isinstance(result, dict):
        params = result.get("parameters", {})
        retry_after = params.get("retry_after")
        if retry_after is not None:
            try:
                return float(retry_after)
            except (TypeError, ValueError):
                return None
    return None


def _copy_with_retry(chat_id: int, from_chat_id: int, message_id: int):
    attempts = max(0, SEND_RETRIES) + 1
    for attempt in range(attempts):
        try:
            return bot.copy_message(chat_id=chat_id, from_chat_id=from_chat_id, message_id=message_id)
        except Exception as exc:
            retry_after = _retry_after_seconds(exc)
            if retry_after is not None and attempt < attempts - 1:
                time.sleep(max(0.05, retry_after))
                continue
            if attempt < attempts - 1:
                time.sleep(0.2 * (attempt + 1))
                continue
            return None
    return None


def _send_single_to_user(uid: int, sender: int, src_message_id: int, store_mapping: bool, now: int) -> list[tuple]:
    sent = _copy_with_retry(chat_id=uid, from_chat_id=sender, message_id=src_message_id)
    if not sent or not store_mapping:
        return []
    return [(sent.message_id, sender, uid, now)]


def _send_album_to_user(uid: int, sender: int, msg_ids: list[int], store_mapping: bool, now: int) -> list[tuple]:
    user_rows: list[tuple] = []
    for msg_id in msg_ids:
        sent = _copy_with_retry(chat_id=uid, from_chat_id=sender, message_id=msg_id)
        if sent and store_mapping:
            user_rows.append((sent.message_id, sender, uid, now))
    return user_rows


@bot.message_handler(commands=["start"])
def start_command(message):
    uid = message.chat.id
    if is_admin(uid):
        if not user_exists(uid):
            exec_sql("INSERT INTO users(user_id, username) VALUES(%s, 'admin') ON CONFLICT DO NOTHING", (uid,))
        bot.send_message(
            uid,
            "Admin access granted.\nUse button below to see all admin commands.",
            reply_markup=start_keyboard(uid),
        )
        return
    if not user_exists(uid):
        if get_setting("join_open", "true") != "true":
            bot.send_message(uid, "Joining is currently closed.")
            return
        exec_sql("INSERT INTO users(user_id) VALUES(%s) ON CONFLICT DO NOTHING", (uid,))
    if not get_username(uid):
        bot.send_message(uid, get_setting("welcome_message"), reply_markup=start_keyboard(uid))
    else:
        bot.send_message(uid, "Welcome back.", reply_markup=start_keyboard(uid))


@bot.callback_query_handler(func=lambda call: call.data in {"show_user_commands", "show_admin_commands"})
def on_command_buttons(call):
    uid = call.message.chat.id
    if call.data == "show_admin_commands":
        if not is_admin(uid):
            bot.answer_callback_query(call.id, "Admin only.", show_alert=True)
            return
        bot.send_message(uid, ADMIN_COMMANDS_TEXT)
    else:
        bot.send_message(uid, USER_COMMANDS_TEXT)
    bot.answer_callback_query(call.id)


@bot.message_handler(commands=["help"])
def help_command(message):
    bot.send_message(message.chat.id, USER_COMMANDS_TEXT)


@bot.message_handler(func=lambda m: get_state(m.chat.id) == "NO_USERNAME", content_types=["text"])
def capture_username(message):
    username = message.text.strip().lower()
    if username.startswith("/") or len(username) < 3:
        return
    if q1("SELECT 1 FROM users WHERE username=%s", (username,)):
        bot.send_message(message.chat.id, "Username already taken.")
        return
    exec_sql("UPDATE users SET username=%s WHERE user_id=%s", (username, message.chat.id))
    bot.send_message(message.chat.id, f"{username} set. Now send {REQUIRED_MEDIA} media to join.")


def relay_single(message) -> None:
    sender = message.chat.id
    mappings: list[tuple] = []
    store_mapping = should_store_mapping(sender)
    now = int(time.time())
    receivers = [uid for uid in active_receivers() if uid != sender]
    workers = max(1, min(SEND_MAX_WORKERS, len(receivers) or 1))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(
                _send_single_to_user,
                uid,
                sender,
                message.message_id,
                store_mapping,
                now,
            )
            for uid in receivers
        ]
        for fut in futures:
            mappings.extend(fut.result())
    exec_many(
        "INSERT INTO message_map(bot_message_id, original_user_id, receiver_id, created_at) VALUES(%s, %s, %s, %s)",
        mappings,
    )


def relay_album(messages: list) -> None:
    sender = messages[0].chat.id
    store_mapping = should_store_mapping(sender)
    mappings: list[tuple] = []
    now = int(time.time())
    msg_ids = [m.message_id for m in messages]
    receivers = [uid for uid in active_receivers() if uid != sender]
    workers = max(1, min(SEND_MAX_WORKERS, len(receivers) or 1))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(
                _send_album_to_user,
                uid,
                sender,
                msg_ids,
                store_mapping,
                now,
            )
            for uid in receivers
        ]
        for fut in futures:
            mappings.extend(fut.result())
    exec_many(
        "INSERT INTO message_map(bot_message_id, original_user_id, receiver_id, created_at) VALUES(%s, %s, %s, %s)",
        mappings,
    )


def worker():
    while True:
        job = broadcast_queue.get()
        try:
            if job["type"] == "single":
                relay_single(job["message"])
            else:
                relay_album(job["messages"])
        finally:
            broadcast_queue.task_done()


@bot.message_handler(func=lambda m: not m.text or not m.text.startswith("/"), content_types=["text", "photo", "video"])
def relay(message):
    state = get_state(message.chat.id)
    if state == "BANNED":
        bot.send_message(message.chat.id, "You are banned.")
        return
    if state in {"NO_USER", "NO_USERNAME"}:
        bot.send_message(message.chat.id, "Use /start first.")
        return
    if message.content_type == "text":
        text = (message.text or "").lower()
        row = q1("SELECT 1 FROM banned_words WHERE %s LIKE CONCAT('%%', word, '%%') LIMIT 1", (text,))
        if row:
            bot.send_message(message.chat.id, "Message contains banned word.")
            return
    if message.content_type in {"photo", "video"} and get_setting("duplicate_filter", "false") == "true":
        file_id = message.photo[-1].file_id if message.content_type == "photo" else message.video.file_id
        if q1("SELECT 1 FROM media_duplicates WHERE file_id=%s", (file_id,)):
            exec_sql(
                "UPDATE media_duplicates SET duplicate_count=duplicate_count+1 WHERE file_id=%s",
                (file_id,),
            )
            return
        exec_sql(
            "INSERT INTO media_duplicates(file_id, first_sender) VALUES(%s, %s)",
            (file_id, message.chat.id),
        )
    if message.media_group_id:
        gid = str(message.media_group_id)
        media_groups[gid].append(message)
        if gid in album_timers:
            return
        album_timers[gid] = True

        def flush():
            time.sleep(1.0)
            msgs = media_groups.pop(gid, [])
            album_timers.pop(gid, None)
            if msgs:
                broadcast_queue.put({"type": "album", "messages": msgs})

        threading.Thread(target=flush, daemon=True).start()
        return
    broadcast_queue.put({"type": "single", "message": message})


@bot.message_handler(commands=["adminmenu"])
def adminmenu(message):
    if admin_guard(message):
        bot.send_message(message.chat.id, ADMIN_COMMANDS_TEXT)


@bot.message_handler(commands=["panel"])
def panel(message):
    if not admin_guard(message):
        return
    kb = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Show Admin Commands", callback_data="show_admin_commands")]]
    )
    bot.send_message(message.chat.id, "Admin Panel", reply_markup=kb)


def parse_target_id(message) -> Optional[int]:
    if message.reply_to_message:
        row = q1(
            "SELECT original_user_id FROM message_map WHERE bot_message_id=%s LIMIT 1",
            (message.reply_to_message.message_id,),
        )
        return int(row[0]) if row else None
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return None
    try:
        return int(parts[1].strip())
    except ValueError:
        return None


@bot.message_handler(commands=["stats"])
def stats(message):
    if not admin_guard(message):
        return
    total = int((q1("SELECT COUNT(*) FROM users") or (0,))[0])
    banned = int((q1("SELECT COUNT(*) FROM users WHERE banned=TRUE") or (0,))[0])
    whitelisted = int((q1("SELECT COUNT(*) FROM users WHERE whitelisted=TRUE") or (0,))[0])
    map_count = int((q1("SELECT COUNT(*) FROM message_map") or (0,))[0])
    dup = int((q1("SELECT COALESCE(SUM(duplicate_count),0) FROM media_duplicates") or (0,))[0])
    join_status = "OPEN" if get_setting("join_open", "true") == "true" else "CLOSED"
    bot.send_message(
        message.chat.id,
        f"Users: {total}\nBanned: {banned}\nWhitelisted: {whitelisted}\nMap: {map_count}\nDuplicates: {dup}\nJoin: {join_status}",
    )


@bot.message_handler(commands=["openjoin"])
def openjoin(message):
    if admin_guard(message):
        set_setting("join_open", "true")
        bot.send_message(message.chat.id, "Join opened.")


@bot.message_handler(commands=["closejoin"])
def closejoin(message):
    if admin_guard(message):
        set_setting("join_open", "false")
        bot.send_message(message.chat.id, "Join closed.")


@bot.message_handler(commands=["dupon"])
def dupon(message):
    if admin_guard(message):
        set_setting("duplicate_filter", "true")
        bot.send_message(message.chat.id, "Duplicate filter enabled.")


@bot.message_handler(commands=["dupoff"])
def dupoff(message):
    if admin_guard(message):
        set_setting("duplicate_filter", "false")
        bot.send_message(message.chat.id, "Duplicate filter disabled.")


@bot.message_handler(commands=["dupstatus"])
def dupstatus(message):
    if admin_guard(message):
        bot.send_message(message.chat.id, f"Duplicate filter: {get_setting('duplicate_filter', 'false')}")


@bot.message_handler(commands=["ban"])
def ban(message):
    if not admin_guard(message):
        return
    target = parse_target_id(message)
    if not target:
        bot.send_message(message.chat.id, "Usage: /ban USER_ID or reply to relayed message.")
        return
    if is_admin(target):
        bot.send_message(message.chat.id, "Cannot ban admin.")
        return
    exec_sql("UPDATE users SET banned=TRUE WHERE user_id=%s", (target,))
    bot.send_message(message.chat.id, f"User {target} banned.")


@bot.message_handler(commands=["unban"])
def unban(message):
    if not admin_guard(message):
        return
    target = parse_target_id(message)
    if not target:
        bot.send_message(message.chat.id, "Usage: /unban USER_ID or reply to relayed message.")
        return
    exec_sql("UPDATE users SET banned=FALSE WHERE user_id=%s", (target,))
    bot.send_message(message.chat.id, f"User {target} unbanned.")


@bot.message_handler(commands=["addadmin"])
def addadmin(message):
    if not admin_guard(message):
        return
    target = parse_target_id(message)
    if not target:
        bot.send_message(message.chat.id, "Usage: /addadmin USER_ID")
        return
    exec_sql("INSERT INTO admins(user_id) VALUES(%s) ON CONFLICT DO NOTHING", (target,))
    bot.send_message(message.chat.id, "Admin added.")


@bot.message_handler(commands=["removeadmin"])
def removeadmin(message):
    if not admin_guard(message):
        return
    target = parse_target_id(message)
    if not target:
        bot.send_message(message.chat.id, "Usage: /removeadmin USER_ID")
        return
    exec_sql("DELETE FROM admins WHERE user_id=%s", (target,))
    bot.send_message(message.chat.id, "Admin removed.")


@bot.message_handler(commands=["whitelist"])
def whitelist(message):
    if not admin_guard(message):
        return
    target = parse_target_id(message)
    if not target:
        bot.send_message(message.chat.id, "Usage: /whitelist USER_ID")
        return
    exec_sql("UPDATE users SET whitelisted=TRUE WHERE user_id=%s", (target,))
    bot.send_message(message.chat.id, f"User {target} whitelisted.")


@bot.message_handler(commands=["unwhitelist"])
def unwhitelist(message):
    if not admin_guard(message):
        return
    target = parse_target_id(message)
    if not target:
        bot.send_message(message.chat.id, "Usage: /unwhitelist USER_ID")
        return
    exec_sql("UPDATE users SET whitelisted=FALSE WHERE user_id=%s", (target,))
    bot.send_message(message.chat.id, f"User {target} unwhitelisted.")


@bot.message_handler(commands=["clearmap"])
def clearmap(message):
    if admin_guard(message):
        exec_sql("DELETE FROM message_map")
        bot.send_message(message.chat.id, "Message map cleared.")


@bot.message_handler(commands=["setwelcome"])
def setwelcome(message):
    if not admin_guard(message):
        return
    if not message.reply_to_message or not message.reply_to_message.text:
        bot.send_message(message.chat.id, "Reply to a text message.")
        return
    set_setting("welcome_message", message.reply_to_message.text)
    bot.send_message(message.chat.id, "Welcome message updated.")


@bot.message_handler(commands=["addword"])
def addword(message):
    if not admin_guard(message):
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.send_message(message.chat.id, "Usage: /addword WORD")
        return
    exec_sql("INSERT INTO banned_words(word) VALUES(%s) ON CONFLICT DO NOTHING", (parts[1].strip().lower(),))
    bot.send_message(message.chat.id, "Word added.")


@bot.message_handler(commands=["removeword"])
def removeword(message):
    if not admin_guard(message):
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.send_message(message.chat.id, "Usage: /removeword WORD")
        return
    exec_sql("DELETE FROM banned_words WHERE word=%s", (parts[1].strip().lower(),))
    bot.send_message(message.chat.id, "Word removed.")


@bot.message_handler(commands=["words"])
def words(message):
    if not admin_guard(message):
        return
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("SELECT word FROM banned_words ORDER BY word")
            rows = [r[0] for r in c.fetchall()]
    bot.send_message(message.chat.id, "\n".join(rows) if rows else "No banned words.")


@bot.message_handler(commands=["info"])
def info(message):
    if not admin_guard(message):
        return
    target = parse_target_id(message)
    if not target:
        bot.send_message(message.chat.id, "Usage: /info USER_ID or reply to relayed message.")
        return
    row = q1(
        "SELECT username,banned,auto_banned,whitelisted,total_media_sent,last_activation_time FROM users WHERE user_id=%s",
        (target,),
    )
    if not row:
        bot.send_message(message.chat.id, "User not found.")
        return
    bot.send_message(
        message.chat.id,
        f"ID: {target}\nUsername: {row[0]}\nBanned: {row[1]}\nAuto-banned: {row[2]}\nWhitelisted: {row[3]}\nTotal media: {row[4]}\nLast activation: {row[5]}",
    )


@bot.message_handler(commands=["del"])
def delete_globally(message):
    if not admin_guard(message):
        return
    if not message.reply_to_message:
        bot.send_message(message.chat.id, "Reply to relayed message.")
        return
    msg_id = message.reply_to_message.message_id
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("SELECT receiver_id FROM message_map WHERE bot_message_id=%s", (msg_id,))
            rows = [r[0] for r in c.fetchall()]
    for receiver in rows:
        try:
            bot.delete_message(receiver, msg_id)
        except Exception:
            pass
    exec_sql("DELETE FROM message_map WHERE bot_message_id=%s", (msg_id,))
    bot.send_message(message.chat.id, "Deleted globally.")


@bot.message_handler(commands=["purge"])
def purge(message):
    if not admin_guard(message):
        return
    target = parse_target_id(message)
    if not target:
        bot.send_message(message.chat.id, "Reply to relayed message or use /purge USER_ID.")
        return
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("SELECT bot_message_id, receiver_id FROM message_map WHERE original_user_id=%s", (target,))
            rows = c.fetchall()
    for msg_id, receiver in rows:
        try:
            bot.delete_message(receiver, msg_id)
        except Exception:
            pass
    exec_sql("DELETE FROM message_map WHERE original_user_id=%s", (target,))
    bot.send_message(message.chat.id, f"Purged relayed messages for {target}.")


@bot.message_handler(commands=["chatid"])
def chatid(message):
    bot.reply_to(message, f"Chat ID: {message.chat.id}")


if __name__ == "__main__":
    init_db()
    threading.Thread(target=worker, daemon=True).start()
    bot.infinity_polling(skip_pending=True)
