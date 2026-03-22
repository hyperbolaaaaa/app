# =========================
# 📦 IMPORTS
# =========================

import os
import time
import threading
import queue
from contextlib import contextmanager
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
 # make sure to have this file for cross-instance forwarding
import psycopg2
from psycopg2 import pool as pg_pool
import telebot
from telebot.types import InputMediaPhoto, InputMediaVideo
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton


# =========================
# ⚙ CONFIGURATION
# =========================

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
FIRST_ADMIN_ID = os.getenv("ADMIN_ID") # replace with your Telegram ID for initial admin access


REQUIRED_MEDIA = 12
INACTIVITY_LIMIT = 6 * 60 * 60  # 6 hours
FORWARD_DELAY = float(os.getenv("FORWARD_DELAY", "0.01"))
SEND_MAX_WORKERS = int(os.getenv("SEND_MAX_WORKERS", "8"))
SEND_RETRIES = int(os.getenv("SEND_RETRIES", "2"))
RECEIVER_CACHE_TTL = int(os.getenv("RECEIVER_CACHE_TTL", "10"))
BROADCAST_QUEUE_SIZE = int(os.getenv("BROADCAST_QUEUE_SIZE", "2000"))
DB_POOL_MIN_CONN = int(os.getenv("DB_POOL_MIN_CONN", "1"))
DB_POOL_MAX_CONN = int(os.getenv("DB_POOL_MAX_CONN", "15"))

if not BOT_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL")

bot = telebot.TeleBot(BOT_TOKEN)
broadcast_queue = queue.Queue(maxsize=BROADCAST_QUEUE_SIZE)
media_groups = defaultdict(list)
album_timers = {}
user_media_buffer = defaultdict(list)
user_media_timer = {}
media_buffer_lock = threading.Lock()
activation_buffer = defaultdict(int)
activation_timer = {}
activation_lock = threading.Lock()
db_pool = None
receiver_cache_lock = threading.Lock()
receiver_cache = []
receiver_cache_at = 0.0

# =========================
# 🗄 DATABASE CONNECTION
# =========================

def init_db_pool():
    global db_pool
    if db_pool is None:
        db_pool = pg_pool.SimpleConnectionPool(
            DB_POOL_MIN_CONN,
            DB_POOL_MAX_CONN,
            dsn=DATABASE_URL,
        )


@contextmanager
def get_connection():
    if db_pool is not None:
        conn = db_pool.getconn()
        use_pool = True
    else:
        conn = psycopg2.connect(DATABASE_URL)
        use_pool = False
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        if use_pool:
            db_pool.putconn(conn)
        else:
            conn.close()
# =========================
# 🧱 DATABASE INITIALIZATION
# =========================

def init_db():

    with get_connection() as conn:
        with conn.cursor() as c:

            # =========================
            # USERS TABLE
            # =========================
            c.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT UNIQUE,
                    banned BOOLEAN DEFAULT FALSE,
                    auto_banned BOOLEAN DEFAULT FALSE,
                    whitelisted BOOLEAN DEFAULT FALSE,
                    activation_media_count INTEGER DEFAULT 0,
                    total_media_sent INTEGER DEFAULT 0,
                    last_activation_time BIGINT
                )
            """)

            # =========================
            # ADMINS TABLE
            # =========================
            c.execute("""
                CREATE TABLE IF NOT EXISTS admins (
                    user_id BIGINT PRIMARY KEY
                )
            """)

            # =========================
            # MESSAGE MAP TABLE
            # =========================
            c.execute("""
                CREATE TABLE IF NOT EXISTS message_map (
                    bot_message_id BIGINT,
                    original_user_id BIGINT,
                    receiver_id BIGINT,
                    created_at BIGINT
                )
            """)

            # =========================
            # BANNED WORDS TABLE
            # =========================
            c.execute("""
                CREATE TABLE IF NOT EXISTS banned_words (
                    word TEXT PRIMARY KEY
                )
            """)

            # =========================
            # SETTINGS TABLE
            # =========================
            c.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)

            # Default Join Setting
            c.execute("""
                INSERT INTO settings(key, value)
                VALUES('join_open', 'true')
                ON CONFLICT DO NOTHING
            """)
            # =========================
            # 📦 DUPLICATE TRACKING
            # =========================
            c.execute("""
                CREATE TABLE IF NOT EXISTS media_duplicates (
                    file_id TEXT PRIMARY KEY,
                    first_sender BIGINT,
                    duplicate_count INTEGER DEFAULT 0
                )
            """)
            # Default Welcome Message
            c.execute("""
                INSERT INTO settings(key, value)
                VALUES('welcome_message', '👋 Welcome!\n\nPlease drop your username:')
                ON CONFLICT DO NOTHING
            """)

            c.execute("""
                INSERT INTO settings(key, value)
                VALUES('duplicate_filter', 'false')
                ON CONFLICT DO NOTHING
            """)
            c.execute("""
                INSERT INTO settings(key, value)
                VALUES('maintenance_mode', 'false')
                ON CONFLICT DO NOTHING
            """)
            # =========================
            # FIRST ADMIN INIT
            # =========================

            first_admin = FIRST_ADMIN_ID

            if first_admin:
                try:
                    first_admin = int(first_admin)

                    c.execute("""
                        INSERT INTO admins(user_id)
                        VALUES(%s)
                        ON CONFLICT DO NOTHING
                    """, (first_admin,))

                    print("First admin ensured.")

                except Exception as e:
                    print("Admin init error:", e)

# =========================
# 👤 USER EXISTENCE
# =========================
def delete_message_globally(bot_message_id):

    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT receiver_id
                FROM message_map
                WHERE bot_message_id=%s
            """, (bot_message_id,))
            rows = c.fetchall()

    for row in rows:
        try:
            bot.delete_message(row[0], bot_message_id)
        except:
            pass

    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                DELETE FROM message_map
                WHERE bot_message_id=%s
            """, (bot_message_id,))
def purge_user_messages(user_id):

    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT bot_message_id, receiver_id
                FROM message_map
                WHERE original_user_id=%s
            """, (user_id,))
            rows = c.fetchall()

    for bot_msg_id, receiver_id in rows:
        try:
            bot.delete_message(receiver_id, bot_msg_id)
        except:
            pass

    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                DELETE FROM message_map
                WHERE original_user_id=%s
            """, (user_id,))

def get_original_sender(bot_message_id):

    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT original_user_id
                FROM message_map
                WHERE bot_message_id=%s
                LIMIT 1
            """, (bot_message_id,))
            row = c.fetchone()

    return row[0] if row else None

def user_exists(user_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "SELECT 1 FROM users WHERE user_id=%s",
                (user_id,)
            )
            return c.fetchone() is not None


def add_user(user_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO users(user_id)
                VALUES(%s)
                ON CONFLICT DO NOTHING
            """, (user_id,))
# =========================
# 🏷 USERNAME HELPERS
# =========================

def get_username(user_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "SELECT username FROM users WHERE user_id=%s",
                (user_id,)
            )
            row = c.fetchone()
            return row[0] if row else None


def set_username(user_id, username):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                UPDATE users
                SET username=%s
                WHERE user_id=%s
            """, (username.lower(), user_id))


def username_taken(username):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "SELECT 1 FROM users WHERE username=%s",
                (username.lower(),)
            )
            return c.fetchone() is not None
# =========================
# 👑 ADMIN HELPERS
# =========================

def is_admin(user_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "SELECT 1 FROM admins WHERE user_id=%s",
                (user_id,)
            )
            return c.fetchone() is not None


def add_admin(user_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO admins(user_id)
                VALUES(%s)
                ON CONFLICT DO NOTHING
            """, (user_id,))


def remove_admin(user_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "DELETE FROM admins WHERE user_id=%s",
                (user_id,)
            )
def build_prefix(user_id):

    username = get_username(user_id)

    if username:
        return f"{username}~\n"

    return "👤 Unknown\n"

# =========================
# 🚫 BAN HELPERS
# =========================

def is_banned(user_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "SELECT banned FROM users WHERE user_id=%s",
                (user_id,)
            )
            row = c.fetchone()
            return row and row[0]


def ban_user(user_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "UPDATE users SET banned=TRUE WHERE user_id=%s",
                (user_id,)
            )


def unban_user(user_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "UPDATE users SET banned=FALSE WHERE user_id=%s",
                (user_id,)
            )
# =========================
# ⭐ WHITELIST HELPERS
# =========================

def is_whitelisted(user_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "SELECT whitelisted FROM users WHERE user_id=%s",
                (user_id,)
            )
            row = c.fetchone()
            return row and row[0]


def whitelist_user(user_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "UPDATE users SET whitelisted=TRUE WHERE user_id=%s",
                (user_id,)
            )


def remove_whitelist(user_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "UPDATE users SET whitelisted=FALSE WHERE user_id=%s",
                (user_id,)
            )
# =========================
# 🚪 JOIN CONTROL
# =========================

def is_join_open():
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "SELECT value FROM settings WHERE key='join_open'"
            )
            row = c.fetchone()
            return row and row[0] == "true"


def set_join_status(status: bool):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                UPDATE settings
                SET value=%s
                WHERE key='join_open'
            """, ("true" if status else "false",))


def is_maintenance_mode():
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("SELECT value FROM settings WHERE key='maintenance_mode'")
            row = c.fetchone()
            return row and row[0] == "true"


def set_maintenance_mode(status: bool):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                """
                INSERT INTO settings(key, value)
                VALUES('maintenance_mode', %s)
                ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
                """,
                ("true" if status else "false",),
            )
# =========================
# 🧠 USER STATE RESOLVER
# =========================

def get_user_state(user_id):

    if is_admin(user_id):
        return "ADMIN"

    if is_banned(user_id):
        return "BANNED"

    if is_whitelisted(user_id):
        return "ACTIVE"

    username = get_username(user_id)

    if username is None:
        return "NO_USERNAME"

    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT last_activation_time
                FROM users
                WHERE user_id=%s
            """, (user_id,))
            row = c.fetchone()

    if not row:
        return "JOINING"

    last_activation_time = row[0]

    if last_activation_time is None:
        return "JOINING"

    if last_activation_time < int(time.time()) - INACTIVITY_LIMIT:
        return "INACTIVE"

    return "ACTIVE"
# =========================
# 📊 GET ACTIVATION DATA
# =========================

def get_activation_data(user_id):
    """
    Returns:
        activation_media_count,
        total_media_sent,
        auto_banned,
        last_activation_time
    """

    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT activation_media_count,
                       total_media_sent,
                       auto_banned,
                       last_activation_time
                FROM users
                WHERE user_id=%s
            """, (user_id,))
            return c.fetchone()
# =========================
# 📈 INCREMENT MEDIA
# =========================

def increment_media(user_id, amount=1):

    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                UPDATE users
                SET activation_media_count = activation_media_count + %s,
                    total_media_sent = total_media_sent + %s
                WHERE user_id=%s
            """, (amount, amount, user_id))
#========================
#wellcome msg by admin helper
#========================   
def get_welcome_message():
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "SELECT value FROM settings WHERE key='welcome_message'"
            )
            row = c.fetchone()
            return row[0] if row else "👋 Welcome!"

def set_welcome_message(text):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                UPDATE settings
                SET value=%s
                WHERE key='welcome_message'
            """, (text,))
# =========================
# 🔄 ACTIVATE USER
# =========================

def activate_user(user_id):

    now = int(time.time())

    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                UPDATE users
                SET activation_media_count = 0,
                    auto_banned = FALSE,
                    last_activation_time = %s
                WHERE user_id=%s
            """, (now, user_id))
# =========================
# ✅ CHECK ACTIVATION
# =========================

def check_activation(user_id):

    data = get_activation_data(user_id)

    if not data:
        return False

    activation_count, _, _, _ = data

    if activation_count >= REQUIRED_MEDIA:
        activate_user(user_id)
        return True

    return False
# =========================
# ⏳ AUTO INACTIVITY CHECK
# =========================

def auto_ban_inactive_users():

    limit = int(time.time()) - INACTIVITY_LIMIT

    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                UPDATE users
                SET auto_banned = TRUE,
                    activation_media_count = 0
                WHERE auto_banned = FALSE
                  AND last_activation_time IS NOT NULL
                  AND last_activation_time < %s
            """, (limit,))
            
def is_duplicate_filter_enabled():
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "SELECT value FROM settings WHERE key='duplicate_filter'"
            )
            row = c.fetchone()
            return row and row[0] == "true"


def set_duplicate_filter(status: bool):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                UPDATE settings
                SET value=%s
                WHERE key='duplicate_filter'
            """, ("true" if status else "false",))


def check_and_register_duplicate(file_id, sender_id):
    """
    Returns True if duplicate
    Returns False if first time
    """

    with get_connection() as conn:
        with conn.cursor() as c:

            c.execute(
                "SELECT 1 FROM media_duplicates WHERE file_id=%s",
                (file_id,)
            )
            exists = c.fetchone()

            if exists:
                c.execute("""
                    UPDATE media_duplicates
                    SET duplicate_count = duplicate_count + 1
                    WHERE file_id=%s
                """, (file_id,))
                return True

            else:
                c.execute("""
                    INSERT INTO media_duplicates(file_id, first_sender)
                    VALUES(%s, %s)
                """, (file_id, sender_id))
                return False
# =========================
# 🚪 START COMMAND
# =========================

@bot.message_handler(commands=['start'])
def start_command(message):

    user_id = message.chat.id

    if is_maintenance_mode() and not is_admin(user_id):
        bot.send_message(user_id, "Bot is under maintenance. Try again later.")
        return

    # 🚫 Manual Ban
    if is_banned(user_id):
        bot.send_message(user_id, "🚫 You are banned.")
        return

    # 👑 Admin Auto Registration
    if is_admin(user_id):
        if not user_exists(user_id):
            add_user(user_id)

        if get_username(user_id) is None:
            set_username(user_id, "admin")

        bot.send_message(user_id, "👑 Admin access granted.")
        return

    # 🆕 New User
    if not user_exists(user_id):

        if not is_join_open():
            bot.send_message(
                user_id,
                "🚪 Joining is currently closed."
            )
            return

        add_user(user_id)

    # 🏷 Ask Username If Not Set
    if get_username(user_id) is None:
        bot.send_message(
            user_id,
            get_welcome_message()
        )
        
        return

    # 🧠 Show Current State
    state = get_user_state(user_id)

    if state == "JOINING":
        bot.send_message(
            user_id,
            f"🔒 Send {REQUIRED_MEDIA} media to join."
        )

    elif state == "INACTIVE":
        bot.send_message(
            user_id,
            f"⏳ You are inactive.\nSend {REQUIRED_MEDIA} media to reactivate."
        )

    else:
        bot.send_message(user_id, "👋 Welcome back!")
# =========================
# 🏷 USERNAME CAPTURE
# =========================

@bot.message_handler(
    func=lambda m: get_username(m.chat.id) is None,
    content_types=['text']
)
def capture_username(message):

    user_id = message.chat.id
    username = message.text.strip().lower()

    # Prevent commands being treated as username
    if username.startswith('/'):
        return

    if len(username) < 3:
        bot.send_message(user_id, "Username too short. Try again.")
        return

    if username_taken(username):
        bot.send_message(user_id, "Username already taken. Try another.")
        return

    set_username(user_id, username)

    bot.send_message(
        user_id,
        f"✅ {username} set.\n\nNow send {REQUIRED_MEDIA} media to join."
    )
# =========================
# 🚫 BANNED WORD CHECK
# =========================

def contains_banned_word(text):

    if not text:
        return False

    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("SELECT word FROM banned_words")
            words = [row[0] for row in c.fetchall()]

    text = text.lower()

    for word in words:
        if word in text:
            return True

    return False
# =========================
# 🔒 HANDLE RESTRICTIONS
# =========================

def handle_restrictions(message):

    user_id = message.chat.id
    state = get_user_state(user_id)

    # 🚫 Manual Ban
    if state == "BANNED":
        bot.send_message(user_id, "🚫 You are banned.")
        return True

    # 👑 Admin Bypass
    if state == "ADMIN":
        return False

    # ⭐ Whitelisted = Always Active
    if is_whitelisted(user_id):
        return False

    # 🚫 Word Filter (text only)
    if message.content_type == "text":
        if contains_banned_word(message.text):
            bot.send_message(user_id, "🚫 Message contains banned word.")
            return True

    # ❌ No Username Yet
    if state == "NO_USERNAME":
        bot.send_message(
            user_id,
            "⚠️ Please set username first using /start."
        )
        return True

    # =========================
    # 🟡 JOINING STATE
    # =========================
    if state == "JOINING":

        if message.content_type in ['photo', 'video']:

            with activation_lock:
                activation_buffer[user_id] += 1

                if user_id in activation_timer:
                    return False  # allow relay but don't respond yet

                activation_timer[user_id] = True

            def finalize_activation():
                time.sleep(1.0)

                with activation_lock:
                    amount = activation_buffer.pop(user_id, 0)
                    activation_timer.pop(user_id, None)

                if amount > 0:
                    increment_media(user_id, amount)

                    activated = check_activation(user_id)

                    if activated:
                        bot.send_message(
                            user_id,
                            "🎉 You are now active for 6 hours!"
                        )
                    else:
                        remaining = REQUIRED_MEDIA - get_activation_data(user_id)[0]
                        bot.send_message(
                            user_id,
                            f"📸 {remaining} media left to join."
                        )

            threading.Thread(target=finalize_activation).start()

            return False  # allow media relay

        bot.send_message(
            user_id,
            f"🔒 Send {REQUIRED_MEDIA} media to join."
        )
        return True


    # =========================
    # 🔴 INACTIVE STATE
    # =========================
    if state == "INACTIVE":

        if message.content_type in ['photo', 'video']:

            with activation_lock:
                activation_buffer[user_id] += 1

                if user_id in activation_timer:
                    return False

                activation_timer[user_id] = True

            def finalize_reactivation():
                time.sleep(1.0)

                with activation_lock:
                    amount = activation_buffer.pop(user_id, 0)
                    activation_timer.pop(user_id, None)

                if amount > 0:
                    increment_media(user_id, amount)

                    activated = check_activation(user_id)

                    if activated:
                        bot.send_message(
                            user_id,
                            "🎉 You are reactivated for 6 hours!"
                        )
                    else:
                        remaining = REQUIRED_MEDIA - get_activation_data(user_id)[0]
                        bot.send_message(
                            user_id,
                            f"📸 {remaining} media left to reactivate."
                        )

            threading.Thread(target=finalize_reactivation).start()

            return False

        bot.send_message(
            user_id,
            f"⏳ You are inactive.\nSend {REQUIRED_MEDIA} media to reactivate."
        )
        return True


    # =========================
    # 🟢 ACTIVE STATE
    # =========================
    if state == "ACTIVE":

        if message.content_type in ['photo', 'video']:

            increment_media(user_id)
            renewed = check_activation(user_id)

        return False
# =========================
# 📥 GET ACTIVE RECEIVERS
# =========================

def get_active_receivers():

    active_cutoff = int(time.time()) - INACTIVITY_LIMIT
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT u.user_id
                FROM users u
                LEFT JOIN admins a ON u.user_id = a.user_id
                WHERE u.banned = FALSE
                  AND u.username IS NOT NULL
                  AND (
                        a.user_id IS NOT NULL
                        OR u.whitelisted = TRUE
                        OR (
                            u.last_activation_time IS NOT NULL
                            AND u.last_activation_time >= %s
                        )
                      )
            """, (active_cutoff,))
            return [row[0] for row in c.fetchall()]


def get_receivers_cached(force=False):
    global receiver_cache, receiver_cache_at
    now = time.time()
    with receiver_cache_lock:
        if force or (now - receiver_cache_at) >= RECEIVER_CACHE_TTL:
            receiver_cache = get_active_receivers()
            receiver_cache_at = now
        return list(receiver_cache)

# =========================
# 📝 SAVE MESSAGE MAP
# =========================

def save_mapping(bot_msg_id, original_user_id, receiver_id):

    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO message_map
                (bot_message_id, original_user_id, receiver_id, created_at)
                VALUES (%s, %s, %s, %s)
            """, (
                bot_msg_id,
                original_user_id,
                receiver_id,
                int(time.time())
            ))


def save_mappings(rows):
    if not rows:
        return
    with get_connection() as conn:
        with conn.cursor() as c:
            c.executemany(
                """
                INSERT INTO message_map
                (bot_message_id, original_user_id, receiver_id, created_at)
                VALUES (%s, %s, %s, %s)
                """,
                rows,
            )


def _retry_after_seconds(error):
    retry_after = getattr(error, "retry_after", None)
    if retry_after is not None:
        try:
            return float(retry_after)
        except Exception:
            return None
    result_json = getattr(error, "result_json", None)
    if isinstance(result_json, dict):
        params = result_json.get("parameters", {})
        retry_after = params.get("retry_after")
        if retry_after is not None:
            try:
                return float(retry_after)
            except Exception:
                return None
    return None


def _copy_message_with_retry(user_id, sender_id, message_id):
    attempts = max(0, SEND_RETRIES) + 1
    for i in range(attempts):
        try:
            sent = bot.copy_message(
                chat_id=user_id,
                from_chat_id=sender_id,
                message_id=message_id,
            )
            if FORWARD_DELAY > 0:
                time.sleep(FORWARD_DELAY)
            return sent
        except Exception as e:
            wait = _retry_after_seconds(e)
            if i < attempts - 1:
                if wait is not None:
                    time.sleep(max(0.05, wait))
                else:
                    time.sleep(0.15 * (i + 1))
                continue
            return None
    return None


def _send_text_with_retry(user_id, text):
    attempts = max(0, SEND_RETRIES) + 1
    for i in range(attempts):
        try:
            sent = bot.send_message(user_id, text)
            if FORWARD_DELAY > 0:
                time.sleep(FORWARD_DELAY)
            return sent
        except Exception as e:
            wait = _retry_after_seconds(e)
            if i < attempts - 1:
                if wait is not None:
                    time.sleep(max(0.05, wait))
                else:
                    time.sleep(0.15 * (i + 1))
                continue
            return None
    return None
# =========================
# 🚀 BROADCAST WORKER
# =========================

def broadcast_worker():

    while True:
        job = broadcast_queue.get()

        try:
            if job["type"] == "single":
                _process_single(job["message"])

            elif job["type"] == "album":
                _process_album(job["messages"])
                # external_forward.forward_single(bot, message)


        except Exception as e:
            print("Broadcast error:", e)

        broadcast_queue.task_done()
# =========================
# 📤 PROCESS SINGLE MESSAGE
# =========================

def _process_single(message):

    sender_id = message.chat.id
    receivers = [uid for uid in get_receivers_cached() if uid != sender_id]
    mappings = []
    now = int(time.time())
    prefix = build_prefix(sender_id)
    if not receivers:
        return
    if message.content_type == "text":
        text_to_send = prefix + (message.text or "")
        send_fn = lambda uid: _send_text_with_retry(uid, text_to_send)
    else:
        send_fn = lambda uid: _copy_message_with_retry(uid, sender_id, message.message_id)
    workers = max(1, min(SEND_MAX_WORKERS, len(receivers)))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_uid = {
            executor.submit(send_fn, user_id): user_id
            for user_id in receivers
        }
        for future in as_completed(future_to_uid):
            user_id = future_to_uid[future]
            try:
                sent = future.result()
                if sent:
                    mappings.append((sent.message_id, sender_id, user_id, now))
            except Exception as e:
                print("Single send error:", e)
    save_mappings(mappings)

   
# =========================
# 📸 PROCESS ALBUM MESSAGE
# =========================

def _process_album(messages):

    sender_id = messages[0].chat.id
    receivers = [uid for uid in get_receivers_cached() if uid != sender_id]
    mappings = []
    now = int(time.time())
    if not receivers:
        return
    prefix = build_prefix(sender_id)
    media_objects = []
    for index, msg in enumerate(messages):
        if msg.content_type == "photo":
            media_objects.append(
                InputMediaPhoto(
                    media=msg.photo[-1].file_id,
                    caption=(prefix if index == 0 else None),
                )
            )
        elif msg.content_type == "video":
            media_objects.append(
                InputMediaVideo(
                    media=msg.video.file_id,
                    caption=(prefix if index == 0 else None),
                )
            )
    chunks = [media_objects[i:i+10] for i in range(0, len(media_objects), 10)]

    def send_album_to_user(user_id):
        rows = []
        for chunk in chunks:
            sent_msgs = bot.send_media_group(user_id, chunk)
            for sent in sent_msgs:
                rows.append((sent.message_id, sender_id, user_id, now))
            if FORWARD_DELAY > 0:
                time.sleep(FORWARD_DELAY)
        return rows

    workers = max(1, min(SEND_MAX_WORKERS, len(receivers)))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(send_album_to_user, user_id) for user_id in receivers]
        for future in as_completed(futures):
            try:
                mappings.extend(future.result())
            except Exception as e:
                print("Album send error:", e)
    save_mappings(mappings)
    # external_forward.forward_album(bot, messages)

# =========================
# 🔁 RELAY HANDLER
# =========================

@bot.message_handler(
    func=lambda m: not m.text or not m.text.startswith('/'),
    content_types=['text', 'photo', 'video']
)
def relay(message):

    if is_maintenance_mode() and not is_admin(message.chat.id):
        bot.send_message(message.chat.id, "Bot is under maintenance. Try again later.")
        return

    if handle_restrictions(message):
        return
    # =========================
    # ♻ DUPLICATE FILTER (EARLY)
    # =========================
    if message.content_type in ['photo', 'video'] and is_duplicate_filter_enabled():

        file_id = (
            message.photo[-1].file_id
            if message.content_type == 'photo'
            else message.video.file_id
        )

        is_dup = check_and_register_duplicate(file_id, message.chat.id)

        if is_dup:
            return  # silently ignore and DO NOT count activation
    # =========================
    # 1️⃣ TELEGRAM ALBUM
    # =========================
    if message.media_group_id:

        group_id = message.media_group_id
        media_groups[group_id].append(message)

        if group_id in album_timers:
            return

        album_timers[group_id] = True

        def finalize():
            time.sleep(1.0)

            album = media_groups.pop(group_id, [])
            album_timers.pop(group_id, None)

            if album:
                broadcast_queue.put({
                    "type": "album",
                    "messages": album
                })

        threading.Thread(target=finalize).start()
        return

    # =========================
    # 2️⃣ MANUAL MEDIA BUFFER
    # =========================
    if message.content_type in ['photo', 'video']:

        user_id = message.chat.id

        with media_buffer_lock:
            user_media_buffer[user_id].append(message)

            if user_id in user_media_timer:
                return

            user_media_timer[user_id] = True

        def finalize_user():
            time.sleep(1.2)

            with media_buffer_lock:
                media_list = user_media_buffer.pop(user_id, [])
                user_media_timer.pop(user_id, None)

            if len(media_list) == 1:
                broadcast_queue.put({
                    "type": "single",
                    "message": media_list[0]
                })
            else:
                broadcast_queue.put({
                    "type": "album",
                    "messages": media_list
                })

        threading.Thread(target=finalize_user).start()
        return

    # =========================
    # 3️⃣ TEXT
    # =========================
    broadcast_queue.put({
        "type": "single",
        "message": message
    })
# =========================
# ⏳ INACTIVITY SCHEDULER
# =========================

def inactivity_scheduler():

    while True:
        try:
            auto_ban_inactive_users()
        except Exception as e:
            print("Inactivity scheduler error:", e)

        time.sleep(60)  # check every 60 seconds
# =========================
# 🧹 MESSAGE MAP CLEANUP
# =========================

MAP_RETENTION_DAYS = 7

def message_map_cleanup_scheduler():

    while True:
        try:
            cutoff = int(time.time()) - (MAP_RETENTION_DAYS * 86400)

            with get_connection() as conn:
                with conn.cursor() as c:
                    c.execute("""
                        DELETE FROM message_map
                        WHERE created_at < %s
                    """, (cutoff,))
        except Exception as e:
            print("Cleanup error:", e)

        time.sleep(3600)  # run every hour
# =========================
# 🚀 START BACKGROUND WORKERS
# =========================

def start_background_workers():

    # Broadcast Worker
    threading.Thread(
        target=broadcast_worker,
        daemon=True
    ).start()

    # Inactivity Scheduler
    threading.Thread(
        target=inactivity_scheduler,
        daemon=True
    ).start()

    # Cleanup Scheduler
    threading.Thread(
        target=message_map_cleanup_scheduler,
        daemon=True
    ).start()
    
# =========================
# ADMIN COMMANDS
# ========================
@bot.message_handler(commands=['dupon'])
def enable_duplicate_filter(message):
    if not is_admin(message.chat.id):
        bot.send_message(message.chat.id, "Not admin.")
        return

    set_duplicate_filter(True)
    bot.send_message(message.chat.id, "✅ Duplicate filter ENABLED.")


@bot.message_handler(commands=['dupoff'])
def disable_duplicate_filter(message):
    if not is_admin(message.chat.id):
        bot.send_message(message.chat.id, "Not admin.")
        return

    set_duplicate_filter(False)
    bot.send_message(message.chat.id, "❌ Duplicate filter DISABLED.")


@bot.message_handler(commands=['dupstatus'])
def duplicate_status(message):
    if not is_admin(message.chat.id):
        return

    status = "ON" if is_duplicate_filter_enabled() else "OFF"
    bot.send_message(message.chat.id, f"♻ Duplicate filter is {status}")

    set_duplicate_filter(False)
    bot.send_message(message.chat.id, "❌ Duplicate filter disabled.")
    
@bot.message_handler(commands=['del'])
def delete_command(message):

    if not is_admin(message.chat.id):
        return

    if not message.reply_to_message:
        bot.send_message(message.chat.id, "Reply to a relayed message.")
        return

    bot_msg_id = message.reply_to_message.message_id

    delete_message_globally(bot_msg_id)

    bot.send_message(message.chat.id, "🗑 Message deleted everywhere.")
@bot.message_handler(commands=['addforward'])
def add_forward_target_cmd(message):

    if not is_admin(message.chat.id):
        return

    parts = message.text.split()

    if len(parts) < 2:
        bot.send_message(message.chat.id, "Usage: /addforward CHAT_ID")
        return

    chat_id = int(parts[1])
    # external_forward.add_forward_target(chat_id)

    bot.send_message(message.chat.id, "Forward target added.")

@bot.message_handler(commands=['purge'])
def purge_command(message):

    if not is_admin(message.chat.id):
        return

    if not message.reply_to_message:
        bot.send_message(message.chat.id, "Reply to a relayed message.")
        return

    bot_msg_id = message.reply_to_message.message_id
    user_id = get_original_sender(bot_msg_id)

    if not user_id:
        bot.send_message(message.chat.id, "User not found.")
        return

    purge_user_messages(user_id)
    bot.send_message(message.chat.id, "🔥 User messages purged.")
@bot.message_handler(commands=['panel'])
def admin_panel(message):

    if not is_admin(message.chat.id):
        return

    markup = InlineKeyboardMarkup(row_width=2)

    markup.add(
        InlineKeyboardButton("📊 Stats", callback_data="admin_stats"),
        InlineKeyboardButton("👥 Users", callback_data="admin_users")
    )

    markup.add(
        InlineKeyboardButton("🚪 Open Join", callback_data="admin_open_join"),
        InlineKeyboardButton("🔒 Close Join", callback_data="admin_close_join")
    )

    markup.add(
        InlineKeyboardButton("⭐ Whitelist", callback_data="admin_whitelist"),
        InlineKeyboardButton("🧹 Clear Map", callback_data="admin_clearmap")
    )

    markup.add(
        InlineKeyboardButton("🚫 Banned List", callback_data="admin_banned"),
        InlineKeyboardButton("⚙ Settings", callback_data="admin_settings")
    )

    bot.send_message(
        message.chat.id,
        "🛠 Admin Control Panel",
        reply_markup=markup
    )
@bot.message_handler(commands=['setwelcome'])
def set_welcome_cmd(message):

    if not is_admin(message.chat.id):
        return

    if not message.reply_to_message:
        bot.send_message(
            message.chat.id,
            "Reply to a message to set it as welcome message."
        )
        return

    new_text = message.reply_to_message.text

    if not new_text:
        bot.send_message(message.chat.id, "Text only.")
        return

    set_welcome_message(new_text)

    bot.send_message(
        message.chat.id,
        "✅ Welcome message updated."
    )

@bot.message_handler(commands=['stats'])
def stats_command(message):

    if not is_admin(message.chat.id):
        return

    active_cutoff = int(time.time()) - INACTIVITY_LIMIT
    with get_connection() as conn:
        with conn.cursor() as c:

            c.execute("SELECT COUNT(*) FROM users")
            total = c.fetchone()[0]

            c.execute("""
                SELECT COUNT(*) FROM users u
                LEFT JOIN admins a ON u.user_id = a.user_id
                WHERE u.banned=FALSE
                  AND u.username IS NOT NULL
                  AND (
                        a.user_id IS NOT NULL
                        OR u.whitelisted=TRUE
                        OR (
                            u.last_activation_time IS NOT NULL
                            AND u.last_activation_time >= %s
                        )
                      )
            """, (active_cutoff,))
            active = c.fetchone()[0]

            c.execute("""
                SELECT COUNT(*) FROM users u
                LEFT JOIN admins a ON u.user_id = a.user_id
                WHERE u.banned=FALSE
                  AND u.username IS NOT NULL
                  AND a.user_id IS NULL
                  AND u.whitelisted=FALSE
                  AND u.last_activation_time IS NOT NULL
                  AND u.last_activation_time < %s
            """, (active_cutoff,))
            inactive = c.fetchone()[0]

            c.execute("SELECT COUNT(*) FROM users WHERE banned=TRUE")
            banned = c.fetchone()[0]

            c.execute("SELECT COUNT(*) FROM users WHERE whitelisted=TRUE")
            whitelisted = c.fetchone()[0]

            c.execute("SELECT COUNT(*) FROM message_map")
            map_count = c.fetchone()[0]
            c.execute("SELECT COALESCE(SUM(duplicate_count), 0) FROM media_duplicates")
            duplicate_total = c.fetchone()[0]
    join_status = "OPEN" if is_join_open() else "CLOSED"

    bot.send_message(
        message.chat.id,
        f"""
📊 BOT STATS

👥 Total: {total}
🟢 Active: {active}
🔴 Inactive: {inactive}
🚫 Banned: {banned}
⭐ Whitelisted: {whitelisted}
♻ Duplicate Media: {duplicate_total}
📦 Message Map Rows: {map_count}
🚪 Join: {join_status}
        """
    )
@bot.message_handler(commands=['info'])
def info_command(message):

    if not is_admin(message.chat.id):
        return

    if not message.reply_to_message:
        bot.send_message(message.chat.id, "Reply to a relayed message.")
        return

    bot_msg_id = message.reply_to_message.message_id
    user_id = get_original_sender(bot_msg_id)

    if not user_id:
        bot.send_message(message.chat.id, "User not found.")
        return

    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT username,
                       banned,
                       auto_banned,
                       whitelisted,
                       activation_media_count,
                       total_media_sent,
                       last_activation_time
                FROM users
                WHERE user_id=%s
            """, (user_id,))
            row = c.fetchone()

    if not row:
        bot.send_message(message.chat.id, "User not found.")
        return

    username, banned, auto_banned, whitelisted, act_count, total_media, last_time = row

    bot.send_message(
        message.chat.id,
        f"""
👤 USER INFO

🆔 ID: {user_id}
🏷 Username: {username}
📸 Activation Media: {act_count}
📦 Total Media Sent: {total_media}

🚫 Manual Ban: {banned}
⏳ Auto Ban: {auto_banned}
⭐ Whitelisted: {whitelisted}
        """
    )

@bot.message_handler(commands=['ban'])
def ban_command(message):

    if not is_admin(message.chat.id):
        return

    target_id = None

    # 🔹 1️⃣ If used as reply
    if message.reply_to_message:
        bot_msg_id = message.reply_to_message.message_id
        target_id = get_original_sender(bot_msg_id)

        if not target_id:
            bot.send_message(message.chat.id, "User not found.")
            return

    # 🔹 2️⃣ If used with ID
    else:
        parts = message.text.split()

        if len(parts) < 2:
            bot.send_message(message.chat.id, "Usage:\n/ban USER_ID\nor reply to a relayed message.")
            return

        try:
            target_id = int(parts[1])
        except:
            bot.send_message(message.chat.id, "Invalid USER_ID.")
            return

    # 🔒 Final validation
    if not user_exists(target_id):
        bot.send_message(message.chat.id, "User not found in database.")
        return

    if is_admin(target_id):
        bot.send_message(message.chat.id, "You cannot ban another admin.")
        return

    ban_user(target_id)

    bot.send_message(
        message.chat.id,
        f"🚫 User {target_id} banned."
    )
@bot.message_handler(commands=['unban'])
def unban_command(message):

    if not is_admin(message.chat.id):
        return

    target_id = None

    # 🔹 1️⃣ If used as reply
    if message.reply_to_message:
        bot_msg_id = message.reply_to_message.message_id
        target_id = get_original_sender(bot_msg_id)

        if not target_id:
            bot.send_message(message.chat.id, "User not found.")
            return

    # 🔹 2️⃣ If used with ID
    else:
        parts = message.text.split()

        if len(parts) < 2:
            bot.send_message(
                message.chat.id,
                "Usage:\n/unban USER_ID\nor reply to a relayed message."
            )
            return

        try:
            target_id = int(parts[1])
        except:
            bot.send_message(message.chat.id, "Invalid USER_ID.")
            return

    # 🔍 Final validation
    if not user_exists(target_id):
        bot.send_message(message.chat.id, "User not found in database.")
        return

    unban_user(target_id)

    bot.send_message(
        message.chat.id,
        f"✅ User {target_id} unbanned."
    )
@bot.message_handler(commands=['addadmin'])
def addadmin_command(message):

    if not is_admin(message.chat.id):
        return

    parts = message.text.split()

    if len(parts) < 2:
        return

    add_admin(int(parts[1]))
    bot.send_message(message.chat.id, "Admin added.")
@bot.message_handler(commands=['removeadmin'])
def removeadmin_command(message):

    if not is_admin(message.chat.id):
        return

    parts = message.text.split()

    if len(parts) < 2:
        return

    remove_admin(int(parts[1]))
    bot.send_message(message.chat.id, "Admin removed.")
@bot.message_handler(commands=['openjoin'])
def openjoin_command(message):

    if not is_admin(message.chat.id):
        return

    set_join_status(True)
    bot.send_message(message.chat.id, "Join opened.")
@bot.message_handler(commands=['closejoin'])
def closejoin_command(message):

    if not is_admin(message.chat.id):
        return

    set_join_status(False)
    bot.send_message(message.chat.id, "Join closed.")
@bot.message_handler(commands=['clearmap'])
def clearmap_command(message):

    if not is_admin(message.chat.id):
        return

    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("DELETE FROM message_map")

    bot.send_message(message.chat.id, "Message map cleared.")
@bot.message_handler(commands=['whitelist'])
def whitelist_command(message):

    if not is_admin(message.chat.id):
        return

    parts = message.text.split()

    if len(parts) < 2:
        bot.send_message(message.chat.id, "Usage: /whitelist USER_ID")
        return

    try:
        target_id = int(parts[1])
    except:
        bot.send_message(message.chat.id, "Invalid USER_ID.")
        return

    whitelist_user(target_id)

    bot.send_message(
        message.chat.id,
        f"⭐ User {target_id} added to whitelist."
    )
@bot.message_handler(commands=['unwhitelist'])
def unwhitelist_command(message):

    if not is_admin(message.chat.id):
        return

    parts = message.text.split()

    if len(parts) < 2:
        bot.send_message(message.chat.id, "Usage: /unwhitelist USER_ID")
        return

    try:
        target_id = int(parts[1])
    except:
        bot.send_message(message.chat.id, "Invalid USER_ID.")
        return

    remove_whitelist(target_id)

    bot.send_message(
        message.chat.id,
        f"❌ User {target_id} removed from whitelist."
    )
@bot.message_handler(commands=['closebot'])
def closebot_command(message):
    if not is_admin(message.chat.id):
        return
    set_maintenance_mode(True)
    bot.send_message(message.chat.id, "Maintenance mode enabled. Bot is closed for users.")


@bot.message_handler(commands=['openbot'])
def openbot_command(message):
    if not is_admin(message.chat.id):
        return
    set_maintenance_mode(False)
    bot.send_message(message.chat.id, "Maintenance mode disabled. Bot is open now.")


@bot.message_handler(commands=['adminmenu'])
def admin_menu(message):

    if not is_admin(message.chat.id):
        return

    bot.send_message(
        message.chat.id,
        """
🛠 ADMIN COMMAND MENU

📊 /stats  
→ Show bot statistics

🔎 /info USER_ID  
→ View user details

🚫 /ban USER_ID  
→ Manually ban user

✅ /unban USER_ID  
→ Remove manual ban

⭐ /whitelist USER_ID  
→ Bypass activation/inactivity

❌ /unwhitelist USER_ID  
→ Remove whitelist access

👑 /addadmin USER_ID  
→ Add new admin

🗑 /removeadmin USER_ID  
→ Remove admin

🚪 /openjoin  
→ Allow new users to join

🔒 /closejoin  
→ Stop new users from joining

🧹 /clearmap  
→ Clear message mapping table

📦 /addword WORD  
→ Add banned word

❌ /removeword WORD  
→ Remove banned word

📃 /words  
→ Show banned words list
        """
    )
@bot.callback_query_handler(func=lambda call: call.data.startswith("admin_"))
def admin_callbacks(call):

    if not is_admin(call.message.chat.id):
        return

    data = call.data

    if data == "admin_stats":
        stats_command(call.message)

    elif data == "admin_open_join":
        set_join_status(True)
        bot.answer_callback_query(call.id, "Join opened.")

    elif data == "admin_close_join":
        set_join_status(False)
        bot.answer_callback_query(call.id, "Join closed.")

    elif data == "admin_clearmap":
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("DELETE FROM message_map")
        bot.answer_callback_query(call.id, "Message map cleared.")

    elif data == "admin_banned":
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("""
                    SELECT user_id FROM users WHERE banned=TRUE
                """)
                rows = c.fetchall()

        if rows:
            text = "\n".join(str(r[0]) for r in rows)
        else:
            text = "No banned users."

        bot.send_message(call.message.chat.id, text)

    bot.answer_callback_query(call.id)
@bot.message_handler(commands=['chatid'], content_types=['text'])
def get_chat_id(message):
    bot.reply_to(message, f"Chat ID: {message.chat.id}")
@bot.channel_post_handler(commands=['cchatid'])
def get_channel_id(message):
    bot.send_message(message.chat.id, f"Channel ID: {message.chat.id}")

# =========================
# 🚀 MAIN BOOT
# =========================

if __name__ == "__main__":

    print("Starting bot...")

    init_db_pool()
    init_db()
    print("Database ready.")

    start_background_workers()
    print("Background workers running.")

    bot.infinity_polling(skip_pending=True)


