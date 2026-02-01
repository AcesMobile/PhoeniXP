import os
import time
import sqlite3
import io
from datetime import datetime, timedelta

import discord
from discord import app_commands
from discord.ext import commands, tasks

# =========================
# CONFIG
# =========================
MAX_XP = 1500

CHAT_COOLDOWN_SECONDS = 60
CHAT_XP_PER_TICK = 1
VC_XP_PER_MIN = 1
PER_MINUTE_XP_CAP = 2

MIN_MESSAGE_CHARS = 4

DECAY_GRACE_HOURS = 72
DECAY_PERCENT_PER_DAY = 0.01
DECAY_MIN_XP_PER_DAY = 1

# Bot-managed ranks ONLY (Prime is manual)
ROLE_NAMES = [
    "Initiate",
    "Operative",
    "Ember",
    "Ascendant",
]

# Manual-only prestige role (bot never adds/removes it)
MANUAL_PRIME_ROLE = "Phoenix Prime"

# Rank thresholds tuned to your current distribution:
# - Initiate: < 3 XP (3 messages to escape Initiate)
# - Operative: 3–24 XP
# - Ember: 25–79 XP
# - Ascendant: 80+ XP (puts your current top ~5 into Ascendant)
INITIATE_EXIT_XP = 3
EMBER_XP = 25
ASCENDANT_XP = 80

# =========================
# DISCORD SETUP
# =========================
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.voice_states = True

bot = commands.Bot(command_prefix="!", intents=intents)

# =========================
# DATABASE
# =========================
DB_PATH = "xp.db"

def now() -> int:
    return int(time.time())

def minute_bucket(ts: int) -> int:
    return ts // 60

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with db() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            guild_id INTEGER,
            user_id INTEGER,
            xp INTEGER DEFAULT 0,
            last_active INTEGER DEFAULT 0,
            chat_cooldown INTEGER DEFAULT 0,
            last_minute INTEGER DEFAULT 0,
            earned_this_minute INTEGER DEFAULT 0,
            PRIMARY KEY (guild_id, user_id)
        )
        """)
        conn.commit()

def get_user(conn, guild_id: int, user_id: int):
    row = conn.execute(
        "SELECT * FROM users WHERE guild_id=? AND user_id=?",
        (guild_id, user_id)
    ).fetchone()
    if not row:
        conn.execute(
            "INSERT INTO users (guild_id, user_id) VALUES (?, ?)",
            (guild_id, user_id)
        )
        conn.commit()
        return get_user(conn, guild_id, user_id)
    return row

def clamp_xp(xp: int) -> int:
    return max(0, min(MAX_XP, int(xp)))

def rank_from_xp(xp: int) -> str:
    # Bot-managed only
    if xp < INITIATE_EXIT_XP:
        return ROLE_NAMES[0]  # Initiate
    if xp < EMBER_XP:
        return ROLE_NAMES[1]  # Operative
    if xp < ASCENDANT_XP:
        return ROLE_NAMES[2]  # Ember
    return ROLE_NAMES[3]      # Ascendant

def try_award_xp(conn, guild_id: int, user_id: int, amount: int) -> int:
    user = get_user(conn, guild_id, user_id)
    ts = now()
    bucket = minute_bucket(ts)

    earned = user["earned_this_minute"]
    if bucket != user["last_minute"]:
        earned = 0

    remaining = PER_MINUTE_XP_CAP - earned
    award = max(0, min(amount, remaining))

    new_xp = clamp_xp(user["xp"] + award)

    conn.execute("""
        UPDATE users
        SET xp=?, last_active=?, chat_cooldown=?,
            last_minute=?, earned_this_minute=?
        WHERE guild_id=? AND user_id=?
    """, (
        new_xp,
        ts if award > 0 else user["last_active"],
        user["chat_cooldown"],
        bucket,
        earned + award,
        guild_id,
        user_id
    ))
    conn.commit()

    return award

def try_award_xp_at(conn, guild_id: int, user_id: int, amount: int, ts: int) -> int:
    """
    Same logic as try_award_xp, but uses a provided timestamp (ts)
    so audit/backfill respects historical per-minute cap.
    (Cooldown is enforced outside this function.)
    """
    user = get_user(conn, guild_id, user_id)
    bucket = minute_bucket(ts)

    earned = user["earned_this_minute"]
    if bucket != user["last_minute"]:
        earned = 0

    remaining = PER_MINUTE_XP_CAP - earned
    award = max(0, min(amount, remaining))

    new_xp = clamp_xp(user["xp"] + award)

    conn.execute("""
        UPDATE users
        SET xp=?, last_active=?,
            last_minute=?, earned_this_minute=?
        WHERE guild_id=? AND user_id=?
    """, (
        new_xp,
        ts if award > 0 else user["last_active"],
        bucket,
        earned + award,
        guild_id,
        user_id
    ))
    conn.commit()

    return award

async def sync_role(member: discord.Member, xp: int):
    """
    Keeps bot-managed roles aligned with XP.
    Does NOT remove Phoenix Prime (manual prestige role).
    (Prime can coexist with XP-rank roles.)
    """
    target = rank_from_xp(xp)
    roles = {r.name: r for r in member.guild.roles}

    if target not in roles:
        return

    to_add = roles[target]

    # remove only bot-managed ranks (ROLE_NAMES), never Prime
    to_remove = [
        roles[name] for name in ROLE_NAMES
        if name in roles and roles[name] in member.roles and name != target
    ]

    try:
        if to_remove:
            await member.remove_roles(*to_remove, reason="XP rank sync")
        if to_add not in member.roles:
            await member.add_roles(to_add, reason="XP rank sync")
    except Exception:
        pass

def display_rank(member: discord.Member, xp: int) -> str:
    """
    What we SHOW in commands:
    - Always compute XP-rank.
    - If they have Phoenix Prime, show it as a prestige overlay too.
      Example: "Phoenix Prime + Ascendant"
    """
    xp_rank = rank_from_xp(xp)
    has_prime = any(r.name == MANUAL_PRIME_ROLE for r in member.roles)
    if has_prime:
        return f"{MANUAL_PRIME_ROLE} + {xp_rank}"
    return xp_rank

# =========================
# EVENTS
# =========================
@bot.event
async def on_ready():
    init_db()
    await bot.tree.sync()
    vc_tick.start()
    decay_loop.start()
    print(f"Logged in as {bot.user}")

@bot.event
async def on_message(msg: discord.Message):
    if msg.author.bot or not msg.guild:
        return

    await bot.process_commands(msg)

    content = (msg.content or "").strip()
    if len(content) < MIN_MESSAGE_CHARS:
        return

    with db() as conn:
        user = get_user(conn, msg.guild.id, msg.author.id)
        if now() < user["chat_cooldown"]:
            return

        awarded = try_award_xp(conn, msg.guild.id, msg.author.id, CHAT_XP_PER_TICK)
        conn.execute(
            "UPDATE users SET chat_cooldown=? WHERE guild_id=? AND user_id=?",
            (now() + CHAT_COOLDOWN_SECONDS, msg.guild.id, msg.author.id)
        )
        conn.commit()

        if awarded:
            await sync_role(msg.author, get_user(conn, msg.guild.id, msg.author.id)["xp"])

# =========================
# VOICE XP
# =========================
@tasks.loop(seconds=60)
async def vc_tick():
    for guild in bot.guilds:
        for vc in guild.voice_channels:
            humans = [m for m in vc.members if not m.bot]
            if len(humans) < 2:
                continue

            for m in humans:
                if m.voice and (m.voice.deaf or m.voice.self_deaf):
                    continue

                with db() as conn:
                    awarded = try_award_xp(conn, guild.id, m.id, VC_XP_PER_MIN)
                    if awarded:
                        await sync_role(m, get_user(conn, guild.id, m.id)["xp"])

# =========================
# DECAY
# =========================
@tasks.loop(hours=24)
async def decay_loop():
    cutoff = now() - (DECAY_GRACE_HOURS * 3600)
    with db() as conn:
        rows = conn.execute("SELECT * FROM users").fetchall()
        for r in rows:
            if r["xp"] <= 0 or r["last_active"] >= cutoff:
                continue

            loss = max(int(r["xp"] * DECAY_PERCENT_PER_DAY), DECAY_MIN_XP_PER_DAY)
            new_xp = clamp_xp(r["xp"] - loss)

            conn.execute(
                "UPDATE users SET xp=? WHERE guild_id=? AND user_id=?",
                (new_xp, r["guild_id"], r["user_id"])
            )
            conn.commit()

            guild = bot.get_guild(r["guild_id"])
            if guild:
                member = guild.get_member(r["user_id"])
                if member:
                    await sync_role(member, new_xp)

# =========================
# SLASH COMMANDS
# =========================
@bot.tree.command(name="balance")
async def balance(interaction: discord.Interaction):
    if not interaction.guild:
        return await interaction.response.send_message("Guild only.", ephemeral=True)

    with db() as conn:
        user = get_user(conn, interaction.guild.id, interaction.user.id)

    rank = display_rank(interaction.user, user["xp"])
    await interaction.response.send_message(
        f"XP: **{user['xp']} / {MAX_XP}**\nRank: **{rank}**",
        ephemeral=True
    )

@bot.tree.command(name="leaderboard")
async def leaderboard(interaction: discord.Interaction):
    if not interaction.guild:
        return await interaction.response.send_message("Guild only.", ephemeral=True)

    guild = interaction.guild
    await interaction.response.defer(ephemeral=True)

    # Build a complete member map (not cache-dependent)
    members: dict[int, discord.Member] = {}
    async for m in guild.fetch_members(limit=None):
        members[m.id] = m

    # Ensure EVERY non-bot member has a DB row (so nobody is missing)
    with db() as conn:
        for uid, m in members.items():
            if not m.bot:
                get_user(conn, guild.id, uid)

        rows = conn.execute(
            "SELECT user_id, xp FROM users WHERE guild_id=? ORDER BY xp DESC, user_id ASC",
            (guild.id,)
        ).fetchall()

    lines = []
    place = 0
    for r in rows:
        uid = r["user_id"]
        xp = r["xp"]
        m = members.get(uid)
        if not m or m.bot:
            continue

        place += 1
        rank = display_rank(m, xp)
        lines.append(f"{place:>4}. {m.display_name} — {xp} XP — {rank}")

    content = "\n".join(lines) if lines else "No users yet."
    file = discord.File(fp=io.BytesIO(content.encode("utf-8")), filename="leaderboard.txt")

    await interaction.followup.send(
        "✅ Full leaderboard (everyone + rank):",
        file=file,
        ephemeral=True
    )

@bot.tree.command(name="audit")
@app_commands.describe(days="How many days back to scan (default 30)")
async def audit(interaction: discord.Interaction, days: int = 30):
    if not interaction.guild:
        return await interaction.response.send_message("Guild only.", ephemeral=True)

    if not interaction.user.guild_permissions.administrator:
        return await interaction.response.send_message("Admin only.", ephemeral=True)

    guild = interaction.guild
    cutoff_dt = datetime.utcnow() - timedelta(days=days)

    await interaction.response.defer(ephemeral=True)

    def can_read_history(ch: discord.abc.GuildChannel) -> bool:
        me = guild.me
        if not me:
            return False
        perms = ch.permissions_for(me)
        return bool(perms.view_channel and perms.read_message_history)

    scanned = 0
    awarded_total = 0
    skipped_histories = 0
    per_user: dict[int, int] = {}

    # Ensure EVERY non-bot member exists in DB
    members: dict[int, discord.Member] = {}
    async for m in guild.fetch_members(limit=None):
        if not m.bot:
            members[m.id] = m

    with db() as conn:
        for uid in members.keys():
            get_user(conn, guild.id, uid)

    async def scan_history(messageable):
        nonlocal scanned, awarded_total, skipped_histories

        if not can_read_history(messageable):
            skipped_histories += 1
            return

        try:
            async for msg in messageable.history(after=cutoff_dt, oldest_first=True, limit=None):
                scanned += 1

                if msg.author.bot:
                    continue

                content = (msg.content or "").strip()
                if len(content) < MIN_MESSAGE_CHARS:
                    continue

                ts = int(msg.created_at.timestamp())

                with db() as conn:
                    user = get_user(conn, guild.id, msg.author.id)

                    # Historical cooldown check
                    if ts < user["chat_cooldown"]:
                        continue

                    awarded = try_award_xp_at(conn, guild.id, msg.author.id, CHAT_XP_PER_TICK, ts)

                    # Set cooldown relative to historical msg time
                    conn.execute(
                        "UPDATE users SET chat_cooldown=? WHERE guild_id=? AND user_id=?",
                        (ts + CHAT_COOLDOWN_SECONDS, guild.id, msg.author.id)
                    )
                    conn.commit()

                if awarded:
                    awarded_total += awarded
                    per_user[msg.author.id] = per_user.get(msg.author.id, 0) + awarded

        except Exception:
            skipped_histories += 1

    async def scan_threads_for_channel(ch: discord.TextChannel):
        # active threads
        for th in getattr(ch, "threads", []):
            await scan_history(th)

        # archived public threads
        try:
            async for th in ch.archived_threads(limit=None, private=False):
                await scan_history(th)
        except Exception:
            pass

        # archived private threads (requires perms)
        try:
            async for th in ch.archived_threads(limit=None, private=True):
                await scan_history(th)
        except Exception:
            pass

    async def scan_forum_channel(forum: discord.ForumChannel):
        try:
            for th in forum.threads:
                await scan_history(th)
        except Exception:
            pass

        try:
            async for th in forum.archived_threads(limit=None, private=False):
                await scan_history(th)
        except Exception:
            pass

        try:
            async for th in forum.archived_threads(limit=None, private=True):
                await scan_history(th)
        except Exception:
            pass

    # Scan all channels
    for ch in guild.channels:
        if isinstance(ch, discord.TextChannel):
            await scan_history(ch)
            await scan_threads_for_channel(ch)
        elif isinstance(ch, discord.ForumChannel):
            await scan_forum_channel(ch)

    # Sync roles for EVERY member (Prime unaffected; XP-rank roles still set)
    updated = 0
    for uid, member in members.items():
        with db() as conn:
            xp = get_user(conn, guild.id, uid)["xp"]
        await sync_role(member, xp)
        updated += 1

    # Top gains
    top = sorted(per_user.items(), key=lambda x: x[1], reverse=True)[:10]
    top_lines = []
    for i, (uid, gained) in enumerate(top, start=1):
        m = members.get(uid)
        name = m.display_name if m else f"User {uid}"
        top_lines.append(f"{i}. **{name}** +{gained} XP")

    # Report file
    report_lines = [
        f"Audit window: last {days} day(s)",
        f"Cutoff (UTC): {cutoff_dt.isoformat()}",
        f"Scanned messages: {scanned}",
        f"Users with gains: {len(per_user)}",
        f"Users role-synced: {updated}",
        f"Total XP awarded: {awarded_total}",
        f"Histories skipped (perms/errors): {skipped_histories}",
        "",
        "Top gains:",
        *(top_lines if top_lines else ["No awards in this window."]),
    ]
    report = "\n".join(report_lines).encode("utf-8")
    file = discord.File(fp=io.BytesIO(report), filename="audit_report.txt")

    await interaction.followup.send(
        "✅ **Audit complete**\n"
        f"Scanned messages: **{scanned}**\n"
        f"Users role-synced: **{updated}**\n"
        f"Total XP awarded: **{awarded_total}**\n"
        f"Skipped histories: **{skipped_histories}**\n\n"
        "**Top gains**\n" + ("\n".join(top_lines) if top_lines else "No awards in this window."),
        file=file,
        ephemeral=True
    )

# =========================
# RUN
# =========================
token = os.getenv("DISCORD_TOKEN")
print("DISCORD_TOKEN present?", bool(token), "len=", len(token) if token else 0)
if not token:
    raise RuntimeError("DISCORD_TOKEN missing in Railway environment")
bot.run(token)
