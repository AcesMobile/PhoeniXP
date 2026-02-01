# main.py
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

ROLE_NAMES = [
    "Initiate",
    "Operative",
    "Ember",
    "Ascendant",
    "Phoenix Prime"
]

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
    return max(0, min(MAX_XP, xp))


def rank_from_xp(xp: int) -> str:
    # Initiate should be basically “joined but didn’t engage”.
    # You wanted 3 qualifying messages → 3 XP (since CHAT_XP_PER_TICK = 1)
    if xp < 3:
        return ROLE_NAMES[0]
    if xp < MAX_XP * 0.4:
        return ROLE_NAMES[1]
    if xp < MAX_XP * 0.65:
        return ROLE_NAMES[2]
    if xp < MAX_XP * 0.9:
        return ROLE_NAMES[3]
    return ROLE_NAMES[4]


def try_award_xp_at(conn, guild_id: int, user_id: int, amount: int, ts: int) -> int:
    """
    Award XP using a provided timestamp.
    Enforces per-minute cap via last_minute/earned_this_minute.
    Does NOT set chat_cooldown here; caller controls cooldown behavior.
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
        SET xp=?, last_active=?, last_minute=?, earned_this_minute=?
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


def try_award_xp(conn, guild_id: int, user_id: int, amount: int) -> int:
    """Live award using current time."""
    return try_award_xp_at(conn, guild_id, user_id, amount, now())


async def sync_role(member: discord.Member, xp: int):
    target = rank_from_xp(xp)
    roles = {r.name: r for r in member.guild.roles}

    if target not in roles:
        return

    to_add = roles[target]
    to_remove = [
        roles[name] for name in ROLE_NAMES
        if name in roles and roles[name] in member.roles and name != target
    ]

    try:
        if to_remove:
            await member.remove_roles(*to_remove)
        if to_add not in member.roles:
            await member.add_roles(to_add)
    except Exception:
        # ignore permission/role hierarchy issues quietly
        pass


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

        # set cooldown (live)
        conn.execute(
            "UPDATE users SET chat_cooldown=? WHERE guild_id=? AND user_id=?",
            (now() + CHAT_COOLDOWN_SECONDS, msg.guild.id, msg.author.id)
        )
        conn.commit()

        if awarded:
            xp = get_user(conn, msg.guild.id, msg.author.id)["xp"]
            await sync_role(msg.author, xp)


# =========================
# VOICE XP (LIVE ONLY)
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
                        xp = get_user(conn, guild.id, m.id)["xp"]
                        await sync_role(m, xp)


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

    await interaction.response.send_message(
        f"XP: **{user['xp']} / {MAX_XP}**\nRank: **{rank_from_xp(user['xp'])}**",
        ephemeral=True
    )


@bot.tree.command(name="leaderboard")
async def leaderboard(interaction: discord.Interaction):
    if not interaction.guild:
        return await interaction.response.send_message("Guild only.", ephemeral=True)

    guild = interaction.guild
    await interaction.response.defer(ephemeral=True)

    # Fetch ALL members (don’t rely on cache)
    members = {}
    async for m in guild.fetch_members(limit=None):
        if not m.bot:
            members[m.id] = m

    # Ensure EVERY member has a DB row
    with db() as conn:
        for uid in members.keys():
            get_user(conn, guild.id, uid)

        rows = conn.execute(
            "SELECT user_id, xp FROM users WHERE guild_id=? ORDER BY xp DESC, user_id ASC",
            (guild.id,)
        ).fetchall()

    lines = []
    for i, r in enumerate(rows, start=1):
        uid = r["user_id"]
        xp = r["xp"]
        m = members.get(uid)
        name = m.display_name if m else f"User {uid}"
        rank = rank_from_xp(xp)
        lines.append(f"{i:>4}. {name} — {xp} XP — {rank}")

    content = "\n".join(lines) if lines else "No users yet."
    file = discord.File(fp=io.BytesIO(content.encode("utf-8")), filename="leaderboard.txt")

    await interaction.followup.send(
        "✅ Full leaderboard (everyone + rank):",
        file=file,
        ephemeral=True
    )


# ---- IMPORTANT: only ONE audit command (you had two) ----
@bot.tree.command(name="audit")
@app_commands.describe(days="How many days back to scan (default 30)")
async def audit(interaction: discord.Interaction, days: int = 30):
    if not interaction.guild:
        return await interaction.response.send_message("Guild only.", ephemeral=True)

    # admin-only
    if not interaction.user.guild_permissions.administrator:
        return await interaction.response.send_message("Admin only.", ephemeral=True)

    guild = interaction.guild
    cutoff_dt = datetime.utcnow() - timedelta(days=days)

    await interaction.response.defer(ephemeral=True)

    # Fetch ALL members (don’t rely on cache)
    members = {}
    async for m in guild.fetch_members(limit=None):
        if not m.bot:
            members[m.id] = m

    # Seed DB rows for everyone + RESET cooldown state so historical audit isn't blocked.
    # This fixes “Trigger only gets 1 XP” caused by old msg timestamps being < stored cooldown timestamps.
    with db() as conn:
        for uid in members.keys():
            get_user(conn, guild.id, uid)

        conn.execute("""
            UPDATE users
            SET chat_cooldown=0,
                last_minute=0,
                earned_this_minute=0
            WHERE guild_id=?
        """, (guild.id,))
        conn.commit()

    scanned = 0
    awarded_total = 0
    skipped_histories = 0
    per_user = {}  # user_id -> gained xp

    def can_read_history(ch: discord.abc.GuildChannel) -> bool:
        me = guild.me
        if not me:
            return False
        perms = ch.permissions_for(me)
        return bool(perms.view_channel and perms.read_message_history)

    async def scan_history(messageable):
        nonlocal scanned, awarded_total, skipped_histories, per_user

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

                    # historical cooldown check (now safe because we reset before scan)
                    if ts < user["chat_cooldown"]:
                        continue

                    awarded = try_award_xp_at(conn, guild.id, msg.author.id, CHAT_XP_PER_TICK, ts)

                    # set cooldown relative to message time (historical)
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
        try:
            for th in ch.threads:
                await scan_history(th)
        except Exception:
            pass

        # archived public threads
        try:
            async for th in ch.archived_threads(limit=None, private=False):
                await scan_history(th)
        except Exception:
            pass

        # archived private threads
        try:
            async for th in ch.archived_threads(limit=None, private=True):
                await scan_history(th)
        except Exception:
            pass

    async def scan_forum_channel(forum: discord.ForumChannel):
        # active threads
        try:
            for th in forum.threads:
                await scan_history(th)
        except Exception:
            pass

        # archived public
        try:
            async for th in forum.archived_threads(limit=None, private=False):
                await scan_history(th)
        except Exception:
            pass

        # archived private
        try:
            async for th in forum.archived_threads(limit=None, private=True):
                await scan_history(th)
        except Exception:
            pass

    # Scan all channels + threads/forums
    for ch in guild.channels:
        if isinstance(ch, discord.TextChannel):
            await scan_history(ch)
            await scan_threads_for_channel(ch)
        elif isinstance(ch, discord.ForumChannel):
            await scan_forum_channel(ch)

    # Sync roles for EVERY member (not just those with gains)
    updated = 0
    for uid, member in members.items():
        with db() as conn:
            xp = get_user(conn, guild.id, uid)["xp"]
        await sync_role(member, xp)
        updated += 1

    # Top gains + report file
    top = sorted(per_user.items(), key=lambda x: x[1], reverse=True)[:10]
    top_lines = []
    for i, (uid, gained) in enumerate(top, start=1):
        m = members.get(uid)
        name = m.display_name if m else f"User {uid}"
        top_lines.append(f"{i}. {name} +{gained} XP")

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

if not token:
    raise RuntimeError("DISCORD_TOKEN missing in Railway environment")
bot.run(token)
