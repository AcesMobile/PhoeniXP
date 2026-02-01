import os
import math
import time
import sqlite3
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

def now():
    return int(time.time())

def minute_bucket(ts):
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

def get_user(conn, guild_id, user_id):
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

def clamp_xp(xp):
    return max(0, min(MAX_XP, xp))

def rank_from_xp(xp):
    if xp < 3:
        return ROLE_NAMES[0]
    if xp < MAX_XP * 0.4:
        return ROLE_NAMES[1]
    if xp < MAX_XP * 0.65:
        return ROLE_NAMES[2]
    if xp < MAX_XP * 0.9:
        return ROLE_NAMES[3]
    return ROLE_NAMES[4]

def try_award_xp(conn, guild_id, user_id, amount):
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

def try_award_xp_at(conn, guild_id, user_id, amount, ts: int):
    """
    Same logic as try_award_xp, but uses a provided timestamp (ts)
    so audit/backfill respects historical cooldown + per-minute cap.
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


async def sync_role(member, xp):
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
    except:
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
async def on_message(msg):
    if msg.author.bot or not msg.guild:
        return

    await bot.process_commands(msg)

    if len(msg.content.strip()) < MIN_MESSAGE_CHARS:
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
                if m.voice.deaf or m.voice.self_deaf:
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

    # Build a complete member map (not cache-dependent)
    members = {}
    async for m in guild.fetch_members(limit=None):
        members[m.id] = m

    # Ensure EVERY member has a DB row (so nobody is missing)
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

    # If huge, send as file
    content = "\n".join(lines) if lines else "No users yet."
    data = content.encode("utf-8")
    file = discord.File(fp=io.BytesIO(data), filename="leaderboard.txt")

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

    # Optional: lock to admins
    if not interaction.user.guild_permissions.administrator:
        return await interaction.response.send_message("Admin only.", ephemeral=True)

    await interaction.response.send_message(
        f"Auditing last **{days}** day(s) of chat…",
        ephemeral=True
    )

    guild = interaction.guild
    cutoff_dt = datetime.utcnow() - timedelta(days=days)

    scanned = 0
    awarded_total = 0
    per_user = {}  # user_id -> gained xp

    for channel in guild.text_channels:
        perms = channel.permissions_for(guild.me)
        if not (perms.read_messages and perms.read_message_history):
            continue

        try:
            async for msg in channel.history(after=cutoff_dt, oldest_first=True, limit=None):
                scanned += 1

                if msg.author.bot:
                    continue

                content = (msg.content or "").strip()
                if len(content) < MIN_MESSAGE_CHARS:
                    continue

                ts = int(msg.created_at.timestamp())

                with db() as conn:
                    user = get_user(conn, guild.id, msg.author.id)

                    # cooldown check based on message time (historical)
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
            # some channels may fail history() due to perms/age/etc
            continue

    # apply roles after audit
    updated = 0
    for user_id, gained in per_user.items():
        member = guild.get_member(user_id)
        if not member:
            continue
        with db() as conn:
            xp = get_user(conn, guild.id, user_id)["xp"]
        await sync_role(member, xp)
        updated += 1

    top = sorted(per_user.items(), key=lambda x: x[1], reverse=True)[:10]
    lines = []
    for i, (uid, gained) in enumerate(top, start=1):
        m = guild.get_member(uid)
        name = m.display_name if m else f"User {uid}"
        lines.append(f"{i}. **{name}** +{gained} XP")

    await interaction.followup.send(
        "✅ **Audit complete**\n"
        f"Scanned messages: **{scanned}**\n"
        f"Users updated: **{updated}**\n"
        f"Total XP awarded: **{awarded_total}**\n\n"
        "**Top gains**\n" + ("\n".join(lines) if lines else "No awards in this window."),
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

