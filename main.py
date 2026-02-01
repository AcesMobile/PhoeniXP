import os, time, sqlite3, io
from datetime import datetime, timedelta

import discord
from discord import app_commands
from discord.ext import commands, tasks

# -------------------------
# CONFIG
# -------------------------
MAX_XP = 1500
MIN_MESSAGE_CHARS = 4

CHAT_COOLDOWN_SECONDS = 60
CHAT_XP_PER_TICK = 1
VC_XP_PER_MIN = 1
PER_MINUTE_XP_CAP = 2

DECAY_GRACE_HOURS = 72
DECAY_PERCENT_PER_DAY = 0.01
DECAY_MIN_XP_PER_DAY = 1
DECAY_FLOOR_XP = 3

INITIATE_EXIT_XP = 3
TOP_ASCENDANT = 5
NEXT_EMBER = 5

ROLE_INITIATE = "Initiate"
ROLE_OPERATIVE = "Operative"
ROLE_EMBER = "Ember"
ROLE_ASCENDANT = "Ascendant"
ROLE_NAMES = [ROLE_INITIATE, ROLE_OPERATIVE, ROLE_EMBER, ROLE_ASCENDANT]

MANUAL_PRIME_ROLE = "Phoenix Prime"

DB_PATH = "xp.db"

# -------------------------
# DISCORD
# -------------------------
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.voice_states = True
bot = commands.Bot(command_prefix="!", intents=intents)

# -------------------------
# DB
# -------------------------
def now(): return int(time.time())
def minute_bucket(ts): return ts // 60

def db():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c

def init_db():
    with db() as c:
        c.execute("""
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
        c.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """)
        c.commit()

def get_user(c, gid, uid):
    r = c.execute(
        "SELECT * FROM users WHERE guild_id=? AND user_id=?",
        (gid, uid)
    ).fetchone()
    if not r:
        c.execute(
            "INSERT INTO users (guild_id, user_id) VALUES (?,?)",
            (gid, uid)
        )
        c.commit()
        return get_user(c, gid, uid)
    return r

def clamp_xp(x): return max(0, min(MAX_XP, int(x)))

def is_admin(i):
    return bool(i.user and i.user.guild_permissions and i.user.guild_permissions.administrator)

def has_prime(m):
    return any(r.name == MANUAL_PRIME_ROLE for r in m.roles)

# -------------------------
# XP
# -------------------------
def award_xp(c, gid, uid, amount, ts=None):
    u = get_user(c, gid, uid)
    ts = ts or now()
    bucket = minute_bucket(ts)

    earned = u["earned_this_minute"]
    if bucket != u["last_minute"]:
        earned = 0

    remaining = PER_MINUTE_XP_CAP - earned
    award = max(0, min(amount, remaining))
    if not award:
        return 0

    new_xp = clamp_xp(u["xp"] + award)
    c.execute("""
        UPDATE users
        SET xp=?, last_active=?, last_minute=?, earned_this_minute=?
        WHERE guild_id=? AND user_id=?
    """, (new_xp, ts, bucket, earned + award, gid, uid))
    c.commit()
    return award

# -------------------------
# RANKING (TOP-X)
# -------------------------
def compute_rank_map(gid, member_ids):
    with db() as c:
        for uid in member_ids:
            get_user(c, gid, uid)
        rows = c.execute(
            "SELECT user_id, xp FROM users WHERE guild_id=?",
            (gid,)
        ).fetchall()

    xp = {r["user_id"]: r["xp"] for r in rows}
    eligible = [(uid, xp.get(uid, 0)) for uid in member_ids if xp.get(uid, 0) >= INITIATE_EXIT_XP]
    eligible.sort(key=lambda x: (-x[1], x[0]))

    topA = set(uid for uid, _ in eligible[:TOP_ASCENDANT])
    nextE = set(uid for uid, _ in eligible[TOP_ASCENDANT:TOP_ASCENDANT + NEXT_EMBER])

    out = {}
    for uid in member_ids:
        x = xp.get(uid, 0)
        if x < INITIATE_EXIT_XP: out[uid] = ROLE_INITIATE
        elif uid in topA: out[uid] = ROLE_ASCENDANT
        elif uid in nextE: out[uid] = ROLE_EMBER
        else: out[uid] = ROLE_OPERATIVE
    return out

async def sync_all_roles(guild):
    roles = {r.name: r for r in guild.roles}
    managed = [roles[n] for n in ROLE_NAMES if n in roles]

    members = {}
    async for m in guild.fetch_members(limit=None):
        if not m.bot:
            members[m.id] = m

    rank_map = compute_rank_map(guild.id, list(members.keys()))

    ok = failed = 0
    for uid, m in members.items():
        target = roles.get(rank_map.get(uid))
        if not target:
            failed += 1
            continue
        try:
            to_remove = [r for r in managed if r in m.roles and r != target]
            if to_remove:
                await m.remove_roles(*to_remove)
            if target not in m.roles:
                await m.add_roles(target)
            ok += 1
        except:
            failed += 1
    return ok, failed

def display_rank(m, computed):
    return f"{MANUAL_PRIME_ROLE} + {computed}" if has_prime(m) else computed

# -------------------------
# SILENT STARTUP AUDIT
# -------------------------
async def silent_startup_audit():
    with db() as c:
        done = c.execute(
            "SELECT value FROM meta WHERE key='startup_audit_done'"
        ).fetchone()
        if done and done["value"] == "1":
            return

        c.execute(
            "INSERT OR REPLACE INTO meta VALUES ('startup_audit_done','1')"
        )
        c.commit()

    for guild in bot.guilds:
        cutoff = datetime.utcnow() - timedelta(days=365)

        with db() as c:
            c.execute("""
                UPDATE users
                SET chat_cooldown=0, last_minute=0, earned_this_minute=0
                WHERE guild_id=?
            """, (guild.id,))
            c.commit()

        for ch in guild.text_channels:
            perms = ch.permissions_for(guild.me)
            if not perms.view_channel or not perms.read_message_history:
                continue
            try:
                async for msg in ch.history(after=cutoff, oldest_first=True, limit=None):
                    if msg.author.bot:
                        continue
                    if len((msg.content or "").strip()) < MIN_MESSAGE_CHARS:
                        continue
                    with db() as c:
                        award_xp(c, guild.id, msg.author.id, CHAT_XP_PER_TICK, ts=int(msg.created_at.timestamp()))
            except:
                pass

        await sync_all_roles(guild)

# -------------------------
# EVENTS
# -------------------------
@bot.event
async def on_ready():
    init_db()
    await bot.tree.sync()
    vc_tick.start()
    decay_loop.start()
    await silent_startup_audit()
    print(f"Logged in as {bot.user}")

@bot.event
async def on_message(msg):
    if msg.author.bot or not msg.guild:
        return
    await bot.process_commands(msg)

    if len((msg.content or "").strip()) < MIN_MESSAGE_CHARS:
        return

    with db() as c:
        u = get_user(c, msg.guild.id, msg.author.id)
        if now() < u["chat_cooldown"]:
            return
        gained = award_xp(c, msg.guild.id, msg.author.id, CHAT_XP_PER_TICK)
        c.execute(
            "UPDATE users SET chat_cooldown=? WHERE guild_id=? AND user_id=?",
            (now() + CHAT_COOLDOWN_SECONDS, msg.guild.id, msg.author.id)
        )
        c.commit()

    if gained:
        await sync_all_roles(msg.guild)

# -------------------------
# TASKS
# -------------------------
@tasks.loop(seconds=60)
async def vc_tick():
    for guild in bot.guilds:
        any_gain = False
        for vc in guild.voice_channels:
            humans = [m for m in vc.members if not m.bot]
            if len(humans) < 2:
                continue
            for m in humans:
                if m.voice and (m.voice.deaf or m.voice.self_deaf):
                    continue
                with db() as c:
                    if award_xp(c, guild.id, m.id, VC_XP_PER_MIN):
                        any_gain = True
        if any_gain:
            await sync_all_roles(guild)

@tasks.loop(hours=24)
async def decay_loop():
    cutoff = now() - DECAY_GRACE_HOURS * 3600
    for guild in bot.guilds:
        changed = False
        with db() as c:
            rows = c.execute(
                "SELECT user_id, xp, last_active FROM users WHERE guild_id=?",
                (guild.id,)
            ).fetchall()

            for r in rows:
                xp = r["xp"]
                if xp <= 0 or r["last_active"] >= cutoff:
                    continue

                loss = max(int(xp * DECAY_PERCENT_PER_DAY), DECAY_MIN_XP_PER_DAY)
                new_xp = clamp_xp(xp - loss)

                if xp >= DECAY_FLOOR_XP:
                    new_xp = max(DECAY_FLOOR_XP, new_xp)

                if new_xp != xp:
                    c.execute(
                        "UPDATE users SET xp=? WHERE guild_id=? AND user_id=?",
                        (new_xp, guild.id, r["user_id"])
                    )
                    changed = True

            c.commit()

        if changed:
            await sync_all_roles(guild)

# -------------------------
# COMMANDS
# -------------------------
@bot.tree.command(name="leaderboard")
async def leaderboard(interaction: discord.Interaction):
    if not interaction.guild:
        return await interaction.response.send_message("Guild only.", ephemeral=True)

    guild = interaction.guild
    await interaction.response.defer(ephemeral=True)

    members = {}
    async for m in guild.fetch_members(limit=None):
        if not m.bot:
            members[m.id] = m

    with db() as c:
        for uid in members.keys():
            get_user(c, guild.id, uid)
        rows = c.execute("""
            SELECT user_id, xp FROM users
            WHERE guild_id=?
            ORDER BY xp DESC, user_id ASC
        """, (guild.id,)).fetchall()

    rank_map = compute_rank_map(guild.id, list(members.keys()))

    lines = []
    place = 0
    for r in rows:
        uid = r["user_id"]
        if uid not in members:
            continue
        place += 1
        m = members[uid]
        xp = r["xp"]
        lines.append(
            f"{place:>4}. {m.display_name} — {xp} XP — {display_rank(m, rank_map.get(uid, ROLE_INITIATE))}"
        )

    preview = "\n".join(lines[:30]) if lines else "No users."
    if len(lines) > 30:
        preview += f"\n… and {len(lines)-30} more (see file)"

    file = discord.File(
        fp=io.BytesIO("\n".join(lines).encode()),
        filename="leaderboard.txt"
    )

    await interaction.followup.send("✅ Leaderboard\n" + preview, ephemeral=True)
    await interaction.followup.send(file=file, ephemeral=True)

@bot.tree.command(name="audit")
@app_commands.describe(days="Days back", announce="Post publicly")
async def audit(interaction: discord.Interaction, days: int = 30, announce: bool = False):
    if not interaction.guild:
        return await interaction.response.send_message("Guild only.", ephemeral=True)
    if not is_admin(interaction):
        return await interaction.response.send_message("Admin only.", ephemeral=True)

    guild = interaction.guild
    cutoff = datetime.utcnow() - timedelta(days=days)

    await interaction.response.defer(ephemeral=not announce)

    scanned = awarded = skipped = 0

    for ch in guild.text_channels:
        perms = ch.permissions_for(guild.me)
        if not perms.view_channel or not perms.read_message_history:
            skipped += 1
            continue
        try:
            async for msg in ch.history(after=cutoff, oldest_first=True, limit=None):
                scanned += 1
                if msg.author.bot:
                    continue
                if len((msg.content or "").strip()) < MIN_MESSAGE_CHARS:
                    continue
                with db() as c:
                    awarded += award_xp(
                        c,
                        guild.id,
                        msg.author.id,
                        CHAT_XP_PER_TICK,
                        ts=int(msg.created_at.timestamp())
                    )
        except:
            skipped += 1

    ok, failed = await sync_all_roles(guild)

    text = (
        f"✅ Audit complete\n"
        f"Days: {days}\n"
        f"Scanned: {scanned}\n"
        f"Awarded XP: {awarded}\n"
        f"Role Sync: {ok} ok / {failed} failed\n"
        f"Skipped Channels: {skipped}"
    )

    file = discord.File(fp=io.BytesIO(text.encode()), filename="audit_report.txt")
    await interaction.followup.send(text, file=file, ephemeral=not announce)

# -------------------------
# RUN
# -------------------------
token = os.getenv("DISCORD_TOKEN")
if not token:
    raise RuntimeError("DISCORD_TOKEN missing")
bot.run(token)
