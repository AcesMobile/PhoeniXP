import os, time, sqlite3, io, asyncio
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

VC_CHECK_SECONDS = 60           # tick VC every minute
VC_MINUTES_PER_XP = 5           # 1 XP per 5 minutes in VC

PER_MINUTE_XP_CAP = 2           # cap per minute bucket (chat + vc combined)

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

MANUAL_PRIME_ROLE = "Phoenix Prime"  # primes are “admins”

DB_PATH = "xp.db"
ROLE_SYNC_DEBOUNCE_SECONDS = 20

STARTUP_AUDIT_DAYS = 365
AUDIT_SLEEP_EVERY_MSGS = 250
AUDIT_SLEEP_SECONDS = 1

# -------------------------
# DISCORD
# -------------------------
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.voice_states = True
bot = commands.Bot(command_prefix="!", intents=intents)

# -------------------------
# DB HELPERS
# -------------------------
def now() -> int: return int(time.time())
def minute_bucket(ts: int) -> int: return ts // 60
def clamp_xp(x: int) -> int: return max(0, min(MAX_XP, int(x)))

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
            vc_minutes INTEGER DEFAULT 0,
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

def get_user(c, gid: int, uid: int):
    r = c.execute("SELECT * FROM users WHERE guild_id=? AND user_id=?", (gid, uid)).fetchone()
    if r:
        return r
    c.execute("INSERT INTO users (guild_id, user_id) VALUES (?,?)", (gid, uid))
    c.commit()
    return c.execute("SELECT * FROM users WHERE guild_id=? AND user_id=?", (gid, uid)).fetchone()

def meta_get(c, key: str, default=None):
    r = c.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return r["value"] if r else default

def meta_set(c, key: str, value):
    c.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (key, str(value)))

def has_prime(m: discord.Member) -> bool:
    return any(r.name == MANUAL_PRIME_ROLE for r in m.roles)

def is_admin(i: discord.Interaction) -> bool:
    return isinstance(i.user, discord.Member) and has_prime(i.user)

async def fetch_members(guild: discord.Guild) -> dict[int, discord.Member]:
    out = {}
    async for m in guild.fetch_members(limit=None):
        if not m.bot:
            out[m.id] = m
    return out

def reset_audit_state(c, gid: int):
    # wipe audit throttles so historical scan isn't blocked by old state
    c.execute("""
        UPDATE users
        SET chat_cooldown=0, last_minute=0, earned_this_minute=0
        WHERE guild_id=?
    """, (gid,))

# -------------------------
# XP CORE
# -------------------------
def award_xp(c, gid: int, uid: int, amount: int, ts: int) -> int:
    """Awards XP with per-minute bucket cap. Caller commits."""
    u = get_user(c, gid, uid)
    bucket = minute_bucket(ts)

    earned = int(u["earned_this_minute"])
    if bucket != int(u["last_minute"]):
        earned = 0

    award = max(0, min(int(amount), PER_MINUTE_XP_CAP - earned))
    if not award:
        return 0

    c.execute("""
        UPDATE users
        SET xp=?, last_active=?, last_minute=?, earned_this_minute=?
        WHERE guild_id=? AND user_id=?
    """, (clamp_xp(int(u["xp"]) + award), ts, bucket, earned + award, gid, uid))
    return award

# -------------------------
# RANKING (TOP-X)
# -------------------------
def compute_rank_map(gid: int, member_ids: list[int]) -> dict[int, str]:
    with db() as c:
        for uid in member_ids:
            get_user(c, gid, uid)
        rows = c.execute("SELECT user_id, xp FROM users WHERE guild_id=?", (gid,)).fetchall()

    xp = {int(r["user_id"]): int(r["xp"]) for r in rows}
    eligible = [(uid, xp.get(uid, 0)) for uid in member_ids if xp.get(uid, 0) >= INITIATE_EXIT_XP]
    eligible.sort(key=lambda x: (-x[1], x[0]))

    topA = {uid for uid, _ in eligible[:TOP_ASCENDANT]}
    nextE = {uid for uid, _ in eligible[TOP_ASCENDANT:TOP_ASCENDANT + NEXT_EMBER]}

    out = {}
    for uid in member_ids:
        x = xp.get(uid, 0)
        if x < INITIATE_EXIT_XP:
            out[uid] = ROLE_INITIATE
        elif uid in topA:
            out[uid] = ROLE_ASCENDANT
        elif uid in nextE:
            out[uid] = ROLE_EMBER
        else:
            out[uid] = ROLE_OPERATIVE
    return out

def display_rank(m: discord.Member, computed: str) -> str:
    return f"{MANUAL_PRIME_ROLE} + {computed}" if has_prime(m) else computed

# -------------------------
# ROLE SYNC (DEBOUNCED)
# -------------------------
_role_sync_tasks: dict[int, asyncio.Task] = {}

async def request_role_sync(guild: discord.Guild):
    if guild.id in _role_sync_tasks:
        return

    async def runner():
        await asyncio.sleep(ROLE_SYNC_DEBOUNCE_SECONDS)
        try:
            await sync_all_roles(guild)
        finally:
            _role_sync_tasks.pop(guild.id, None)

    _role_sync_tasks[guild.id] = asyncio.create_task(runner())

async def sync_all_roles(guild: discord.Guild):
    roles = {r.name: r for r in guild.roles}
    managed = [roles[n] for n in ROLE_NAMES if n in roles]

    members = await fetch_members(guild)
    ids = list(members.keys())
    rank_map = compute_rank_map(guild.id, ids)

    ok = failed = 0
    for uid, m in members.items():
        target_role = roles.get(rank_map.get(uid, ROLE_INITIATE))
        if not target_role:
            failed += 1
            continue
        try:
            to_remove = [r for r in managed if r in m.roles and r != target_role]
            if to_remove:
                await m.remove_roles(*to_remove, reason="Rank sync")
            if target_role not in m.roles:
                await m.add_roles(target_role, reason="Rank sync")
            ok += 1
        except Exception:
            failed += 1
    return ok, failed

# -------------------------
# STARTUP AUDIT (SLOW + THROTTLED)
# -------------------------
async def silent_startup_audit():
    for guild in bot.guilds:
        key = f"startup_audit_done:{guild.id}"

        with db() as c:
            if meta_get(c, key) == "1":
                continue
            meta_set(c, key, "1")
            reset_audit_state(c, guild.id)
            c.commit()

        cutoff = datetime.utcnow() - timedelta(days=STARTUP_AUDIT_DAYS)
        throttle = 0

        with db() as c:
            for ch in guild.text_channels:
                me = guild.me
                if not me:
                    continue
                perms = ch.permissions_for(me)
                if not perms.view_channel or not perms.read_message_history:
                    continue

                try:
                    async for msg in ch.history(after=cutoff, oldest_first=True, limit=None):
                        if msg.author.bot:
                            continue
                        if len((msg.content or "").strip()) < MIN_MESSAGE_CHARS:
                            continue

                        ts = int(msg.created_at.timestamp())
                        u = get_user(c, guild.id, msg.author.id)
                        if ts < int(u["chat_cooldown"]):
                            continue

                        gained = award_xp(c, guild.id, msg.author.id, CHAT_XP_PER_TICK, ts)
                        if gained:
                            c.execute("UPDATE users SET chat_cooldown=? WHERE guild_id=? AND user_id=?",
                                      (ts + CHAT_COOLDOWN_SECONDS, guild.id, msg.author.id))

                        throttle += 1
                        if throttle % AUDIT_SLEEP_EVERY_MSGS == 0:
                            c.commit()
                            await asyncio.sleep(AUDIT_SLEEP_SECONDS)
                except Exception:
                    pass

            c.commit()

        await sync_all_roles(guild)

# -------------------------
# EVENTS
# -------------------------
@bot.event
async def on_ready():
    init_db()
    await bot.tree.sync()
    decay_loop.start()
    vc_xp_loop.start()
    await silent_startup_audit()
    print("Ready:", bot.user)

@bot.event
async def on_message(msg: discord.Message):
    if msg.author.bot or not msg.guild:
        return
    await bot.process_commands(msg)

    if len((msg.content or "").strip()) < MIN_MESSAGE_CHARS:
        return

    ts = now()
    with db() as c:
        u = get_user(c, msg.guild.id, msg.author.id)
        if ts < int(u["chat_cooldown"]):
            return

        gained = award_xp(c, msg.guild.id, msg.author.id, CHAT_XP_PER_TICK, ts)
        c.execute("UPDATE users SET chat_cooldown=? WHERE guild_id=? AND user_id=?",
                  (ts + CHAT_COOLDOWN_SECONDS, msg.guild.id, msg.author.id))
        c.commit()

    if gained:
        await request_role_sync(msg.guild)

# -------------------------
# VC XP (1 XP per 5 minutes)
# -------------------------
@tasks.loop(seconds=VC_CHECK_SECONDS)
async def vc_xp_loop():
    ts = now()
    for guild in bot.guilds:
        any_gain = False

        for vc in guild.voice_channels:
            humans = [m for m in vc.members if not m.bot]
            if len(humans) < 2:
                continue

            with db() as c:
                for m in humans:
                    if m.voice and (m.voice.deaf or m.voice.self_deaf):
                        continue

                    u = get_user(c, guild.id, m.id)
                    minutes = int(u["vc_minutes"]) + 1

                    if minutes >= VC_MINUTES_PER_XP:
                        if award_xp(c, guild.id, m.id, 1, ts):
                            any_gain = True
                        minutes = 0

                    c.execute("UPDATE users SET vc_minutes=? WHERE guild_id=? AND user_id=?",
                              (minutes, guild.id, m.id))
                c.commit()

        if any_gain:
            await request_role_sync(guild)

# -------------------------
# DECAY
# -------------------------
@tasks.loop(hours=24)
async def decay_loop():
    cutoff = now() - DECAY_GRACE_HOURS * 3600
    for guild in bot.guilds:
        changed = False
        with db() as c:
            rows = c.execute("SELECT user_id, xp, last_active FROM users WHERE guild_id=?",
                             (guild.id,)).fetchall()

            for r in rows:
                xp = int(r["xp"])
                if xp <= 0 or int(r["last_active"]) >= cutoff:
                    continue

                loss = max(int(xp * DECAY_PERCENT_PER_DAY), DECAY_MIN_XP_PER_DAY)
                new_xp = clamp_xp(xp - loss)
                if xp >= DECAY_FLOOR_XP:
                    new_xp = max(DECAY_FLOOR_XP, new_xp)

                if new_xp != xp:
                    c.execute("UPDATE users SET xp=? WHERE guild_id=? AND user_id=?",
                              (new_xp, guild.id, int(r["user_id"])))
                    changed = True

            c.commit()

        if changed:
            await request_role_sync(guild)

# -------------------------
# COMMANDS
# -------------------------
@bot.tree.command(name="standing")
async def standing(interaction: discord.Interaction):
    if not interaction.guild:
        return await interaction.response.send_message("Guild only.", ephemeral=True)

    guild = interaction.guild
    me = interaction.user
    await interaction.response.defer(ephemeral=True)

    members = await fetch_members(guild)
    ids = list(members.keys())

    with db() as c:
        for uid in ids:
            get_user(c, guild.id, uid)
        rows = c.execute("""
            SELECT user_id, xp FROM users
            WHERE guild_id=?
            ORDER BY xp DESC, user_id ASC
        """, (guild.id,)).fetchall()

    rank_map = compute_rank_map(guild.id, ids)

    place = total = myxp = 0
    for r in rows:
        uid = int(r["user_id"])
        if uid not in members:
            continue
        total += 1
        if uid == me.id:
            place = total
            myxp = int(r["xp"])

    await interaction.followup.send(
        f"📊 Standing\nPlace: #{place}/{total}\nXP: {myxp}/{MAX_XP}\n"
        f"Tier: {display_rank(me, rank_map.get(me.id, ROLE_INITIATE))}",
        ephemeral=True
    )

@bot.tree.command(name="leaderboard")
@app_commands.describe(announce="Post publicly")
async def leaderboard(interaction: discord.Interaction, announce: bool = False):
    if not interaction.guild:
        return await interaction.response.send_message("Guild only.", ephemeral=True)

    guild = interaction.guild
    await interaction.response.defer(ephemeral=not announce)

    members = await fetch_members(guild)
    ids = list(members.keys())

    with db() as c:
        for uid in ids:
            get_user(c, guild.id, uid)
        rows = c.execute("""
            SELECT user_id, xp FROM users
            WHERE guild_id=?
            ORDER BY xp DESC, user_id ASC
        """, (guild.id,)).fetchall()

    rank_map = compute_rank_map(guild.id, ids)

    lines, place = [], 0
    for r in rows:
        uid = int(r["user_id"])
        if uid not in members:
            continue
        place += 1
        m = members[uid]
        xp = int(r["xp"])
        lines.append(f"{place:>4}. {m.display_name} — {xp} XP — {display_rank(m, rank_map.get(uid, ROLE_INITIATE))}")

    preview = "\n".join(lines[:30]) if lines else "No users."
    file = discord.File(fp=io.BytesIO("\n".join(lines).encode()), filename="leaderboard.txt")

    await interaction.followup.send("✅ Leaderboard\n" + preview, ephemeral=not announce)
    await interaction.followup.send(file=file, ephemeral=not announce)

@bot.tree.command(name="audit")
@app_commands.describe(days="Days back", announce="Post publicly")
async def audit(interaction: discord.Interaction, days: int = 30, announce: bool = False):
    if not interaction.guild:
        return await interaction.response.send_message("Guild only.", ephemeral=True)
    if not is_admin(interaction):
        return await interaction.response.send_message("Phoenix Prime only.", ephemeral=True)

    guild = interaction.guild
    cutoff = datetime.utcnow() - timedelta(days=days)
    await interaction.response.defer(ephemeral=not announce)

    scanned = awarded = skipped = throttle = 0
    with db() as c:
        reset_audit_state(c, guild.id)
        c.commit()

        for ch in guild.text_channels:
            me = guild.me
            if not me:
                continue
            perms = ch.permissions_for(me)
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

                    ts = int(msg.created_at.timestamp())
                    u = get_user(c, guild.id, msg.author.id)
                    if ts < int(u["chat_cooldown"]):
                        continue

                    gained = award_xp(c, guild.id, msg.author.id, CHAT_XP_PER_TICK, ts)
                    if gained:
                        awarded += gained
                        c.execute("UPDATE users SET chat_cooldown=? WHERE guild_id=? AND user_id=?",
                                  (ts + CHAT_COOLDOWN_SECONDS, guild.id, msg.author.id))

                    throttle += 1
                    if throttle % AUDIT_SLEEP_EVERY_MSGS == 0:
                        c.commit()
                        await asyncio.sleep(AUDIT_SLEEP_SECONDS)

            except Exception:
                skipped += 1

        c.commit()

    ok, failed = await sync_all_roles(guild)
    await interaction.followup.send(
        f"Audit complete\nDays: {days}\nScanned: {scanned}\nAwarded XP: {awarded}\n"
        f"Role Sync: {ok}/{failed}\nSkipped Channels: {skipped}",
        ephemeral=not announce
    )

@bot.tree.command(name="resetranks")
@app_commands.describe(member="Optional single member")
async def resetranks(interaction: discord.Interaction, member: discord.Member | None = None):
    if not interaction.guild:
        return await interaction.response.send_message("Guild only.", ephemeral=True)
    if not is_admin(interaction):
        return await interaction.response.send_message("Phoenix Prime only.", ephemeral=True)

    guild = interaction.guild
    await interaction.response.defer(ephemeral=True)

    members = await fetch_members(guild)
    targets = [member.id] if member else list(members.keys())

    changed = 0
    with db() as c:
        for uid in targets:
            u = get_user(c, guild.id, uid)
            old = int(u["xp"])
            new = old if old < INITIATE_EXIT_XP else INITIATE_EXIT_XP
            if new != old:
                changed += 1
            c.execute("""
                UPDATE users
                SET xp=?, last_active=0, chat_cooldown=0, last_minute=0,
                    earned_this_minute=0, vc_minutes=0
                WHERE guild_id=? AND user_id=?
            """, (new, guild.id, uid))
        c.commit()

    ok, failed = await sync_all_roles(guild)
    await interaction.followup.send(f"Reset complete\nChanged XP: {changed}\nRole Sync: {ok}/{failed}", ephemeral=True)

@bot.tree.command(name="setxp")
@app_commands.describe(member="User", xp="New XP", announce="Public?")
async def setxp(interaction: discord.Interaction, member: discord.Member, xp: int, announce: bool = False):
    if not interaction.guild:
        return await interaction.response.send_message("Guild only.", ephemeral=True)
    if not is_admin(interaction):
        return await interaction.response.send_message("Phoenix Prime only.", ephemeral=True)

    xp = clamp_xp(xp)
    await interaction.response.defer(ephemeral=not announce)

    with db() as c:
        get_user(c, interaction.guild.id, member.id)
        c.execute("""
            UPDATE users
            SET xp=?, last_active=?, chat_cooldown=0, last_minute=0,
                earned_this_minute=0, vc_minutes=0
            WHERE guild_id=? AND user_id=?
        """, (xp, now(), interaction.guild.id, member.id))
        c.commit()

    ok, failed = await sync_all_roles(interaction.guild)
    await interaction.followup.send(f"Set {member.display_name} → {xp} XP\nSync {ok}/{failed}", ephemeral=not announce)

# -------------------------
# RUN
# -------------------------
token = os.getenv("DISCORD_TOKEN")
if not token:
    raise RuntimeError("DISCORD_TOKEN missing")
bot.run(token)
