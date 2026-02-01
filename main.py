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
DECAY_FLOOR_XP = 3  # once you hit 3, decay never drops you below 3

INITIATE_EXIT_XP = 3
TOP_ASCENDANT = 5
NEXT_EMBER = 10

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
# DB HELPERS
# -------------------------
def now() -> int:
    return int(time.time())

def minute_bucket(ts: int) -> int:
    return ts // 60

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
        c.commit()

def get_user(c, guild_id: int, user_id: int):
    row = c.execute("SELECT * FROM users WHERE guild_id=? AND user_id=?",
                    (guild_id, user_id)).fetchone()
    if not row:
        c.execute("INSERT INTO users (guild_id, user_id) VALUES (?,?)", (guild_id, user_id))
        c.commit()
        return get_user(c, guild_id, user_id)
    return row

def clamp_xp(xp: int) -> int:
    return max(0, min(MAX_XP, int(xp)))

def is_admin(i: discord.Interaction) -> bool:
    return bool(i.user and i.user.guild_permissions and i.user.guild_permissions.administrator)

def has_prime(m: discord.Member) -> bool:
    return any(r.name == MANUAL_PRIME_ROLE for r in m.roles)

def award_xp(c, guild_id: int, user_id: int, amount: int, ts: int | None = None) -> int:
    u = get_user(c, guild_id, user_id)
    ts = ts if ts is not None else now()
    bucket = minute_bucket(ts)

    earned = int(u["earned_this_minute"])
    if bucket != int(u["last_minute"]):
        earned = 0

    remaining = PER_MINUTE_XP_CAP - earned
    award = max(0, min(amount, remaining))
    if award == 0:
        return 0

    new_xp = clamp_xp(int(u["xp"]) + award)
    c.execute("""
        UPDATE users
        SET xp=?, last_active=?, last_minute=?, earned_this_minute=?
        WHERE guild_id=? AND user_id=?
    """, (new_xp, ts, bucket, earned + award, guild_id, user_id))
    c.commit()
    return award

# -------------------------
# RANKING (TOP-X)
# -------------------------
def compute_rank_map(guild_id: int, member_ids: list[int]) -> dict[int, str]:
    with db() as c:
        for uid in member_ids:
            get_user(c, guild_id, uid)
        rows = c.execute("SELECT user_id, xp FROM users WHERE guild_id=?", (guild_id,)).fetchall()

    xp = {int(r["user_id"]): int(r["xp"]) for r in rows}
    eligible = [(uid, xp.get(uid, 0)) for uid in member_ids if xp.get(uid, 0) >= INITIATE_EXIT_XP]
    eligible.sort(key=lambda x: (-x[1], x[0]))

    topA = set(uid for uid, _ in eligible[:TOP_ASCENDANT])
    nextE = set(uid for uid, _ in eligible[TOP_ASCENDANT:TOP_ASCENDANT + NEXT_EMBER])

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

async def sync_all_roles(guild: discord.Guild) -> tuple[int, int]:
    roles = {r.name: r for r in guild.roles}
    managed = [roles[n] for n in ROLE_NAMES if n in roles]

    members: dict[int, discord.Member] = {}
    async for m in guild.fetch_members(limit=None):
        if not m.bot:
            members[m.id] = m

    rank_map = compute_rank_map(guild.id, list(members.keys()))
    updated = failed = 0

    for uid, m in members.items():
        target_name = rank_map.get(uid, ROLE_INITIATE)
        target_role = roles.get(target_name)
        if not target_role:
            failed += 1
            continue

        to_remove = [r for r in managed if r in m.roles and r.name != target_name]
        try:
            if to_remove:
                await m.remove_roles(*to_remove, reason="Rank sync")
            if target_role not in m.roles:
                await m.add_roles(target_role, reason="Rank sync")
            updated += 1
        except Exception:
            failed += 1

    return updated, failed

def display_rank(m: discord.Member, computed: str) -> str:
    # show Prime in output, but never auto-manage it
    return f"{MANUAL_PRIME_ROLE} + {computed}" if has_prime(m) else computed

# -------------------------
# RESET LOGIC
# -------------------------
def compute_reset_xp(old_xp: int) -> int:
    """
    Reset rule:
    - if old < 3, keep it (0,1,2 stay 0,1,2)
    - else set to 3 (Operative floor)
    """
    return int(old_xp) if int(old_xp) < INITIATE_EXIT_XP else INITIATE_EXIT_XP

async def reset_ranks_for_members(guild: discord.Guild, member_ids: list[int]) -> tuple[int, int]:
    """
    Applies reset-xp rule to selected members, then re-syncs roles for everyone (top-X depends on whole ladder).
    Returns (changed_xp_count, role_updated_ok_minus_failed) is not tracked here; resync returns (ok, failed).
    """
    changed = 0
    with db() as c:
        for uid in member_ids:
            u = get_user(c, guild.id, uid)
            old_xp = int(u["xp"])
            new_xp = compute_reset_xp(old_xp)
            if new_xp != old_xp:
                c.execute("UPDATE users SET xp=?, last_active=?, chat_cooldown=?, last_minute=?, earned_this_minute=? WHERE guild_id=? AND user_id=?",
                          (new_xp, 0, 0, 0, 0, guild.id, uid))
                changed += 1
            else:
                # still wipe cooldown/minute state so they aren't "stuck"
                c.execute("UPDATE users SET last_active=?, chat_cooldown=?, last_minute=?, earned_this_minute=? WHERE guild_id=? AND user_id=?",
                          (0, 0, 0, 0, guild.id, uid))
        c.commit()

    ok, failed = await sync_all_roles(guild)
    return changed, (ok, failed)

async def fetch_nonbot_members(guild: discord.Guild) -> dict[int, discord.Member]:
    members: dict[int, discord.Member] = {}
    async for m in guild.fetch_members(limit=None):
        if not m.bot:
            members[m.id] = m
    return members

# -------------------------
# EVENTS
# -------------------------
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

    with db() as c:
        u = get_user(c, msg.guild.id, msg.author.id)
        if now() < int(u["chat_cooldown"]):
            return

        gained = award_xp(c, msg.guild.id, msg.author.id, CHAT_XP_PER_TICK)
        c.execute("UPDATE users SET chat_cooldown=? WHERE guild_id=? AND user_id=?",
                  (now() + CHAT_COOLDOWN_SECONDS, msg.guild.id, msg.author.id))
        c.commit()

    if gained:
        await sync_all_roles(msg.guild)

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
        any_change = False
        with db() as c:
            rows = c.execute("SELECT user_id, xp, last_active FROM users WHERE guild_id=?",
                             (guild.id,)).fetchall()
            for r in rows:
                uid = int(r["user_id"])
                xp = int(r["xp"])
                last_active = int(r["last_active"])
                if xp <= 0 or last_active >= cutoff:
                    continue

                loss = max(int(xp * DECAY_PERCENT_PER_DAY), DECAY_MIN_XP_PER_DAY)
                new_xp = clamp_xp(xp - loss)

                # decay floor applies only once they have reached 3+
                if xp >= DECAY_FLOOR_XP:
                    new_xp = max(DECAY_FLOOR_XP, new_xp)

                if new_xp != xp:
                    c.execute("UPDATE users SET xp=? WHERE guild_id=? AND user_id=?",
                              (new_xp, guild.id, uid))
                    any_change = True
            c.commit()
        if any_change:
            await sync_all_roles(guild)

# -------------------------
# SLASH COMMANDS
# -------------------------
@bot.tree.command(name="leaderboard")
async def leaderboard(interaction: discord.Interaction):
    if not interaction.guild:
        return await interaction.response.send_message("Guild only.", ephemeral=True)

    guild = interaction.guild
    await interaction.response.defer(ephemeral=True)

    members = await fetch_nonbot_members(guild)

    with db() as c:
        for uid in members.keys():
            get_user(c, guild.id, uid)
        rows = c.execute("""
            SELECT user_id, xp FROM users
            WHERE guild_id=?
            ORDER BY xp DESC, user_id ASC
        """, (guild.id,)).fetchall()

    rank_map = compute_rank_map(guild.id, list(members.keys()))

    lines, place = [], 0
    for r in rows:
        uid = int(r["user_id"])
        if uid not in members:
            continue
        place += 1
        m = members[uid]
        xp = int(r["xp"])
        lines.append(
            f"{place:>4}. {m.display_name} — {xp} XP — {display_rank(m, rank_map.get(uid, ROLE_INITIATE))}"
        )

    # send text in chat (ephemeral) + txt file (also ephemeral)
    text_preview = "\n".join(lines[:30]) if lines else "No users."
    if len(lines) > 30:
        text_preview += f"\n… and **{len(lines)-30}** more (see file)"

    file = discord.File(
        fp=io.BytesIO(("\n".join(lines) if lines else "No users.").encode("utf-8")),
        filename="leaderboard.txt"
    )

    await interaction.followup.send(
        "✅ **Leaderboard**\n" + text_preview,
        ephemeral=True
    )
    await interaction.followup.send(
        "📎 Full leaderboard file:",
        file=file,
        ephemeral=True
    )

@bot.tree.command(name="audit")
@app_commands.describe(days="How many days back to scan (default 30)")
async def audit(interaction: discord.Interaction, days: int = 30):
    if not interaction.guild:
        return await interaction.response.send_message("Guild only.", ephemeral=True)
    if not is_admin(interaction):
        return await interaction.response.send_message("Admin only.", ephemeral=True)

    guild = interaction.guild
    cutoff_dt = datetime.utcnow() - timedelta(days=days)
    await interaction.response.defer(ephemeral=True)

    members = await fetch_nonbot_members(guild)

    with db() as c:
        for uid in members.keys():
            get_user(c, guild.id, uid)
        # reset audit state so historical cooldown doesn't block
        c.execute("""
            UPDATE users
            SET chat_cooldown=0, last_minute=0, earned_this_minute=0
            WHERE guild_id=?
        """, (guild.id,))
        c.commit()

    scanned = 0
    awarded_total = 0
    skipped_channels = 0
    skipped_msgs_short = 0
    skipped_msgs_bots = 0
    per_user: dict[int, int] = {}

    for ch in guild.text_channels:
        me = guild.me
        if not me:
            continue
        perms = ch.permissions_for(me)
        if not (perms.view_channel and perms.read_message_history):
            skipped_channels += 1
            continue

        try:
            async for msg in ch.history(after=cutoff_dt, oldest_first=True, limit=None):
                scanned += 1

                if msg.author.bot:
                    skipped_msgs_bots += 1
                    continue

                content = (msg.content or "").strip()
                if len(content) < MIN_MESSAGE_CHARS:
                    skipped_msgs_short += 1
                    continue

                ts = int(msg.created_at.timestamp())
                with db() as c:
                    u = get_user(c, guild.id, msg.author.id)
                    if ts < int(u["chat_cooldown"]):
                        continue

                    gained = award_xp(c, guild.id, msg.author.id, CHAT_XP_PER_TICK, ts=ts)
                    if gained:
                        per_user[msg.author.id] = per_user.get(msg.author.id, 0) + gained
                    c.execute("UPDATE users SET chat_cooldown=? WHERE guild_id=? AND user_id=?",
                              (ts + CHAT_COOLDOWN_SECONDS, guild.id, msg.author.id))
                    c.commit()

                awarded_total += gained
        except Exception:
            skipped_channels += 1

    ok, failed = await sync_all_roles(guild)

    # top gains
    top = sorted(per_user.items(), key=lambda x: (-x[1], x[0]))[:10]
    top_lines = []
    for i, (uid, gained) in enumerate(top, start=1):
        name = members[uid].display_name if uid in members else str(uid)
        top_lines.append(f"{i}. {name} +{gained} XP")

    # file report
    report = [
        f"Audit window: last {days} day(s)",
        f"Cutoff (UTC): {cutoff_dt.isoformat()}",
        "",
        f"Scanned messages: {scanned}",
        f"XP awarded total: {awarded_total}",
        f"Channels skipped (perms/errors): {skipped_channels}",
        f"Msgs skipped (bots): {skipped_msgs_bots}",
        f"Msgs skipped (too short): {skipped_msgs_short}",
        "",
        f"Role sync: ok={ok} failed={failed}",
        "",
        "Top gains:",
        *(top_lines if top_lines else ["None"]),
    ]
    report_file = discord.File(fp=io.BytesIO("\n".join(report).encode("utf-8")), filename="audit_report.txt")

    await interaction.followup.send(
        "✅ **Audit complete**\n"
        f"- Scanned: **{scanned}** msgs\n"
        f"- Awarded: **{awarded_total}** XP\n"
        f"- Skipped channels: **{skipped_channels}**\n"
        f"- Skipped msgs: **{skipped_msgs_bots}** bots, **{skipped_msgs_short}** too short\n"
        f"- Role sync: **{ok}** ok / **{failed}** failed\n\n"
        "**Top gains**\n" + ("\n".join(top_lines) if top_lines else "None"),
        ephemeral=True
    )
    await interaction.followup.send("📎 Full audit report:", file=report_file, ephemeral=True)

@bot.tree.command(name="resetranks")
@app_commands.describe(member="Optional: reset just one member (default: everyone)")
async def resetranks(interaction: discord.Interaction, member: discord.Member | None = None):
    if not interaction.guild:
        return await interaction.response.send_message("Guild only.", ephemeral=True)
    if not is_admin(interaction):
        return await interaction.response.send_message("Admin only.", ephemeral=True)

    guild = interaction.guild
    await interaction.response.defer(ephemeral=True)

    members = await fetch_nonbot_members(guild)

    if member is not None:
        if member.bot:
            return await interaction.followup.send("Can't reset a bot.", ephemeral=True)
        target_ids = [member.id]
        scope = f"one member: **{member.display_name}**"
    else:
        target_ids = list(members.keys())
        scope = f"**everyone** ({len(target_ids)} users)"

    changed, (ok, failed) = await reset_ranks_for_members(guild, target_ids)

    await interaction.followup.send(
        "✅ **Reset ranks complete**\n"
        f"- Scope: {scope}\n"
        f"- XP rule: `<3 stays`, `>=3 -> 3`\n"
        f"- Users with XP changed: **{changed}**\n"
        f"- Role sync: **{ok}** ok / **{failed}** failed\n"
        f"- `{MANUAL_PRIME_ROLE}` preserved (bot never touches it)",
        ephemeral=True
    )

# -------------------------
# RUN
# -------------------------
token = os.getenv("DISCORD_TOKEN")
if not token:
    raise RuntimeError("DISCORD_TOKEN missing")
bot.run(token)
