import os, time, sqlite3, io, asyncio, re, traceback, json, secrets
from datetime import datetime, timedelta, timezone

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

VC_CHECK_SECONDS = 60
VC_MINUTES_PER_XP = 5  # 1 XP per 5 minutes

PER_MINUTE_XP_CAP = 2  # chat + vc combined

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

# ✅ primes are admins (any of these roles grant admin powers)
MANUAL_PRIME_ROLES = {"Phoenix Prime", "MIDNIGHT DEV TEAM"}

# ✅ Persist DB on Railway volume (mount volume to /app/data)
DB_PATH = os.getenv("XP_DB_PATH", "/app/data/xp.db")

ROLE_SYNC_DEBOUNCE_SECONDS = 20

# /audit throttling
AUDIT_SLEEP_EVERY_MSGS = 250
AUDIT_SLEEP_SECONDS = 1

ANNOUNCE_CHANNEL_NAME = "📢announcements"

# Notify picture upload window
NOTIFY_IMAGE_WAIT_SECONDS = 60
NOTIFY_MAX_IMAGE_BYTES = 8 * 1024 * 1024  # 8MB safety cap
NOTIFY_MAX_IMAGES = 10  # Discord single-message attachment limit

# Throttles
ROLE_SYNC_SLEEP_EVERY_EDITS = 8
ROLE_SYNC_SLEEP_SECONDS = 1.0

# Poll config
POLL_MAX_OPTIONS = 10                 # fits button UI nicely
POLL_MIN_MINUTES = 1                  # minimum duration
POLL_MAX_DAYS = 14                    # safety cap (in days)
POLL_CLOSE_CHECK_SECONDS = 20         # close sweep interval


# -------------------------
# /notify auto-bold list
# -------------------------
AUTO_BOLD_PHRASES = [
    "Sol System","Super Earth","Mars","Barnard Sector","Fornskogur II","Veil","Marre IV","Midasburg",
    "Darrowsport","Hydrofall Prime","Cancri Sector","Prosperity Falls","Cerberus IIIc","Effluvia",
    "Seyshel Beach","Fort Sanctuary","Gothmar Sector","Okul VI","Solghast","Diluvia","Cantolus Sector",
    "Kelvinor","Martyr’s Bay","Freedom Peak","Viridia Prime","Obari","Idun Sector","Wraith","Atrama",
    "Myradesh","Maw","Kelvin Sector","Zegema Paradise","Fort Justice","New Kiruna","Igla","Emeria",
    "Altus Sector","Pathfinder V","Klen Dahth II","Widow’s Harbor","New Haven","Pilen V","Celeste Sector",
    "Sulfura","Nublaria I","Krakatwo","Ivis","Slif","Moradesh","Korpus Sector","Crucible","Volterra",
    "Caramoor","Alta V","Inari","Gallux Sector","Kharst","Bashyr","Rasp","Acubens Prime","Adhara",
    "Afoyay Bay","Morgon Sector","Myrium","Eukoria","Regnus","Mog","Rictus Sector","Valmox","Iro",
    "Grafmere","Kerth Secundus","Parsh","Oasis","Genesis Prime","Saleria Sector","Calypso","Outpost 32",
    "Reaf","Irulta","Meridian Sector","Emorath","Ilduna Prime","Baldrick Prime","Liberty Ridge",
    "Sagan Sector","Oslo Station","Gunvald","Borea","Marspira Sector","Curia","Barabos","Fenmire","Tarsh",
    "Mastia","Talus Sector","Shallus","Shelt","Gaellivare","Imber","Iptus Sector","Providence","Primordia",
    "Krakabos","Iridica","Valgaard","Ratch","Orion Sector","Terrek","Azterra","Fort Union","Cirrus","Heeth",
    "Angel’s Venture","Veld","Ursa Sector","Skaash","Acrab XI","Acrux IX","Gemma","Ferris Sector","Hadar",
    "Haldus","Zea Rugosia","Herthon Secundus","Hanzo Sector","Heze Bay","Alairt III","Alamak VII",
    "New Stockholm","Ain-5","Akira Sector","Alaraph","Alathfar XI","Andar","Asperoth Prime","Keid",
    "Guang Sector","Elysian Meadows","Alderidge Cove","Bellatrix","Botein","Khandark","Tarragon Sector",
    "East Iridium Trading Bay","Brink-2","Osupsam","Canopus","Bunda Secundus","Theseus Sector","The Weir",
    "Kuper","Caph","Castor","Tien Kwan","Lastofe","Nanos Sector","Dolph","Julheim","Bekvam III","Duma Tyr",
    "Hydra Sector","Aesir Pass","Vernen Wells","Menkent","Lacaille Sector","Lesath","Penta","Chort Bay",
    "Choohe","Tanis Sector","Claorell","Vog–Sojoth","Clasa","Yed Prior","Zefia","Demiurg","Arturion Sector",
    "Mortax Prime","Kirrik","Wilford Station","Arkturus","Pioneer II","Electra Bay","Deneb Secundus",
    "Falstaff Sector","Bore Rock","Esker","Socorro III","Erson Sands","Umlaut Sector","Erata Prime",
    "Fenrir III","Meridia","Turing","Borgus Sector","Ursica XI","Achird III","Achernar Secundus","Darius II",
    "Alstrad Sector","Kneth Port","Klaka 5","Kraz","Andromeda Sector","Charbal-VII","Charon Prime","Martale",
    "Marfark","Matar Bay","Mirin Sector","Hellmire","Nivel 43","Zagon Prime","Oshaune","Draco Sector",
    "Crimsica","Estanu","Fori Prime","Jin Xi Sector","Acamar IV","Pandion-XXIV","Gacrux","Phact Bay",
    "Gar Haren","Gatria","Sten Sector","Trandor","Peacock","Partion","Overgoe Prime","Azur Secundus",
    "L’estrade Sector","Navi VII","Omicron","Nabatea Secundus","Gemstone Bluffs","Epsilon Phoencis VI",
    "Enuliale","Disapora X","Hawking Sector","Mordia 9","Euphoria III","Skitter","Kuma","Gellert Sector",
    "Minchir","Mintoria","Blistica","Zzaniah Prime","Zosma","Valdis Sector","Merga IV","Merak","Cyberstan",
    "Aurora Bay","Mekbuda","Videmitarix Prime","Ymir Sector","Meissa","Wasat","X-45","Vega Bay","Wezen",
    "Trigon Sector","Varylia 5","Choepessa IV","Ustotu","Troost","Vandalon IV","Xzar Sector","Mort",
    "Pöpli IX","Ingmar","Mantes","Draupnir","Severin Sector","Maia","Malevelon Creek","Durgen","Ubanea",
    "Tibit","Quintus Sector","Termadon","Stor Tha Prime","Spherion","Stout","Leng Secundus","Xi Tauri Sector",
    "Skat Bay","Sirius","Siemnot","Shete","Omega Sector","Setia","Senge 23","Seasse","Hydrobius","Karlia",
    "Rigel Sector","Rogue 5","RD-4","Hesoe Prime","Hort","Rirga Bay","Leo Sector","Ras Algethi","Propus",
    "Halies Port","Haka","Farsight Sector","Prasa","Pollux 31","Polaris Prime","Pherkad Secundus","Grand Errant",
]

# Precompile bold patterns once (big CPU win)
_COMPILED_BOLD_PATTERNS: list[re.Pattern] = []
for _phrase in sorted(AUTO_BOLD_PHRASES, key=len, reverse=True):
    if not _phrase:
        continue
    esc = re.escape(_phrase)
    _COMPILED_BOLD_PATTERNS.append(
        re.compile(rf"(?i)(?<![0-9A-Za-z_])({esc})(?![0-9A-Za-z_])")
    )


def auto_bold_phrases(text: str) -> str:
    if not text:
        return text

    for pattern in _COMPILED_BOLD_PATTERNS:
        def repl(m: re.Match) -> str:
            start, end = m.span(1)
            if start >= 2 and end + 2 <= len(text):
                if text[start - 2:start] == "**" and text[end:end + 2] == "**":
                    return m.group(1)
            return f"**{m.group(1)}**"

        text = pattern.sub(repl, text)

    return text


def _is_image_attachment(a: discord.Attachment) -> bool:
    ct = (a.content_type or "").lower()
    name = (a.filename or "").lower()
    if ct.startswith("image/"):
        return True
    return name.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp"))


def _safe_filename(name: str) -> str:
    name = (name or "").strip() or "image.png"
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
    if "." not in name:
        name += ".png"
    return name[:120]


def _dedupe_filename(existing: set[str], desired: str) -> str:
    if desired not in existing:
        existing.add(desired)
        return desired
    base, dot, ext = desired.rpartition(".")
    if not dot:
        base, ext = desired, "png"
    for i in range(2, 999):
        cand = f"{base}_{i}.{ext}"
        if cand not in existing:
            existing.add(cand)
            return cand
    cand = f"{base}_{int(time.time())}.{ext}"
    existing.add(cand)
    return cand


# -------------------------
# DISCORD
# -------------------------
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.voice_states = True
bot = commands.Bot(command_prefix="!", intents=intents)

# -------------------------
# GLOBAL LOCKS
# -------------------------
DB_LOCK = asyncio.Lock()
_role_sync_tasks: dict[int, asyncio.Task] = {}


# -------------------------
# DB HELPERS
# -------------------------
def now() -> int:
    return int(time.time())


def minute_bucket(ts: int) -> int:
    return ts // 60


def clamp_xp(x: int) -> int:
    return max(0, min(MAX_XP, int(x)))


def _ensure_db_dir():
    try:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    except Exception:
        pass


def db():
    _ensure_db_dir()
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL;")
    c.execute("PRAGMA synchronous=NORMAL;")
    c.execute("PRAGMA busy_timeout=30000;")
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
        c.execute("CREATE INDEX IF NOT EXISTS idx_users_guild_xp ON users(guild_id, xp DESC, user_id ASC)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_users_guild_last_active ON users(guild_id, last_active)")

        # ---- Poll tables (anonymous voting, no swaps, results revealed at end) ----
        c.execute("""
        CREATE TABLE IF NOT EXISTS polls (
            poll_id TEXT PRIMARY KEY,
            guild_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            created_by INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            ends_at INTEGER NOT NULL,
            question TEXT NOT NULL,
            options_json TEXT NOT NULL,
            ping_mode TEXT NOT NULL DEFAULT 'none',   -- none/here/everyone/role
            role_id INTEGER DEFAULT NULL,             -- for role ping
            dm_enabled INTEGER NOT NULL DEFAULT 0,     -- 0/1
            closed INTEGER NOT NULL DEFAULT 0          -- 0/1
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS poll_votes (
            poll_id TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            option_index INTEGER NOT NULL,
            voted_at INTEGER NOT NULL,
            PRIMARY KEY (poll_id, user_id)
        )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_polls_guild_ends ON polls(guild_id, ends_at, closed)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_poll_votes_poll ON poll_votes(poll_id)")
        c.commit()


def ensure_users_exist(c: sqlite3.Connection, gid: int, member_ids: list[int]) -> None:
    if not member_ids:
        return
    c.executemany(
        "INSERT INTO users (guild_id, user_id) VALUES (?, ?) "
        "ON CONFLICT(guild_id, user_id) DO NOTHING",
        [(gid, uid) for uid in member_ids],
    )


def get_user(c: sqlite3.Connection, gid: int, uid: int):
    r = c.execute("SELECT * FROM users WHERE guild_id=? AND user_id=?", (gid, uid)).fetchone()
    if r:
        return r
    c.execute(
        "INSERT INTO users (guild_id, user_id) VALUES (?, ?) "
        "ON CONFLICT(guild_id, user_id) DO NOTHING",
        (gid, uid),
    )
    c.commit()
    return c.execute("SELECT * FROM users WHERE guild_id=? AND user_id=?", (gid, uid)).fetchone()


def meta_get(c: sqlite3.Connection, key: str, default=None):
    r = c.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return r["value"] if r else default


def meta_set(c: sqlite3.Connection, key: str, value):
    c.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (key, str(value)))


def reset_audit_state(c: sqlite3.Connection, gid: int):
    c.execute("""
        UPDATE users
        SET chat_cooldown=0, last_minute=0, earned_this_minute=0
        WHERE guild_id=?
    """, (gid,))


def has_prime(m: discord.Member) -> bool:
    role_names = {r.name for r in m.roles}
    return any(name in role_names for name in MANUAL_PRIME_ROLES)


def is_admin(i: discord.Interaction) -> bool:
    return isinstance(i.user, discord.Member) and has_prime(i.user)


async def fetch_members(guild: discord.Guild) -> dict[int, discord.Member]:
    out: dict[int, discord.Member] = {}
    async for m in guild.fetch_members(limit=None):
        if not m.bot:
            out[m.id] = m
    return out


def get_announce_channel(guild: discord.Guild):
    return discord.utils.get(guild.text_channels, name=ANNOUNCE_CHANNEL_NAME)


# -------------------------
# XP CORE
# -------------------------
def award_xp(c: sqlite3.Connection, gid: int, uid: int, amount: int, ts: int) -> int:
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
    if not member_ids:
        return {}

    with db() as c:
        placeholders = ",".join("?" for _ in member_ids)
        rows = c.execute(
            f"SELECT user_id, xp FROM users WHERE guild_id=? AND user_id IN ({placeholders})",
            (gid, *member_ids),
        ).fetchall()

    xp_map = {int(r["user_id"]): int(r["xp"]) for r in rows}

    eligible = [(uid, xp_map.get(uid, 0)) for uid in member_ids if xp_map.get(uid, 0) >= INITIATE_EXIT_XP]
    eligible.sort(key=lambda x: (-x[1], x[0]))

    topA = {uid for uid, _ in eligible[:TOP_ASCENDANT]}
    nextE = {uid for uid, _ in eligible[TOP_ASCENDANT:TOP_ASCENDANT + NEXT_EMBER]}

    out: dict[int, str] = {}
    for uid in member_ids:
        x = xp_map.get(uid, 0)
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
    return f"Prime + {computed}" if has_prime(m) else computed


# -------------------------
# ROLE SYNC (DEBOUNCED)
# -------------------------
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
    roles_by_name = {r.name: r for r in guild.roles}
    managed = [roles_by_name[n] for n in ROLE_NAMES if n in roles_by_name]

    members = await fetch_members(guild)
    ids = list(members.keys())

    async with DB_LOCK:
        with db() as c:
            ensure_users_exist(c, guild.id, ids)
            c.commit()

    rank_map = compute_rank_map(guild.id, ids)

    ok = failed = 0
    edits = 0

    for uid, m in members.items():
        target_name = rank_map.get(uid, ROLE_INITIATE)
        target_role = roles_by_name.get(target_name)
        if not target_role:
            failed += 1
            continue

        current_managed = [r for r in managed if r in m.roles]
        if len(current_managed) == 1 and current_managed[0].id == target_role.id:
            continue

        to_remove = [r for r in current_managed if r.id != target_role.id]

        try:
            if to_remove:
                await m.remove_roles(*to_remove, reason="Rank sync")
            if target_role not in m.roles:
                await m.add_roles(target_role, reason="Rank sync")
            ok += 1
            edits += 1
        except Exception:
            failed += 1

        if edits and edits % ROLE_SYNC_SLEEP_EVERY_EDITS == 0:
            await asyncio.sleep(ROLE_SYNC_SLEEP_SECONDS)

    return ok, failed


# -------------------------
# NOTIFY (helpers)
# -------------------------
def _ping_label(mode: str) -> str:
    return {"here": "@here", "everyone": "@everyone", "role": "Role"}.get(mode, "No ping")


def _can_post(guild: discord.Guild, channel: discord.abc.Messageable, content: str, has_files: bool, has_embeds: bool) -> str | None:
    """Return a human readable problem, or None if ok / cannot check."""
    me = guild.me
    if not me:
        return "Bot member not resolved (guild.me is None)."

    if not isinstance(channel, discord.abc.GuildChannel):
        return None  # can't reliably check perms on non-guild channel-like objects

    perms = channel.permissions_for(me)

    if not perms.view_channel:
        return "I can't view that channel."
    if not perms.send_messages:
        return "Missing **Send Messages** permission."
    if has_files and not perms.attach_files:
        return "Missing **Attach Files** permission."
    if has_embeds and not perms.embed_links:
        return "Missing **Embed Links** permission."

    if ("@everyone" in content or "@here" in content) and not perms.mention_everyone:
        return "Missing **Mention @everyone/@here** permission."

    return None


class NotifyModal(discord.ui.Modal, title="Build Announcement"):
    def __init__(self, title_default: str = "", body_default: str = "", note_default: str = ""):
        super().__init__()
        self.title_in = discord.ui.TextInput(
            label="Title", max_length=80, placeholder="Short title", default=title_default
        )
        self.body_in = discord.ui.TextInput(
            label="Body", style=discord.TextStyle.paragraph, max_length=2000, default=body_default
        )
        self.note_in = discord.ui.TextInput(
            label="Note (optional)",
            style=discord.TextStyle.paragraph,
            required=False,
            max_length=400,
            placeholder="Shows as -# note text",
            default=note_default,
        )
        self.add_item(self.title_in)
        self.add_item(self.body_in)
        self.add_item(self.note_in)

        self.title_value = ""
        self.body_value = ""
        self.note_value = ""

    async def on_submit(self, interaction: discord.Interaction):
        self.title_value = str(self.title_in.value).strip()
        self.body_value = str(self.body_in.value).strip()
        self.note_value = str(self.note_in.value).strip() if self.note_in.value else ""
        await interaction.response.defer(ephemeral=True)


class EveryoneConfirmModal(discord.ui.Modal, title="Confirm DM @everyone"):
    def __init__(self):
        super().__init__()
        self.confirm_in = discord.ui.TextInput(
            label='Type "EVERYONE" to confirm',
            max_length=16,
            placeholder="EVERYONE",
        )
        self.add_item(self.confirm_in)
        self.ok = False

    async def on_submit(self, interaction: discord.Interaction):
        self.ok = (str(self.confirm_in.value).strip().upper() == "EVERYONE")
        await interaction.response.defer(ephemeral=True)


class NotifyView(discord.ui.View):
    def __init__(self, author_id: int, channel: discord.abc.Messageable, title: str, body: str, note: str = ""):
        super().__init__(timeout=900)
        self.author_id = author_id
        self.channel: discord.abc.Messageable = channel  # post destination

        self.title = title
        self.body = body
        self.note = note.strip() if note else ""

        self.ping_mode = "none"   # none/here/everyone/role
        self.role: discord.Role | None = None

        self.dm_enabled = False
        self.dm_everyone_armed = False

        self.images: list[tuple[bytes, str]] = []
        self.waiting_for_image = False

        self._channel_select = discord.ui.ChannelSelect(
            placeholder="Post channel",
            min_values=1,
            max_values=1,
            channel_types=[discord.ChannelType.text, discord.ChannelType.news],
            row=0,
        )
        self._channel_select.callback = self._on_channel

        self._ping_select = discord.ui.Select(
            placeholder="Ping mode",
            options=[
                discord.SelectOption(label="No ping", value="none"),
                discord.SelectOption(label="@here", value="here"),
                discord.SelectOption(label="@everyone", value="everyone"),
                discord.SelectOption(label="Role ping", value="role"),
            ],
            row=1,
        )
        self._ping_select.callback = self._on_ping_mode

        self._role_select = discord.ui.RoleSelect(
            placeholder="Role",
            min_values=0,
            max_values=1,
            row=2,
        )
        self._role_select.callback = self._on_role

        self._dm_button = discord.ui.Button(
            label="DM pinged users: OFF",
            style=discord.ButtonStyle.secondary,
            row=3,
        )
        self._dm_button.callback = self._toggle_dm

        self.add_item(self._channel_select)
        self.add_item(self._ping_select)

        self._refresh_dynamic_controls()
        self._refresh_dm_button()
        self._refresh_image_controls()
        self._refresh_add_pictures_label()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.author_id

    async def _edit_preview(self, interaction: discord.Interaction, content: str, view: discord.ui.View | None):
        """
        Ephemeral messages frequently fail to update via interaction.message.edit().
        Always try edit_original_response first, then fall back.
        """
        try:
            await interaction.edit_original_response(content=content, view=view)
            return
        except Exception:
            pass

        try:
            if interaction.message:
                await interaction.message.edit(content=content, view=view)
        except Exception:
            pass

    def _ping_text(self) -> str:
        if self.ping_mode == "here":
            return "@here"
        if self.ping_mode == "everyone":
            return "@everyone"
        if self.ping_mode == "role" and self.role:
            return self.role.mention
        return ""

    def _dm_allowed(self) -> bool:
        return self.ping_mode in ("everyone", "role")

    def _dm_audience_ok(self) -> bool:
        if self.ping_mode == "everyone":
            return True
        if self.ping_mode == "role":
            return self.role is not None
        return False

    def _refresh_dynamic_controls(self):
        role_should_exist = (self.ping_mode == "role")
        role_in_view = (self._role_select in self.children)
        if role_should_exist and not role_in_view:
            self.add_item(self._role_select)
        elif (not role_should_exist) and role_in_view:
            self.remove_item(self._role_select)

        dm_should_exist = self._dm_allowed() and self._dm_audience_ok()
        dm_in_view = (self._dm_button in self.children)
        if dm_should_exist and not dm_in_view:
            self.add_item(self._dm_button)
        elif (not dm_should_exist) and dm_in_view:
            self.dm_enabled = False
            self.dm_everyone_armed = False
            self.remove_item(self._dm_button)

    def _refresh_image_controls(self):
        has_imgs = bool(self.images)
        for item in (self.remove_last_picture, self.clear_pictures):
            in_view = item in self.children
            if has_imgs and not in_view:
                self.add_item(item)
            if (not has_imgs) and in_view:
                self.remove_item(item)

    def _refresh_add_pictures_label(self):
        self.add_pictures.label = "Add More Picture(s)" if self.images else "Add Picture(s)"

    def render_public(self) -> str:
        ping = self._ping_text()
        title = auto_bold_phrases(self.title)
        body = auto_bold_phrases(self.body)
        note = auto_bold_phrases(self.note) if self.note else ""

        msg = f"{(ping + chr(10)) if ping else ''}# {title}\n{body}"
        if note:
            msg += f"\n-# {note}"
        return msg

    def render_preview(self) -> str:
        msg = self.render_public()
        if self.images:
            msg += f"\n\n🖼️ Images: **{len(self.images)}**"
        return msg

    def _preview_header(self) -> str:
        ping = _ping_label(self.ping_mode)

        ch_mention = "Unknown"
        try:
            ch_mention = getattr(self.channel, "mention", "Unknown")
        except Exception:
            pass

        extra = f"\nChannel: {ch_mention} | Ping: **{ping}**"
        if self.ping_mode == "role" and self.role:
            extra += f" (**{self.role.name}**)"
        if self._dm_allowed():
            extra += f" | DM: **{'ON' if self.dm_enabled else 'OFF'}**"
        if self.waiting_for_image:
            extra += " | 🖼️ **WAITING FOR IMAGE**"
        return "📝 **Preview (private)** — Edit / Pictures / Post / Cancel" + extra + "\n\n"

    def _refresh_dm_button(self):
        if self.ping_mode == "everyone":
            self._dm_button.label = f"DM @everyone: {'ON' if self.dm_enabled else 'OFF'}"
            self._dm_button.style = discord.ButtonStyle.danger if self.dm_enabled else discord.ButtonStyle.secondary
        else:
            self._dm_button.label = f"DM role: {'ON' if self.dm_enabled else 'OFF'}"
            self._dm_button.style = discord.ButtonStyle.success if self.dm_enabled else discord.ButtonStyle.secondary

    async def _rerender(self, interaction: discord.Interaction):
        self._refresh_dynamic_controls()
        if self._dm_button in self.children:
            self._refresh_dm_button()
        self._refresh_image_controls()
        self._refresh_add_pictures_label()
        await interaction.response.edit_message(
            content=self._preview_header() + self.render_preview(),
            view=self,
        )

    async def _on_channel(self, interaction: discord.Interaction):
        picked = self._channel_select.values[0]
        guild = interaction.guild
        if not guild:
            return await self._rerender(interaction)

        resolved = guild.get_channel(picked.id)
        if resolved is None:
            try:
                resolved = await guild.fetch_channel(picked.id)
            except Exception:
                resolved = None

        if resolved is not None and hasattr(resolved, "send"):
            self.channel = resolved  # type: ignore

        await self._rerender(interaction)

    async def _on_ping_mode(self, interaction: discord.Interaction):
        self.ping_mode = self._ping_select.values[0]
        self.dm_enabled = False
        self.dm_everyone_armed = False

        if self.ping_mode != "role":
            self.role = None

        await self._rerender(interaction)

    async def _on_role(self, interaction: discord.Interaction):
        self.role = self._role_select.values[0] if self._role_select.values else None
        if self.ping_mode == "role" and not self.role:
            self.dm_enabled = False
        await self._rerender(interaction)

    async def _toggle_dm(self, interaction: discord.Interaction):
        if not self._dm_allowed():
            return await self._rerender(interaction)

        if not self._dm_audience_ok():
            try:
                await interaction.response.send_message("Pick a role first (Role ping).", ephemeral=True)
            except Exception:
                pass
            return

        if self.dm_enabled:
            self.dm_enabled = False
            self.dm_everyone_armed = False
            return await self._rerender(interaction)

        if self.ping_mode == "everyone":
            modal = EveryoneConfirmModal()
            await interaction.response.send_modal(modal)
            await modal.wait()
            if not modal.ok:
                try:
                    await interaction.followup.send("❌ Cancelled DM @everyone.", ephemeral=True)
                except Exception:
                    pass
                return

            self.dm_everyone_armed = True
            self.dm_enabled = True

            # ✅ update ephemeral preview reliably
            await self._edit_preview(interaction, self._preview_header() + self.render_preview(), self)
            return

        self.dm_enabled = True
        await self._rerender(interaction)

    async def _dm_targets(self, guild: discord.Guild) -> list[discord.Member]:
        members = await fetch_members(guild)
        ms = list(members.values())

        if self.ping_mode == "everyone":
            return [m for m in ms if not m.bot]
        if self.ping_mode == "role" and self.role:
            return [m for m in ms if (not m.bot and self.role in m.roles)]
        return []

    def _build_embeds_and_files(self) -> tuple[list[discord.Embed], list[discord.File]]:
        if not self.images:
            return [], []

        embeds: list[discord.Embed] = []
        files: list[discord.File] = []

        for data, filename in self.images[:NOTIFY_MAX_IMAGES]:
            f = discord.File(fp=io.BytesIO(data), filename=filename)
            files.append(f)
            e = discord.Embed()
            e.set_image(url=f"attachment://{filename}")
            embeds.append(e)

        return embeds, files

    async def _send_dms(self, guild: discord.Guild, content: str, embeds: list[discord.Embed]) -> tuple[int, int]:
        sent = failed = 0
        targets = await self._dm_targets(guild)

        for m in targets:
            try:
                files: list[discord.File] = []
                if self.images:
                    for data, filename in self.images[:NOTIFY_MAX_IMAGES]:
                        files.append(discord.File(fp=io.BytesIO(data), filename=filename))

                if files and embeds:
                    await m.send(content, embeds=embeds, files=files)
                elif embeds:
                    await m.send(content, embeds=embeds)
                else:
                    await m.send(content)

                sent += 1
            except Exception:
                failed += 1

            await asyncio.sleep(0.6)

        return sent, failed

    async def _wait_for_image_message(self, channel: discord.abc.Messageable, author_id: int) -> discord.Message | None:
        def check(m: discord.Message) -> bool:
            if m.author.id != author_id:
                return False
            if m.channel.id != getattr(channel, "id", None):
                return False
            if not m.attachments:
                return False
            return any(_is_image_attachment(a) for a in m.attachments)

        try:
            msg = await bot.wait_for("message", timeout=NOTIFY_IMAGE_WAIT_SECONDS, check=check)
            return msg
        except asyncio.TimeoutError:
            return None

    async def _append_images_from_message(self, msg: discord.Message) -> tuple[bool, str]:
        imgs = [a for a in msg.attachments if _is_image_attachment(a)]
        if not imgs:
            return False, "No image attachment found."

        if len(self.images) >= NOTIFY_MAX_IMAGES:
            return False, f"Already at max images ({NOTIFY_MAX_IMAGES})."

        added = 0
        names_used = {fn for _, fn in self.images}

        for a in imgs:
            if len(self.images) >= NOTIFY_MAX_IMAGES:
                break

            if a.size and a.size > NOTIFY_MAX_IMAGE_BYTES:
                continue

            try:
                data = await a.read()
            except Exception:
                continue

            if len(data) > NOTIFY_MAX_IMAGE_BYTES:
                continue

            desired = _safe_filename(a.filename or "image.png")
            filename = _dedupe_filename(names_used, desired)

            self.images.append((data, filename))
            added += 1

        if added == 0:
            return False, "No valid images added (type/size?)."

        if len(imgs) > added:
            return True, f"Added {added} image(s). Some were skipped (limit/size)."

        return True, f"Added {added} image(s)."

    @discord.ui.button(label="Edit", style=discord.ButtonStyle.blurple, row=3)
    async def edit(self, interaction: discord.Interaction, _):
        modal = NotifyModal(self.title, self.body, self.note)
        await interaction.response.send_modal(modal)
        await modal.wait()

        if not modal.title_value or not modal.body_value:
            try:
                await interaction.followup.send("❌ Title/Body required.", ephemeral=True)
            except Exception:
                pass
            return

        self.title = modal.title_value
        self.body = modal.body_value
        self.note = modal.note_value

        # ✅ ephemeral-safe update
        self._refresh_dynamic_controls()
        self._refresh_image_controls()
        self._refresh_add_pictures_label()
        if self._dm_button in self.children:
            self._refresh_dm_button()

        await self._edit_preview(interaction, self._preview_header() + self.render_preview(), self)

    @discord.ui.button(label="Add Picture(s)", style=discord.ButtonStyle.secondary, row=3)
    async def add_pictures(self, interaction: discord.Interaction, _):
        if self.waiting_for_image:
            return await self._rerender(interaction)

        if len(self.images) >= NOTIFY_MAX_IMAGES:
            return await interaction.response.send_message(
                f"Already at max images ({NOTIFY_MAX_IMAGES}).", ephemeral=True
            )

        self.waiting_for_image = True
        await self._rerender(interaction)

        channel = interaction.channel
        if channel is None:
            self.waiting_for_image = False
            await self._edit_preview(interaction, self._preview_header() + self.render_preview(), self)
            return

        msg = await self._wait_for_image_message(channel, interaction.user.id)
        if msg is None:
            self.waiting_for_image = False
            await self._edit_preview(
                interaction,
                self._preview_header() + self.render_preview() + "\n\n⏱️ *(Image upload timed out.)*",
                self
            )
            return

        ok, note = await self._append_images_from_message(msg)

        try:
            await msg.delete()
        except Exception:
            pass

        self.waiting_for_image = False
        tail = f"\n\n✅ *{note}*" if ok else f"\n\n❌ *{note}*"

        self._refresh_image_controls()
        self._refresh_add_pictures_label()
        await self._edit_preview(interaction, self._preview_header() + self.render_preview() + tail, self)

    @discord.ui.button(label="Remove Last", style=discord.ButtonStyle.gray, row=3)
    async def remove_last_picture(self, interaction: discord.Interaction, _):
        if self.images:
            self.images.pop()
        await self._rerender(interaction)

    @discord.ui.button(label="Clear Pictures", style=discord.ButtonStyle.gray, row=3)
    async def clear_pictures(self, interaction: discord.Interaction, _):
        self.images.clear()
        await self._rerender(interaction)

    @discord.ui.button(label="Post", style=discord.ButtonStyle.green, row=4)
    async def post(self, interaction: discord.Interaction, _):
        if not interaction.guild:
            return await interaction.response.send_message("Guild only.", ephemeral=True)

        if self.dm_enabled and self.ping_mode == "everyone":
            modal = EveryoneConfirmModal()
            await interaction.response.send_modal(modal)
            await modal.wait()
            if not modal.ok:
                try:
                    await interaction.followup.send("❌ Post cancelled (DM @everyone not confirmed).", ephemeral=True)
                except Exception:
                    pass
                return

        # show immediate UI feedback
        try:
            await interaction.response.edit_message(content="⏳ Posting…", view=None)
        except Exception:
            try:
                await interaction.followup.send("⏳ Posting…", ephemeral=True)
            except Exception:
                pass

        content = self.render_public()
        embeds, files = self._build_embeds_and_files()

        # permission check
        problem = _can_post(interaction.guild, self.channel, content, bool(files), bool(embeds))
        if problem:
            try:
                await interaction.edit_original_response(content=f"❌ Can't post: {problem}", view=None)
            except Exception:
                pass
            return

        try:
            if files and embeds:
                await self.channel.send(
                    content,
                    embeds=embeds,
                    files=files,
                    allowed_mentions=discord.AllowedMentions.all(),
                )  # type: ignore
            else:
                await self.channel.send(
                    content,
                    allowed_mentions=discord.AllowedMentions.all(),
                )  # type: ignore

            # ✅ update the ephemeral UI after success
            ch = getattr(self.channel, "mention", "the selected channel")
            try:
                await interaction.edit_original_response(content=f"✅ Posted in {ch}.", view=None)
            except Exception:
                pass

            if self.dm_enabled:
                sent, failed = await self._send_dms(interaction.guild, content, embeds)
                try:
                    await interaction.followup.send(f"DMs: {sent} sent, {failed} failed", ephemeral=True)
                except Exception:
                    pass

        except Exception as e:
            traceback.print_exc()
            msg = f"❌ Failed: `{type(e).__name__}: {e}`"
            try:
                await interaction.followup.send(msg, ephemeral=True)
            except Exception:
                try:
                    await interaction.edit_original_response(content=msg, view=None)
                except Exception:
                    pass

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.red, row=4)
    async def cancel(self, interaction: discord.Interaction, _):
        try:
            await interaction.response.edit_message(content="Cancelled.", view=None)
        except Exception:
            try:
                await interaction.followup.send("Cancelled.", ephemeral=True)
            except Exception:
                pass


@bot.tree.command(name="notify")
async def notify(interaction: discord.Interaction):
    if not interaction.guild:
        return await interaction.response.send_message("Guild only.", ephemeral=True)
    if not is_admin(interaction):
        return await interaction.response.send_message("Prime only.", ephemeral=True)

    default_channel = get_announce_channel(interaction.guild)
    if default_channel is None and isinstance(interaction.channel, discord.TextChannel):
        default_channel = interaction.channel
    if default_channel is None:
        default_channel = interaction.guild.text_channels[0] if interaction.guild.text_channels else None
    if default_channel is None:
        return await interaction.response.send_message("No text channels found.", ephemeral=True)

    modal = NotifyModal()
    await interaction.response.send_modal(modal)
    await modal.wait()

    if not modal.title_value or not modal.body_value:
        return

    view = NotifyView(interaction.user.id, default_channel, modal.title_value, modal.body_value, modal.note_value)
    await interaction.followup.send(
        view._preview_header() + view.render_preview(),
        view=view,
        ephemeral=True,
    )


# -------------------------
# POLLS (anonymous, timed, no swaps, hidden results until end)
# -------------------------
def _parse_duration_to_seconds(raw: str) -> int | None:
    """
    Accepts:
      - minutes as integer (e.g. "30")
      - "10m", "2h", "1d", "1h30m"
    Returns seconds or None.
    """
    s = (raw or "").strip().lower()
    if not s:
        return None
    if s.isdigit():
        mins = int(s)
        return mins * 60

    # composite like 1h30m
    total = 0
    for num, unit in re.findall(r"(\d+)\s*([smhd])", s):
        n = int(num)
        if unit == "s":
            total += n
        elif unit == "m":
            total += n * 60
        elif unit == "h":
            total += n * 3600
        elif unit == "d":
            total += n * 86400
    return total or None


def _poll_ping_text(mode: str, role: discord.Role | None) -> str:
    if mode == "here":
        return "@here"
    if mode == "everyone":
        return "@everyone"
    if mode == "role" and role:
        return role.mention
    return ""


def _poll_make_id() -> str:
    # short, url-safe-ish
    return secrets.token_urlsafe(9)


def _poll_time_left_text(ends_at: int) -> str:
    remaining = max(0, ends_at - now())
    if remaining <= 0:
        return "Ended"
    mins = remaining // 60
    hrs = mins // 60
    days = hrs // 24
    if days > 0:
        return f"{days}d {hrs % 24}h"
    if hrs > 0:
        return f"{hrs}h {mins % 60}m"
    return f"{mins}m"


def _poll_render_active(question: str, options: list[str], ends_at: int) -> str:
    q = auto_bold_phrases(question)
    lines = [f"🗳️ **POLL**", f"**{q}**", "", "*(Anonymous voting — results hidden until the poll ends. No vote changes.)*"]
    lines.append(f"⏳ Poll lasts: **{_poll_time_left_text(ends_at)}**")
    lines.append("")
    for i, opt in enumerate(options, start=1):
        lines.append(f"{i}. {auto_bold_phrases(opt)}")
    return "\n".join(lines)


def _poll_render_closed(question: str, options: list[str], counts: list[int], ends_at: int) -> str:
    total = sum(counts)
    q = auto_bold_phrases(question)
    lines = [f"🗳️ **POLL — CLOSED**", f"**{q}**", f"🕒 Ended: <t:{ends_at}:f>", ""]
    if total <= 0:
        lines.append("No votes were cast.")
        lines.append("")
        for i, opt in enumerate(options, start=1):
            lines.append(f"{i}. {auto_bold_phrases(opt)} — **0** (0%)")
        return "\n".join(lines)

    for i, opt in enumerate(options, start=1):
        c = counts[i - 1] if i - 1 < len(counts) else 0
        pct = (c / total) * 100.0
        lines.append(f"{i}. {auto_bold_phrases(opt)} — **{c}** ({pct:.1f}%)")
    lines.append("")
    lines.append(f"Total votes: **{total}**")
    return "\n".join(lines)


class PollModal(discord.ui.Modal, title="Create Poll"):
    def __init__(self):
        super().__init__()
        self.question_in = discord.ui.TextInput(
            label="Question",
            max_length=200,
            placeholder="What are we voting on?",
        )
        self.duration_in = discord.ui.TextInput(
            label="Duration (minutes or 10m / 2h / 1d / 1h30m)",
            max_length=32,
            placeholder="Example: 30 or 2h",
        )
        self.options_in = discord.ui.TextInput(
            label="Options (one per line, 2–10)",
            style=discord.TextStyle.paragraph,
            max_length=1500,
            placeholder="Option A\nOption B\nOption C",
        )
        self.add_item(self.question_in)
        self.add_item(self.duration_in)
        self.add_item(self.options_in)

        self.question = ""
        self.duration_seconds: int | None = None
        self.options: list[str] = []

    async def on_submit(self, interaction: discord.Interaction):
        self.question = str(self.question_in.value).strip()
        self.duration_seconds = _parse_duration_to_seconds(str(self.duration_in.value))
        raw_opts = [o.strip() for o in str(self.options_in.value).splitlines()]
        self.options = [o for o in raw_opts if o]
        await interaction.response.defer(ephemeral=True)


class PollVoteView(discord.ui.View):
    def __init__(self, poll_id: str, options: list[str], ends_at: int, disabled: bool = False):
        super().__init__(timeout=None)
        self.poll_id = poll_id
        self.options = options
        self.ends_at = ends_at

        # build buttons
        for idx, label in enumerate(options):
            # 5 per row
            row = idx // 5
            self.add_item(PollVoteButton(poll_id=poll_id, option_index=idx, label=label[:80], row=row, disabled=disabled))


class PollVoteButton(discord.ui.Button):
    def __init__(self, poll_id: str, option_index: int, label: str, row: int, disabled: bool):
        super().__init__(
            label=label,
            style=discord.ButtonStyle.secondary,
            row=row,
            disabled=disabled,
            custom_id=f"poll:{poll_id}:{option_index}",
        )
        self.poll_id = poll_id
        self.option_index = option_index

    async def callback(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message("Guild only.", ephemeral=True)

        # load poll
        async with DB_LOCK:
            with db() as c:
                poll = c.execute("SELECT * FROM polls WHERE poll_id=?", (self.poll_id,)).fetchone()
                if not poll:
                    return await interaction.response.send_message("Poll not found.", ephemeral=True)

                if int(poll["closed"]) == 1 or now() >= int(poll["ends_at"]):
                    return await interaction.response.send_message("This poll has ended.", ephemeral=True)

                # enforce "no swaps" by PK (poll_id, user_id)
                try:
                    c.execute(
                        "INSERT INTO poll_votes (poll_id, user_id, option_index, voted_at) VALUES (?,?,?,?)",
                        (self.poll_id, interaction.user.id, int(self.option_index), now()),
                    )
                    c.commit()
                except sqlite3.IntegrityError:
                    return await interaction.response.send_message(
                        "You already voted in this poll. *(No vote changes.)*",
                        ephemeral=True,
                    )
                except Exception as e:
                    return await interaction.response.send_message(f"Vote failed: `{type(e).__name__}: {e}`", ephemeral=True)

        return await interaction.response.send_message("✅ Vote recorded. *(Anonymous — results revealed when the poll ends.)*", ephemeral=True)


class PollSetupView(discord.ui.View):
    def __init__(self, author_id: int, channel: discord.abc.Messageable, question: str, options: list[str], duration_seconds: int):
        super().__init__(timeout=900)
        self.author_id = author_id
        self.channel: discord.abc.Messageable = channel
        self.question = question
        self.options = options
        self.duration_seconds = duration_seconds

        self.ping_mode = "none"   # none/here/everyone/role
        self.role: discord.Role | None = None

        self.dm_enabled = False
        self.dm_everyone_armed = False

        self._channel_select = discord.ui.ChannelSelect(
            placeholder="Post channel",
            min_values=1,
            max_values=1,
            channel_types=[discord.ChannelType.text, discord.ChannelType.news],
            row=0,
        )
        self._channel_select.callback = self._on_channel

        self._ping_select = discord.ui.Select(
            placeholder="Ping mode",
            options=[
                discord.SelectOption(label="No ping", value="none"),
                discord.SelectOption(label="@here", value="here"),
                discord.SelectOption(label="@everyone", value="everyone"),
                discord.SelectOption(label="Role ping", value="role"),
            ],
            row=1,
        )
        self._ping_select.callback = self._on_ping_mode

        self._role_select = discord.ui.RoleSelect(
            placeholder="Role",
            min_values=0,
            max_values=1,
            row=2,
        )
        self._role_select.callback = self._on_role

        self._dm_button = discord.ui.Button(
            label="DM pinged users: OFF",
            style=discord.ButtonStyle.secondary,
            row=3,
        )
        self._dm_button.callback = self._toggle_dm

        self.add_item(self._channel_select)
        self.add_item(self._ping_select)

        self._refresh_dynamic_controls()
        self._refresh_dm_button()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.author_id

    def _dm_allowed(self) -> bool:
        return self.ping_mode in ("everyone", "role")

    def _dm_audience_ok(self) -> bool:
        if self.ping_mode == "everyone":
            return True
        if self.ping_mode == "role":
            return self.role is not None
        return False

    def _refresh_dm_button(self):
        if self.ping_mode == "everyone":
            self._dm_button.label = f"DM @everyone: {'ON' if self.dm_enabled else 'OFF'}"
            self._dm_button.style = discord.ButtonStyle.danger if self.dm_enabled else discord.ButtonStyle.secondary
        else:
            self._dm_button.label = f"DM role: {'ON' if self.dm_enabled else 'OFF'}"
            self._dm_button.style = discord.ButtonStyle.success if self.dm_enabled else discord.ButtonStyle.secondary

    def _refresh_dynamic_controls(self):
        role_should_exist = (self.ping_mode == "role")
        role_in_view = (self._role_select in self.children)
        if role_should_exist and not role_in_view:
            self.add_item(self._role_select)
        elif (not role_should_exist) and role_in_view:
            self.remove_item(self._role_select)

        dm_should_exist = self._dm_allowed() and self._dm_audience_ok()
        dm_in_view = (self._dm_button in self.children)
        if dm_should_exist and not dm_in_view:
            self.add_item(self._dm_button)
        elif (not dm_should_exist) and dm_in_view:
            self.dm_enabled = False
            self.dm_everyone_armed = False
            self.remove_item(self._dm_button)

    def _preview_header(self) -> str:
        ping = _ping_label(self.ping_mode)
        ch_mention = "Unknown"
        try:
            ch_mention = getattr(self.channel, "mention", "Unknown")
        except Exception:
            pass

        extra = f"\nChannel: {ch_mention} | Ping: **{ping}**"
        if self.ping_mode == "role" and self.role:
            extra += f" (**{self.role.name}**)"
        if self._dm_allowed():
            extra += f" | DM: **{'ON' if self.dm_enabled else 'OFF'}**"
        return "🗳️ **Poll Setup (private)** — Post / Cancel" + extra + "\n\n"

    def render_preview(self) -> str:
        ends_at = now() + self.duration_seconds
        return _poll_render_active(self.question, self.options, ends_at)

    async def _rerender(self, interaction: discord.Interaction):
        self._refresh_dynamic_controls()
        if self._dm_button in self.children:
            self._refresh_dm_button()
        await interaction.response.edit_message(content=self._preview_header() + self.render_preview(), view=self)

    async def _on_channel(self, interaction: discord.Interaction):
        picked = self._channel_select.values[0]
        guild = interaction.guild
        if not guild:
            return await self._rerender(interaction)

        resolved = guild.get_channel(picked.id)
        if resolved is None:
            try:
                resolved = await guild.fetch_channel(picked.id)
            except Exception:
                resolved = None

        if resolved is not None and hasattr(resolved, "send"):
            self.channel = resolved  # type: ignore

        await self._rerender(interaction)

    async def _on_ping_mode(self, interaction: discord.Interaction):
        self.ping_mode = self._ping_select.values[0]
        self.dm_enabled = False
        self.dm_everyone_armed = False
        if self.ping_mode != "role":
            self.role = None
        await self._rerender(interaction)

    async def _on_role(self, interaction: discord.Interaction):
        self.role = self._role_select.values[0] if self._role_select.values else None
        if self.ping_mode == "role" and not self.role:
            self.dm_enabled = False
        await self._rerender(interaction)

    async def _toggle_dm(self, interaction: discord.Interaction):
        if not self._dm_allowed():
            return await self._rerender(interaction)
        if not self._dm_audience_ok():
            try:
                await interaction.response.send_message("Pick a role first (Role ping).", ephemeral=True)
            except Exception:
                pass
            return

        if self.dm_enabled:
            self.dm_enabled = False
            self.dm_everyone_armed = False
            return await self._rerender(interaction)

        if self.ping_mode == "everyone":
            modal = EveryoneConfirmModal()
            await interaction.response.send_modal(modal)
            await modal.wait()
            if not modal.ok:
                try:
                    await interaction.followup.send("❌ Cancelled DM @everyone.", ephemeral=True)
                except Exception:
                    pass
                return
            self.dm_everyone_armed = True
            self.dm_enabled = True
            try:
                await interaction.edit_original_response(content=self._preview_header() + self.render_preview(), view=self)
            except Exception:
                pass
            return

        self.dm_enabled = True
        await self._rerender(interaction)

    async def _poll_dm_targets(self, guild: discord.Guild) -> list[discord.Member]:
        members = await fetch_members(guild)
        ms = list(members.values())
        if self.ping_mode == "everyone":
            return [m for m in ms if not m.bot]
        if self.ping_mode == "role" and self.role:
            return [m for m in ms if (not m.bot and self.role in m.roles)]
        return []

    async def _poll_send_dms(self, guild: discord.Guild, content: str) -> tuple[int, int]:
        sent = failed = 0
        targets = await self._poll_dm_targets(guild)
        for m in targets:
            try:
                await m.send(content)
                sent += 1
            except Exception:
                failed += 1
            await asyncio.sleep(0.6)
        return sent, failed

    @discord.ui.button(label="Post Poll", style=discord.ButtonStyle.green, row=4)
    async def post_poll(self, interaction: discord.Interaction, _):
        if not interaction.guild:
            return await interaction.response.send_message("Guild only.", ephemeral=True)

        # confirm DM everyone again right before posting (same as notify)
        if self.dm_enabled and self.ping_mode == "everyone":
            modal = EveryoneConfirmModal()
            await interaction.response.send_modal(modal)
            await modal.wait()
            if not modal.ok:
                try:
                    await interaction.followup.send("❌ Post cancelled (DM @everyone not confirmed).", ephemeral=True)
                except Exception:
                    pass
                return

        try:
            await interaction.response.edit_message(content="⏳ Posting poll…", view=None)
        except Exception:
            try:
                await interaction.followup.send("⏳ Posting poll…", ephemeral=True)
            except Exception:
                pass

        # resolve role id for storage (even if role object disappears later)
        role_id = int(self.role.id) if (self.ping_mode == "role" and self.role) else None

        poll_id = _poll_make_id()
        created_at = now()
        ends_at = created_at + int(self.duration_seconds)

        # clamp duration safety
        min_s = POLL_MIN_MINUTES * 60
        max_s = POLL_MAX_DAYS * 86400
        if ends_at - created_at < min_s:
            ends_at = created_at + min_s
        if ends_at - created_at > max_s:
            ends_at = created_at + max_s

        ping_text = _poll_ping_text(self.ping_mode, self.role)
        content_top = f"{(ping_text + chr(10)) if ping_text else ''}{_poll_render_active(self.question, self.options, ends_at)}"

        # permission check
        problem = _can_post(interaction.guild, self.channel, content_top, has_files=False, has_embeds=False)
        if problem:
            try:
                await interaction.edit_original_response(content=f"❌ Can't post: {problem}", view=None)
            except Exception:
                pass
            return

        view = PollVoteView(poll_id=poll_id, options=self.options, ends_at=ends_at, disabled=False)

        try:
            msg = await self.channel.send(
                content_top,
                view=view,
                allowed_mentions=discord.AllowedMentions.all(),
            )  # type: ignore
        except Exception as e:
            msg2 = f"❌ Failed to post poll: `{type(e).__name__}: {e}`"
            try:
                await interaction.edit_original_response(content=msg2, view=None)
            except Exception:
                pass
            return

        # store poll
        async with DB_LOCK:
            with db() as c:
                c.execute(
                    """
                    INSERT INTO polls (poll_id, guild_id, channel_id, message_id, created_by, created_at, ends_at,
                                      question, options_json, ping_mode, role_id, dm_enabled, closed)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0)
                    """,
                    (
                        poll_id,
                        interaction.guild.id,
                        int(getattr(self.channel, "id", 0)),
                        msg.id,
                        interaction.user.id,
                        created_at,
                        ends_at,
                        self.question,
                        json.dumps(self.options, ensure_ascii=False),
                        self.ping_mode,
                        role_id,
                        1 if self.dm_enabled else 0,
                    ),
                )
                c.commit()

        # UI success
        ch = getattr(self.channel, "mention", "the selected channel")
        try:
            await interaction.edit_original_response(content=f"✅ Poll posted in {ch}.", view=None)
        except Exception:
            pass

        # DM audience (link + short text). Keeps it lean.
        if self.dm_enabled:
            jump = msg.jump_url
            dm_text = f"🗳️ New poll: **{self.question}**\nVote here: {jump}"
            sent, failed = await self._poll_send_dms(interaction.guild, dm_text)
            try:
                await interaction.followup.send(f"DMs: {sent} sent, {failed} failed", ephemeral=True)
            except Exception:
                pass

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.red, row=4)
    async def cancel(self, interaction: discord.Interaction, _):
        try:
            await interaction.response.edit_message(content="Cancelled.", view=None)
        except Exception:
            try:
                await interaction.followup.send("Cancelled.", ephemeral=True)
            except Exception:
                pass


@bot.tree.command(name="poll", description="Create an anonymous timed poll (results hidden until end, no vote changes).")
async def poll(interaction: discord.Interaction):
    if not interaction.guild:
        return await interaction.response.send_message("Guild only.", ephemeral=True)
    if not is_admin(interaction):
        return await interaction.response.send_message("Prime only.", ephemeral=True)

    # pick a sensible default channel like notify does
    default_channel = get_announce_channel(interaction.guild)
    if default_channel is None and isinstance(interaction.channel, discord.TextChannel):
        default_channel = interaction.channel
    if default_channel is None:
        default_channel = interaction.guild.text_channels[0] if interaction.guild.text_channels else None
    if default_channel is None:
        return await interaction.response.send_message("No text channels found.", ephemeral=True)

    modal = PollModal()
    await interaction.response.send_modal(modal)
    await modal.wait()

    if not modal.question or not modal.options:
        return

    if modal.duration_seconds is None:
        try:
            return await interaction.followup.send("❌ Invalid duration.", ephemeral=True)
        except Exception:
            return

    opts = modal.options[:POLL_MAX_OPTIONS]
    if len(opts) < 2:
        return await interaction.followup.send("❌ Need at least 2 options.", ephemeral=True)

    # clamp duration safety
    min_s = POLL_MIN_MINUTES * 60
    max_s = POLL_MAX_DAYS * 86400
    dur = int(modal.duration_seconds)
    if dur < min_s:
        dur = min_s
    if dur > max_s:
        dur = max_s

    view = PollSetupView(interaction.user.id, default_channel, modal.question, opts, dur)
    await interaction.followup.send(
        view._preview_header() + view.render_preview(),
        view=view,
        ephemeral=True,
    )


async def _close_poll_row(row: sqlite3.Row):
    poll_id = str(row["poll_id"])
    guild_id = int(row["guild_id"])
    channel_id = int(row["channel_id"])
    message_id = int(row["message_id"])
    ends_at = int(row["ends_at"])
    question = str(row["question"])
    try:
        options = json.loads(row["options_json"])
        if not isinstance(options, list):
            options = []
    except Exception:
        options = []

    # count votes
    counts = [0 for _ in range(len(options))]
    async with DB_LOCK:
        with db() as c:
            vote_rows = c.execute("SELECT option_index, COUNT(*) AS n FROM poll_votes WHERE poll_id=? GROUP BY option_index", (poll_id,)).fetchall()
            for vr in vote_rows:
                idx = int(vr["option_index"])
                n = int(vr["n"])
                if 0 <= idx < len(counts):
                    counts[idx] = n

            # mark closed in DB
            c.execute("UPDATE polls SET closed=1 WHERE poll_id=?", (poll_id,))
            c.commit()

    guild = bot.get_guild(guild_id)
    if not guild:
        return

    channel = guild.get_channel(channel_id)
    if channel is None:
        try:
            channel = await guild.fetch_channel(channel_id)
        except Exception:
            channel = None
    if channel is None or not hasattr(channel, "fetch_message"):
        return

    try:
        msg = await channel.fetch_message(message_id)  # type: ignore
    except Exception:
        return

    # remove ping on close (so it doesn't re-ping on edit)
    closed_text = _poll_render_closed(question, options, counts, ends_at)
    closed_view = PollVoteView(poll_id=poll_id, options=options, ends_at=ends_at, disabled=True)

    try:
        await msg.edit(content=closed_text, view=closed_view)
    except Exception:
        # fallback: at least disable view
        try:
            await msg.edit(view=None)
        except Exception:
            pass


@tasks.loop(seconds=POLL_CLOSE_CHECK_SECONDS)
async def poll_close_loop():
    cutoff = now()
    due: list[sqlite3.Row] = []
    async with DB_LOCK:
        with db() as c:
            rows = c.execute(
                "SELECT * FROM polls WHERE closed=0 AND ends_at <= ? ORDER BY ends_at ASC LIMIT 25",
                (cutoff,),
            ).fetchall()
            due = list(rows)

    for r in due:
        try:
            await _close_poll_row(r)
        except Exception:
            traceback.print_exc()
        await asyncio.sleep(0.4)


# -------------------------
# EVENTS
# -------------------------
@bot.event
async def on_ready():
    init_db()
    await bot.tree.sync()
    if not decay_loop.is_running():
        decay_loop.start()
    if not vc_xp_loop.is_running():
        vc_xp_loop.start()
    if not poll_close_loop.is_running():
        poll_close_loop.start()

    print("Ready:", bot.user)


@bot.event
async def on_message(msg: discord.Message):
    if msg.author.bot or not msg.guild:
        return
    await bot.process_commands(msg)

    if len((msg.content or "").strip()) < MIN_MESSAGE_CHARS:
        return

    ts = int(msg.created_at.timestamp())

    gained = 0
    async with DB_LOCK:
        with db() as c:
            u = get_user(c, msg.guild.id, msg.author.id)
            if ts < int(u["chat_cooldown"]):
                return

            gained = award_xp(c, msg.guild.id, msg.author.id, CHAT_XP_PER_TICK, ts)

            if gained:
                c.execute(
                    "UPDATE users SET chat_cooldown=? WHERE guild_id=? AND user_id=?",
                    (ts + CHAT_COOLDOWN_SECONDS, msg.guild.id, msg.author.id),
                )

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

        async with DB_LOCK:
            with db() as c:
                for vc in guild.voice_channels:
                    humans = [m for m in vc.members if not m.bot]
                    if len(humans) < 2:
                        continue

                    for m in humans:
                        if m.voice and (m.voice.deaf or m.voice.self_deaf):
                            continue

                        u = get_user(c, guild.id, m.id)
                        minutes = int(u["vc_minutes"]) + 1

                        if minutes >= VC_MINUTES_PER_XP:
                            if award_xp(c, guild.id, m.id, 1, ts):
                                any_gain = True
                            minutes = 0

                        c.execute(
                            "UPDATE users SET vc_minutes=? WHERE guild_id=? AND user_id=?",
                            (minutes, guild.id, m.id),
                        )

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

        async with DB_LOCK:
            with db() as c:
                rows = c.execute(
                    "SELECT user_id, xp, last_active FROM users WHERE guild_id=?",
                    (guild.id,),
                ).fetchall()

                for r in rows:
                    xp = int(r["xp"])
                    if xp <= 0 or int(r["last_active"]) >= cutoff:
                        continue

                    loss = max(int(xp * DECAY_PERCENT_PER_DAY), DECAY_MIN_XP_PER_DAY)
                    new_xp = clamp_xp(xp - loss)
                    if xp >= DECAY_FLOOR_XP:
                        new_xp = max(DECAY_FLOOR_XP, new_xp)

                    if new_xp != xp:
                        c.execute(
                            "UPDATE users SET xp=? WHERE guild_id=? AND user_id=?",
                            (new_xp, guild.id, int(r["user_id"])),
                        )
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

    async with DB_LOCK:
        with db() as c:
            ensure_users_exist(c, guild.id, ids)
            c.commit()
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
        ephemeral=True,
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

    async with DB_LOCK:
        with db() as c:
            ensure_users_exist(c, guild.id, ids)
            c.commit()
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
        lines.append(
            f"{place:>4}. {m.display_name} — {xp} XP — {display_rank(m, rank_map.get(uid, ROLE_INITIATE))}"
        )

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
        return await interaction.response.send_message("Prime only.", ephemeral=True)

    guild = interaction.guild
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    await interaction.response.defer(ephemeral=not announce)

    scanned = awarded = skipped = throttle = 0

    async with DB_LOCK:
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
                            c.execute(
                                "UPDATE users SET chat_cooldown=? WHERE guild_id=? AND user_id=?",
                                (ts + CHAT_COOLDOWN_SECONDS, guild.id, msg.author.id),
                            )

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
        ephemeral=not announce,
    )


@bot.tree.command(name="resetranks")
@app_commands.describe(member="Optional single member")
async def resetranks(interaction: discord.Interaction, member: discord.Member | None = None):
    if not interaction.guild:
        return await interaction.response.send_message("Guild only.", ephemeral=True)
    if not is_admin(interaction):
        return await interaction.response.send_message("Prime only.", ephemeral=True)

    guild = interaction.guild
    await interaction.response.defer(ephemeral=True)

    members = await fetch_members(guild)
    targets = [member.id] if member else list(members.keys())

    changed = 0
    async with DB_LOCK:
        with db() as c:
            ensure_users_exist(c, guild.id, targets)
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
    await interaction.followup.send(
        f"Reset complete\nChanged XP: {changed}\nRole Sync: {ok}/{failed}",
        ephemeral=True,
    )


@bot.tree.command(name="setxp")
@app_commands.describe(member="User", xp="New XP", announce="Public?")
async def setxp(interaction: discord.Interaction, member: discord.Member, xp: int, announce: bool = False):
    if not interaction.guild:
        return await interaction.response.send_message("Guild only.", ephemeral=True)
    if not is_admin(interaction):
        return await interaction.response.send_message("Prime only.", ephemeral=True)

    xp = clamp_xp(xp)
    await interaction.response.defer(ephemeral=not announce)

    async with DB_LOCK:
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
    await interaction.followup.send(
        f"Set {member.display_name} → {xp} XP\nSync {ok}/{failed}",
        ephemeral=not announce,
    )


# -------------------------
# RUN
# -------------------------
token = os.getenv("DISCORD_TOKEN")
if not token:
    raise RuntimeError("DISCORD_TOKEN missing")
bot.run(token)
