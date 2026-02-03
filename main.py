# -------------------------
# NOTIFY (Prime-only, private preview -> posts to 📢announcements)
# -------------------------

class NotifyModal(discord.ui.Modal, title="Build Announcement"):
    title_in = discord.ui.TextInput(label="Title", max_length=80, placeholder="Short title")
    body_in = discord.ui.TextInput(label="Body", style=discord.TextStyle.paragraph, max_length=2000)
    note_in = discord.ui.TextInput(
        label="Note (optional)",
        style=discord.TextStyle.paragraph,
        required=False,
        max_length=400,
        placeholder="Shows as -# note text",
    )

    def __init__(self, preset_title: str = "", preset_body: str = "", preset_note: str = ""):
        super().__init__()
        if preset_title:
            self.title_in.default = preset_title[:80]
        if preset_body:
            self.body_in.default = preset_body[:2000]
        if preset_note:
            self.note_in.default = preset_note[:400]

        self.title_value = ""
        self.body_value = ""
        self.note_value = ""

    async def on_submit(self, interaction: discord.Interaction):
        self.title_value = str(self.title_in.value).strip()
        self.body_value = str(self.body_in.value).strip()
        self.note_value = str(self.note_in.value).strip() if self.note_in.value else ""
        await interaction.response.defer(ephemeral=True)


class ConfirmEveryoneDMModal(discord.ui.Modal, title="Confirm DM @everyone"):
    confirm_in = discord.ui.TextInput(
        label="Type CONFIRM",
        placeholder="CONFIRM",
        max_length=20,
    )

    def __init__(self, prompt_line: str):
        super().__init__()
        self.prompt_line = prompt_line
        self.ok = False

    async def on_submit(self, interaction: discord.Interaction):
        self.ok = (str(self.confirm_in.value).strip().upper() == "CONFIRM")
        await interaction.response.defer(ephemeral=True)


class NotifyView(discord.ui.View):
    def __init__(self, author_id: int, channel: discord.TextChannel, title: str, body: str, note: str = ""):
        super().__init__(timeout=600)
        self.author_id = author_id
        self.channel = channel

        self.title = title
        self.body = body
        self.note = note.strip() if note else ""

        # ping mode: none | here | everyone | role
        self.ping_mode = "none"
        self.role: discord.Role | None = None

        # DM behavior: only available if ping_mode != none
        self.dm_enabled = False

        # Double-confirm for @everyone DMs:
        self.dm_everyone_armed = False  # set true after first confirm

        # Ping select
        self.ping_select = discord.ui.Select(
            placeholder="Ping mode (optional)",
            options=[
                discord.SelectOption(label="No ping", value="none"),
                discord.SelectOption(label="@here", value="here"),
                discord.SelectOption(label="@everyone", value="everyone"),
                discord.SelectOption(label="Role ping", value="role"),
            ],
        )
        self.ping_select.callback = self._on_ping_mode
        self.add_item(self.ping_select)

        # Role select (only meaningful if ping_mode == role)
        self.role_select = discord.ui.RoleSelect(
            placeholder="Role (only used if Role ping)",
            min_values=0,
            max_values=1,
        )
        self.role_select.callback = self._on_role
        self.add_item(self.role_select)

        # DM toggle (shown only when ping is chosen)
        self.dm_select = discord.ui.Select(
            placeholder="DM the ping target? (only available if ping is chosen)",
            options=[
                discord.SelectOption(label="No DMs", value="off"),
                discord.SelectOption(label="DM the ping target", value="on"),
            ],
        )
        self.dm_select.callback = self._on_dm_toggle
        # NOTE: we don't add it yet; we add/remove dynamically in _refresh_components()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.author_id

    def _ping_text(self) -> str:
        if self.ping_mode == "here":
            return "@here"
        if self.ping_mode == "everyone":
            return "@everyone"
        if self.ping_mode == "role" and self.role:
            return self.role.mention
        return ""

    def render(self) -> str:
        ping = self._ping_text()
        msg = f"{(ping + chr(10)) if ping else ''}# {self.title}\n{self.body}"
        if self.note:
            msg += f"\n-# {self.note}"
        return msg

    def _status_line(self) -> str:
        ping = self._ping_text() or "none"
        dm = "on" if (self.dm_enabled and self.ping_mode != "none") else "off"
        if self.ping_mode == "everyone" and self.dm_enabled:
            dm += " (needs confirm)"
            if self.dm_everyone_armed:
                dm = "on (ARMED, final confirm on post)"
        return f"Ping: **{ping}** | DM: **{dm}**"

    def _refresh_components(self):
        # DM option only exists if ping chosen (not none)
        has_dm_item = any(isinstance(i, discord.ui.Select) and i is self.dm_select for i in self.children)
        should_have_dm = (self.ping_mode != "none")

        if should_have_dm and not has_dm_item:
            self.add_item(self.dm_select)
        if not should_have_dm and has_dm_item:
            self.remove_item(self.dm_select)

        # If ping removed, reset DM + arming
        if self.ping_mode == "none":
            self.dm_enabled = False
            self.dm_everyone_armed = False

        # If ping changes away from everyone, drop arming
        if self.ping_mode != "everyone":
            self.dm_everyone_armed = False

    async def _update_preview(self, interaction: discord.Interaction):
        self._refresh_components()
        await interaction.response.edit_message(
            content="📝 **Preview (private)**\n" + self._status_line() + "\n\n" + self.render(),
            view=self,
        )

    async def _on_ping_mode(self, interaction: discord.Interaction):
        self.ping_mode = self.ping_select.values[0]

        # If switching off role ping, clear role selection
        if self.ping_mode != "role":
            self.role = None

        # DM only allowed if ping chosen; switching ping resets DM arming rules
        if self.ping_mode == "none":
            self.dm_enabled = False
            self.dm_everyone_armed = False
        else:
            # keep dm_enabled as-is, but if you moved into everyone, require arming again
            if self.ping_mode == "everyone":
                self.dm_everyone_armed = False

        await self._update_preview(interaction)

    async def _on_role(self, interaction: discord.Interaction):
        self.role = self.role_select.values[0] if self.role_select.values else None
        await self._update_preview(interaction)

    async def _on_dm_toggle(self, interaction: discord.Interaction):
        val = self.dm_select.values[0]
        self.dm_enabled = (val == "on")

        # If DM is enabled with @everyone, require FIRST confirmation immediately
        if self.dm_enabled and self.ping_mode == "everyone" and not self.dm_everyone_armed:
            modal = ConfirmEveryoneDMModal("First confirmation (arming).")
            await interaction.response.send_modal(modal)
            await modal.wait()

            if not modal.ok:
                self.dm_enabled = False
                self.dm_everyone_armed = False
                return await interaction.followup.send("❌ DM @everyone not armed (confirmation failed).", ephemeral=True)

            self.dm_everyone_armed = True
            return await interaction.followup.send("✅ DM @everyone ARMED. Posting will require
