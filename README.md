Tracks a target user across all servers the bot is in, auto-joins their
voice channel, and records channel audio to per-speaker WAV files.

Core design:
- Single reconcile loop owns voice presence. Each pass compares where the
  target is against where the bot actually is, and fixes any difference:
  join, follow on channel switch, leave, or rejoin after a disconnect.
  Voice state events only wake the loop; a per-guild lock serializes passes.
  This handles the cases a pure event handler misses (target already in
  voice at startup, silent disconnects, reboots).
- Custom voice receive (no external extensions). RecvVoiceClient subclasses
  discord.VoiceClient and reads raw RTP via add_socket_listener: transport
  decrypt (aead_xchacha20_poly1305_rtpsize) -> strip RTP extension -> DAVE
  E2EE decrypt (davey), keyed by user from the SPEAKING op.
- Per-speaker recording. Each Opus frame is decoded on arrival with
  discord.opus.Decoder; RTP timestamp gaps are filled with silence so every
  track stays aligned to the session timeline. Output: 48kHz stereo 16-bit.

Config (.env):
- DISCORD_BOT_TOKEN, TARGET_USER_ID
- RECORD_MODE=all|target (everyone in the channel, or only the target)
- RECORDINGS_DIR, LOG_FILE, POLL_SECONDS, TZ

Behavior:
- Joins self-muted (never self-deafened -- deafening would stop incoming
  audio and kill recording).
- Connects with invisible presence (shows offline in the member list).
- SIGTERM/Ctrl+C finalizes all open WAVs before exit so headers are written.

Deps: discord.py[voice] (bundles libopus + davey), python-dotenv.
