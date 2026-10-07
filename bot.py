#!/usr/bin/env python3
"""
Discord voice recorder bot.
Tracks target user, auto-joins voice channels, records all audio.

Design: a single reconcile loop owns the bot's voice presence. Each pass it
compares where the target is against where the bot actually is, and fixes any
difference. This covers the cases a pure event handler misses:
  - target already in voice when the bot starts / reconnects
  - bot disconnected (kicked, moved, network drop) with the target still there
  - recording/connection state drifting out of sync
The loop wakes every POLL_SECONDS, or immediately when on_voice_state_update
signals the target moved. A per-guild lock serializes passes so overlapping
wakeups can't fight over the same voice client.
"""
import os
import asyncio
import discord
from discord.ext import commands
from dotenv import load_dotenv
import logging
import sys
import io
import signal
from logging.handlers import RotatingFileHandler
from pathlib import Path

if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

from voice_patch import RecvVoiceClient
from recorder import ChannelRecorder

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(os.getenv('LOG_FILE', 'bot.log'), maxBytes=5_000_000,
                            backupCount=3, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logging.getLogger('discord').setLevel(logging.WARNING)

TOKEN = os.getenv('DISCORD_BOT_TOKEN')
TARGET_USER_ID = int(os.getenv('TARGET_USER_ID') or 0)
RECORD_MODE = (os.getenv('RECORD_MODE') or 'all').strip().lower()   # all | target
RECORDINGS_DIR = Path(os.getenv('RECORDINGS_DIR', 'recordings'))
POLL_SECONDS = float(os.getenv('POLL_SECONDS', '5'))

if RECORD_MODE not in ('all', 'target'):
    print(f"Invalid RECORD_MODE={RECORD_MODE!r}, expected 'all' or 'target'")
    sys.exit(1)

if not TOKEN or not TARGET_USER_ID:
    logger.error('Missing DISCORD_BOT_TOKEN or TARGET_USER_ID')
    sys.exit(1)

intents = discord.Intents.default()
intents.members = True
intents.voice_states = True
intents.guilds = True
# status=invisible: the bot shows as offline in the member list (sorted into the
# offline group). It still connects, stays in voice, and records normally. Note
# this cannot hide it from a voice channel's own participant panel while it's
# connected -- only from the main online/presence list.
bot = commands.Bot(command_prefix='!', intents=intents,
                   status=discord.Status.invisible)

recording_sessions = {}          # guild_id -> ChannelRecorder
_guild_locks = {}                # guild_id -> asyncio.Lock
_wake = asyncio.Event()          # set by events to run a reconcile pass early


def _lock(guild_id: int) -> asyncio.Lock:
    lock = _guild_locks.get(guild_id)
    if lock is None:
        lock = _guild_locks[guild_id] = asyncio.Lock()
    return lock


def _target_channel(guild: discord.Guild):
    """Voice channel the target is in, or None."""
    m = guild.get_member(TARGET_USER_ID)
    if m and m.voice and m.voice.channel:
        return m.voice.channel
    return None


def _bot_channel(guild: discord.Guild):
    """Voice channel the bot is actually connected to, or None."""
    vc = guild.voice_client
    if vc and vc.is_connected() and vc.channel:
        return vc.channel
    return None


@bot.event
async def on_ready():
    logger.info(f'Bot logged in as {bot.user}')


@bot.event
async def on_voice_state_update(member, before, after):
    # Only the target's moves matter; wake the loop to react within ~instantly.
    if member.id == TARGET_USER_ID:
        _wake.set()


async def reconcile_guild(guild: discord.Guild):
    """Make the bot's presence match the target's presence in this guild."""
    async with _lock(guild.id):
        want = _target_channel(guild)
        have = _bot_channel(guild)
        want_id = want.id if want else None
        have_id = have.id if have else None

        if want_id == have_id:
            # Aligned on channel; make sure recording state matches connection.
            if have and guild.id not in recording_sessions:
                await _start_recording(guild)
            elif not have and guild.id in recording_sessions:
                await _stop_recording(guild)
            return

        # Channel mismatch: tear down, then join the target if they're in voice.
        if guild.id in recording_sessions or guild.voice_client:
            await _stop_recording(guild)
            await _leave(guild)

        if want is not None:
            if await _join(guild, want):
                await asyncio.sleep(0.5)     # let the voice session settle
                await _start_recording(guild)


async def _join(guild: discord.Guild, channel) -> bool:
    try:
        if guild.voice_client:               # clear any stale client first
            try:
                await guild.voice_client.disconnect(force=True)
            except Exception:
                pass
        # self_mute: the bot never speaks, so show it muted.
        # self_deaf MUST stay False -- deafening tells Discord to stop sending us
        # audio, which would kill recording.
        await channel.connect(cls=RecvVoiceClient, timeout=20.0, reconnect=False,
                              self_mute=True, self_deaf=False)
        logger.info(f'[{guild.name}] Joined {channel.name}')
        return True
    except Exception as e:
        logger.error(f'[{guild.name}] Join failed: {e}')
        try:
            if guild.voice_client:
                await guild.voice_client.disconnect(force=True)
        except Exception:
            pass
        return False


async def _leave(guild: discord.Guild):
    vc = guild.voice_client
    if vc:
        try:
            await vc.disconnect(force=True)
            logger.info(f'[{guild.name}] Left channel')
        except Exception as e:
            logger.error(f'[{guild.name}] Leave failed: {e}')


async def _start_recording(guild: discord.Guild) -> bool:
    vc = guild.voice_client
    if not vc or not vc.is_connected():
        return False
    if guild.id in recording_sessions:
        return True
    try:
        recorder = ChannelRecorder(
            guild.id,
            root=RECORDINGS_DIR,
            only_user=TARGET_USER_ID if RECORD_MODE == 'target' else None,
        )
        recorder.start(vc)
        recording_sessions[guild.id] = recorder
        return True
    except Exception as e:
        logger.error(f'[{guild.name}] Recording start failed: {e}')
        return False


async def _stop_recording(guild: discord.Guild) -> bool:
    recorder = recording_sessions.pop(guild.id, None)
    if not recorder:
        return False
    try:
        recorder.stop()
        return True
    except Exception as e:
        logger.error(f'[{guild.name}] Recording stop failed: {e}')
        return False


async def reconcile_loop():
    """Single owner of voice presence. Wakes on a timer or on target events."""
    await bot.wait_until_ready()
    logger.info('Reconcile loop started')
    while not bot.is_closed():
        for guild in list(bot.guilds):
            try:
                await reconcile_guild(guild)
            except Exception as e:
                logger.error(f'Reconcile failed in {guild.name}: {e}')
        # Sleep until the next tick or until an event wakes us, whichever first.
        try:
            await asyncio.wait_for(_wake.wait(), timeout=POLL_SECONDS)
        except asyncio.TimeoutError:
            pass
        finally:
            _wake.clear()


@bot.event
async def setup_hook():
    bot.loop.create_task(reconcile_loop())


def _finalize_all():
    """Close every open WAV so headers are written (unclosed WAVs are unreadable)."""
    for gid in list(recording_sessions):
        try:
            recording_sessions.pop(gid).stop()
        except Exception as e:
            logger.error(f'Finalize failed for {gid}: {e}')


def _sigterm(*_):
    raise KeyboardInterrupt  # docker stop -> same clean path as Ctrl+C


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, _sigterm)
    try:
        bot.run(TOKEN, log_handler=None)
    finally:
        _finalize_all()
        logger.info('Shut down')