# mishmarsh_record

Follows TARGET_USER_ID across servers, joins their voice channel, writes one WAV per speaker.

## Setup (Windows)
```
.venv\Scripts\activate
pip uninstall -y opuslib
pip install -r requirements.txt
copy .env.example .env   # fill DISCORD_BOT_TOKEN, TARGET_USER_ID
python bot.py
```
No Opus build/DLL needed: discord.py ships libopus-0.x64.dll in discord/bin/.

## Output
`recordings/session_<guild>_<YYYYmmdd_HHMMSS>/<username>_<userid>.wav` — 48 kHz stereo 16-bit,
silence-padded from each speaker's first packet so tracks stay time-aligned.

## Pipeline (voice_patch.py)
UDP packet -> aead_xchacha20_poly1305_rtpsize decrypt (AAD = RTP header, nonce = trailing 4 bytes)
-> strip RTP extension -> DAVE E2EE decrypt (davey, keyed by user from SPEAKING op) -> Opus
-> recorder.py decodes per frame with discord.opus.Decoder.

## Log signals
- `New track: name_id.wav` — audio is flowing for that user
- `transport decrypt failed` — cipher/nonce mismatch
- `DAVE decrypt failed` — E2EE frames arriving but session can't decrypt them
