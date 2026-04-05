"""
audio_processor.py — Production-ready Uzbek speech recognition pipeline
========================================================================

Pipeline (per voice message):
    raw OGG bytes (Telegram)
      → convert_ogg_to_wav()     [pydub+ffmpeg: 16kHz mono, normalize, trim silence edges]
      → reduce_noise()           [high-pass filter at 80Hz — removes mic rumble & wind noise]
      → split_audio()            [silence-based chunks, max 28s each for best Whisper accuracy]
      → transcribe_chunk()       [Whisper API, language=uz, temperature=0, retry 2x]
      → merge_transcriptions()   [join, deduplicate boundary words, remove repeated sentences]

Why 16kHz mono WAV?
    Whisper was trained on 16kHz audio. Sending OGG/Opus directly forces Whisper to
    re-decode internally; pre-converting avoids that extra lossy step.

Why language="uz"?
    Without it Whisper auto-detects and often picks Arabic (similar phonemes).
    Forcing "uz" gives a 20-40% WER improvement on Uzbek speech.

Why 28s chunks (not 45s)?
    Whisper's attention mechanism degrades on audio longer than ~30s.
    25-28s is the sweet spot for accuracy vs. number of API calls.

Why the high-pass filter?
    Cheap USB microphones and phone mics produce strong sub-80Hz rumble that
    Whisper's mel spectrogram registers as speech noise. Filtering it out
    reduces WER noticeably on low-quality recordings.

Why the dynamic prompt?
    Whisper treats the prompt as a style hint. Injecting column names and person
    names from the loaded sheet pre-loads the model's vocabulary so it spells
    them correctly rather than guessing phonetically.

Author: OnBrain AI
"""

from __future__ import annotations

import asyncio
import io
import logging
import re
from typing import Any

logger = logging.getLogger("onbrain-ai-bot")

# ── Optional imports ──────────────────────────────────────────────────────────
try:
    from pydub import AudioSegment
    from pydub.silence import split_on_silence, detect_nonsilent
    PYDUB_AVAILABLE = True
except ImportError:
    PYDUB_AVAILABLE = False
    logger.warning("⚠️  pydub not installed — audio chunking disabled, using raw OGG")

# ── Tunable constants ─────────────────────────────────────────────────────────
TARGET_SAMPLE_RATE  = 16_000   # Hz — Whisper's native sample rate
TARGET_CHANNELS     = 1        # Mono
TARGET_DBFS         = -18.0    # Normalize louder than -20 to catch soft speakers
MAX_CHUNK_MS        = 28_000   # 28s — optimal Whisper attention window (was 45s)
MIN_SILENCE_MS      = 400      # ms of silence needed to split (was 500)
SILENCE_THRESH_DB   = -38      # dBFS threshold for silence detection (was -40)
KEEP_SILENCE_MS     = 200      # ms of padding silence to keep at chunk edges
MAX_WHISPER_BYTES   = 24 * 1024 * 1024  # 24 MB safety limit
WHISPER_RETRIES     = 2        # retry count on transient API errors

# High-pass filter frequency — removes rumble below 80 Hz (mic noise, wind)
HPF_CUTOFF_HZ       = 80


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1: OGG → WAV conversion (16kHz mono, normalize, trim silence)
# ─────────────────────────────────────────────────────────────────────────────

def convert_ogg_to_wav(ogg_bytes: bytes) -> bytes:
    """Convert Telegram OGG/Opus audio bytes to 16kHz mono WAV bytes.

    Steps:
      1. Decode with ffmpeg via pydub
      2. Resample to 16 kHz mono (Whisper's optimal format)
      3. Normalize loudness to TARGET_DBFS (-18 dBFS) — louder than old -20
         to better capture soft-spoken users
      4. Trim leading / trailing silence (>500ms blocks, <-40dBFS)
         so Whisper doesn't waste attention on empty signal

    Falls back to raw OGG bytes if pydub/ffmpeg unavailable.
    """
    if not PYDUB_AVAILABLE:
        logger.warning("⚠️  pydub unavailable — skipping OGG→WAV conversion")
        return ogg_bytes

    try:
        audio: AudioSegment = AudioSegment.from_file(
            io.BytesIO(ogg_bytes), format="ogg"
        )

        # Resample + mono
        audio = audio.set_frame_rate(TARGET_SAMPLE_RATE).set_channels(TARGET_CHANNELS)

        # Normalize loudness — crucial for quiet speakers
        if audio.dBFS < -60:
            logger.warning("⚠️  Audio is nearly silent (%.1f dBFS)", audio.dBFS)
        else:
            delta = TARGET_DBFS - audio.dBFS
            if abs(delta) > 0.5:
                audio = audio.apply_gain(delta)

        # Trim leading/trailing silence (>500ms blocks)
        audio = _trim_edges(audio)

        buf = io.BytesIO()
        audio.export(buf, format="wav")
        wav_bytes = buf.getvalue()

        logger.info(
            "🎵 OGG→WAV: %d B → %d B (%.1fs, %.0f dBFS)",
            len(ogg_bytes), len(wav_bytes),
            audio.duration_seconds, audio.dBFS,
        )
        return wav_bytes

    except Exception as exc:
        logger.warning("⚠️  OGG→WAV conversion failed: %s — falling back to raw OGG", exc)
        return ogg_bytes


def _trim_edges(audio: "AudioSegment") -> "AudioSegment":
    """Strip leading/trailing silence longer than 500ms."""
    try:
        nonsilent = detect_nonsilent(audio, min_silence_len=500, silence_thresh=-40)
        if nonsilent:
            start_ms = max(0, nonsilent[0][0] - 200)
            end_ms   = min(len(audio), nonsilent[-1][1] + 200)
            return audio[start_ms:end_ms]
    except Exception:
        pass
    return audio


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: Noise reduction (high-pass filter at 80 Hz)
# ─────────────────────────────────────────────────────────────────────────────

def reduce_noise(wav_bytes: bytes) -> bytes:
    """Apply a high-pass filter to remove low-frequency noise.

    Removes microphone handling noise, wind rumble and desk vibration
    (all below 80 Hz). Speech starts at ~85 Hz so this filter has zero
    impact on voice quality while significantly cleaning up the signal
    Whisper receives.

    Returns wav_bytes unchanged if pydub is unavailable or filter fails.
    """
    if not PYDUB_AVAILABLE:
        return wav_bytes
    try:
        audio: AudioSegment = AudioSegment.from_file(io.BytesIO(wav_bytes), format="wav")
        audio = audio.high_pass_filter(HPF_CUTOFF_HZ)
        buf = io.BytesIO()
        audio.export(buf, format="wav")
        return buf.getvalue()
    except Exception as exc:
        logger.debug("High-pass filter skipped: %s", exc)
        return wav_bytes




# ─────────────────────────────────────────────────────────────────────────────
# STEP 3: Split audio into chunks
# ─────────────────────────────────────────────────────────────────────────────

def split_audio(
    audio_bytes: bytes,
    fmt: str = "wav",
    max_chunk_ms: int = MAX_CHUNK_MS,
) -> list[tuple[bytes, str]]:
    """Split audio into chunks of at most max_chunk_ms milliseconds.

    Strategy:
      1. Audio ≤ max_chunk_ms (28s) → return as single chunk
      2. Split at silence points (natural sentence boundaries, 400ms silence)
      3. Merge short adjacent chunks; hard-split any still-oversized chunks

    Args:
        audio_bytes: WAV (or OGG if conversion failed) audio bytes.
        fmt: Format string for pydub ("wav" or "ogg").
        max_chunk_ms: Maximum chunk length in milliseconds.

    Returns:
        List of (chunk_bytes, "wav") tuples ready for Whisper.
    """
    if not PYDUB_AVAILABLE:
        return [(audio_bytes, fmt)]

    try:
        audio: AudioSegment = AudioSegment.from_file(io.BytesIO(audio_bytes), format=fmt)
        total_ms = len(audio)

        if total_ms <= max_chunk_ms and len(audio_bytes) < MAX_WHISPER_BYTES:
            logger.info("🎵 Audio %.1fs — no splitting needed", total_ms / 1000)
            return [(audio_bytes, fmt)]

        logger.info("🎵 Splitting %.1fs audio into chunks...", total_ms / 1000)

        raw_chunks: list[AudioSegment] = split_on_silence(
            audio,
            min_silence_len=MIN_SILENCE_MS,
            silence_thresh=SILENCE_THRESH_DB,
            keep_silence=KEEP_SILENCE_MS,
        )

        if not raw_chunks:
            raw_chunks = [audio]

        merged = _merge_and_split(raw_chunks, max_chunk_ms)

        result: list[tuple[bytes, str]] = []
        for i, chunk in enumerate(merged):
            buf = io.BytesIO()
            chunk.export(buf, format="wav")
            chunk_b = buf.getvalue()
            logger.info(
                "  Chunk %d/%d: %.1fs, %d B",
                i + 1, len(merged), len(chunk) / 1000, len(chunk_b),
            )
            result.append((chunk_b, "wav"))

        return result

    except Exception as exc:
        logger.warning("⚠️  Audio splitting failed: %s — using single chunk", exc)
        return [(audio_bytes, fmt)]


def _merge_and_split(
    chunks: list["AudioSegment"],
    max_ms: int,
) -> list["AudioSegment"]:
    """Merge short silence-split chunks and hard-split any that exceed max_ms."""
    merged: list[AudioSegment] = []
    current = AudioSegment.empty()

    for chunk in chunks:
        if len(current) + len(chunk) > max_ms and len(current) > 0:
            merged.append(current)
            current = AudioSegment.empty()

        if len(chunk) > max_ms:
            if len(current) > 0:
                merged.append(current)
                current = AudioSegment.empty()
            for start in range(0, len(chunk), max_ms):
                merged.append(chunk[start: start + max_ms])
        else:
            current = current + chunk

    if len(current) > 0:
        merged.append(current)

    return merged


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: Transcribe a single chunk via Whisper
# ─────────────────────────────────────────────────────────────────────────────

async def transcribe_chunk(
    client: Any,
    chunk_bytes: bytes,
    fmt: str = "wav",
    chunk_index: int = 0,
    total_chunks: int = 1,
    extra_prompt: str = "",
) -> str | None:
    """Transcribe one audio chunk using OpenAI Whisper (language='uz').

    Key accuracy settings applied:
      - language="uz"    forces Uzbek; prevents Arabic/Tajik misdetection
      - temperature=0    deterministic output; reduces random hallucination
      - prompt           Uzbek domain vocabulary + optional sheet names
      - response_format  "text" — plaintext, no JSON parsing overhead

    Retries up to WHISPER_RETRIES times with exponential backoff.
    Returns None if all attempts fail.
    """
    base_prompt = (
        "O'zbek tilida so'zlashuv nutqi. "
        "Maktab, universitet, talaba, o'quvchi, fan, ball, umumiy ball, reyting. "
        "Ismlar: Yodgorbek, Moxizoda, Jasurbek, Dilnoza, Sarvinoz, Abdulloh, Kamola. "
        "Savollar: necha ball? umumiy bali qancha? kim eng ko'p ball olgan? "
        "fanidan necha ball olgan? ballini ayting."
    )
    prompt = f"{base_prompt} {extra_prompt}".strip() if extra_prompt else base_prompt

    filename = f"chunk_{chunk_index}.{fmt}"

    for attempt in range(WHISPER_RETRIES + 1):
        try:
            result = await client.audio.transcriptions.create(
                model="whisper-1",
                file=(filename, io.BytesIO(chunk_bytes), f"audio/{fmt}"),
                response_format="text",
                language="uz",    # ← forces Uzbek (was removed, now safe with preprocessing)
                temperature=0,    # ← deterministic: no random hallucination
                prompt=prompt,
            )
            text = result.strip() if isinstance(result, str) else str(result).strip()
            logger.info(
                "  ✅ Chunk %d/%d (attempt %d): %r",
                chunk_index + 1, total_chunks, attempt + 1, text[:70],
            )
            return text

        except Exception as exc:
            err = str(exc)
            if attempt < WHISPER_RETRIES:
                wait = 2 ** attempt   # 1s, 2s
                logger.warning(
                    "  ⚠️  Chunk %d attempt %d failed: %s — retry in %ds",
                    chunk_index + 1, attempt + 1, err[:80], wait,
                )
                await asyncio.sleep(wait)
            else:
                logger.error(
                    "  ❌ Chunk %d failed after %d attempts: %s",
                    chunk_index + 1, WHISPER_RETRIES + 1, err[:120],
                )
                return None


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5: Main entry point — full pipeline
# ─────────────────────────────────────────────────────────────────────────────

async def transcribe_audio(
    ogg_bytes: bytes,
    openai_api_key: str,
    sheet_vocabulary: str = "",
) -> tuple[str | None, str]:
    """Full pipeline: raw OGG bytes → clean Uzbek transcription.

    Steps:
      1. Convert OGG → 16kHz mono WAV (normalize, trim silence edges)
      2. High-pass filter (remove mic rumble below 80 Hz)
      3. Split into ≤28s chunks at silence boundaries
      4. Transcribe each chunk (Whisper, language=uz, temperature=0, retry)
      5. Merge and deduplicate chunk boundaries

    Args:
        ogg_bytes:        Raw Telegram voice message bytes.
        openai_api_key:   OpenAI API key for Whisper.
        sheet_vocabulary: Optional extra prompt text — pass column/person names
                          extracted from the loaded sheet via build_sheet_vocabulary().
                          Significantly boosts name spelling accuracy.

    Returns:
        (text, "")         on success
        (None, reason_key) on failure, where reason_key is one of:
            "openai_not_installed"
            "all_chunks_failed"
            "no_speech_detected"
            "empty_transcription"
    """
    try:
        from openai import AsyncOpenAI
    except ImportError:
        return None, "openai_not_installed"

    if not ogg_bytes:
        return None, "no_speech_detected"

    # Step 1: OGG → WAV
    wav_bytes = await asyncio.to_thread(convert_ogg_to_wav, ogg_bytes)
    audio_fmt = "wav" if wav_bytes is not ogg_bytes else "ogg"

    # Step 2: Noise reduction
    wav_bytes = await asyncio.to_thread(reduce_noise, wav_bytes)

    # Step 3: Split
    chunks = await asyncio.to_thread(split_audio, wav_bytes, audio_fmt)
    total  = len(chunks)
    logger.info("🎤 Transcribing %d chunk(s)", total)

    # Step 4: Transcribe
    client = AsyncOpenAI(api_key=openai_api_key)
    texts: list[str] = []
    failed = 0

    for i, (chunk_bytes, fmt) in enumerate(chunks):
        text = await transcribe_chunk(
            client, chunk_bytes, fmt,
            chunk_index=i,
            total_chunks=total,
            extra_prompt=sheet_vocabulary,
        )
        if text:
            texts.append(text)
        else:
            failed += 1

    # Step 5: Merge
    if not texts:
        return None, ("all_chunks_failed" if failed == total else "no_speech_detected")

    merged = merge_transcriptions(texts)
    if not merged.strip():
        return None, "empty_transcription"

    logger.info("✅ Final transcription (%d chars): %r", len(merged), merged[:100])
    return merged, ""




# ─────────────────────────────────────────────────────────────────────────────
# STEP 6: Merge chunk transcriptions
# ─────────────────────────────────────────────────────────────────────────────

def merge_transcriptions(texts: list[str]) -> str:
    """Join per-chunk transcriptions into one clean string.

    Handles:
    - Duplicate words at chunk boundaries (Whisper overlap artifact)
    - Duplicate sentences (Whisper sometimes repeats itself on short chunks)
    - Extra whitespace / punctuation spacing

    Args:
        texts: List of per-chunk transcription strings.

    Returns:
        Single merged transcription string.
    """
    if not texts:
        return ""
    if len(texts) == 1:
        return texts[0].strip()

    parts: list[str] = [texts[0].strip()]

    for chunk_text in texts[1:]:
        chunk_text = chunk_text.strip()
        if not chunk_text:
            continue

        prev_words  = parts[-1].split()
        next_words  = chunk_text.split()

        # Remove duplicate leading word at boundary (Whisper overlap artifact)
        if prev_words and next_words and prev_words[-1].lower() == next_words[0].lower():
            next_words = next_words[1:]

        rejoined = " ".join(next_words)

        # Skip chunks that are fully contained in the previous part
        # (Whisper hallucination: repeating itself on silence chunks)
        if rejoined and rejoined.lower() not in parts[-1].lower():
            parts.append(rejoined)

    result = " ".join(parts)
    # Collapse multiple spaces
    result = re.sub(r" {2,}", " ", result).strip()
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Utility: extract sheet vocabulary for dynamic Whisper prompt
# ─────────────────────────────────────────────────────────────────────────────

def build_sheet_vocabulary(session: Any, max_names: int = 40) -> str:
    """Extract person names and column headers from the session's loaded sheet data.

    Injects these into the Whisper prompt so it can spell domain-specific
    names correctly (student names, subject names, column titles) rather
    than guessing phonetically.

    Args:
        session:   UserSession object.
        max_names: Maximum number of names to include in the prompt.

    Returns:
        Short string like "Ismlar: Abdulloh, Kamola. Ustunlar: Matematika, Fizika."
        Returns "" if no sheet data is loaded.
    """
    all_rows: list[list[Any]] = []

    if getattr(session, "excel_files", None):
        for rows in session.excel_files.values():
            all_rows.extend(rows[:200])
    elif getattr(session, "excel_data", None):
        all_rows.extend(session.excel_data[:200])
    elif getattr(session, "all_sheets_data", None):
        for rows in session.all_sheets_data.values():
            all_rows.extend(rows[:200])
    elif getattr(session, "all_folder_sheets_data", None):
        for sheets in session.all_folder_sheets_data.values():
            for rows in sheets.values():
                all_rows.extend(rows[:200])

    if not all_rows:
        return ""

    # Collect header row (row 0) and text cells from all rows.
    # Strategy: any cell that is 2-50 chars long, contains NO digits,
    # and consists of letters/spaces/apostrophes is a candidate name/word.
    # This captures single-word names like "Yodgorbek" that a regex requiring
    # multi-word structure would miss.
    _text_only = re.compile(r"^[\w'\- ]{2,50}$", re.UNICODE)
    _has_digit  = re.compile(r"\d")

    headers: list[str] = []
    names:   list[str] = []
    seen:    set[str]  = set()

    for row_idx, row in enumerate(all_rows):
        for col_idx, cell in enumerate(row):
            val = str(cell).strip()
            if not val or len(val) < 2 or len(val) > 50:
                continue
            if row_idx == 0:
                # First row = column headers (subject names, field names)
                if val.lower() not in seen:
                    seen.add(val.lower())
                    headers.append(val)
            else:
                # Skip purely numeric cells (scores, roll numbers)
                if _has_digit.search(val):
                    continue
                # Keep text cells — likely names, categories
                if _text_only.match(val) and val.lower() not in seen:
                    seen.add(val.lower())
                    names.append(val)
                    if len(names) >= max_names:
                        break
        if len(names) >= max_names:
            break

    parts: list[str] = []
    if names:
        parts.append("Ismlar: " + ", ".join(names) + ".")
    if headers:
        parts.append("Ustunlar: " + ", ".join(headers[:10]) + ".")

    return " ".join(parts)
