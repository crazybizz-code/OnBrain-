"""
audio_processor.py — Production-ready audio pipeline for OnBrain AI Bot

Pipeline:
    raw OGG bytes (Telegram)
        → convert_ogg_to_wav()   [pydub + ffmpeg: resample to 16kHz mono]
        → split_audio()          [chunk at silence, max 45s each]
        → transcribe_chunks()    [Whisper API, retry on failure]
        → merge_transcriptions() [join chunks into one clean string]

Design:
    - All functions are pure (bytes in, bytes/str out) — easy to unit-test
    - Stateless: no global state, safe for concurrent async use
    - Swappable: replace transcribe_chunk() to use a different STT backend
    - ffmpeg required in PATH (provided by Dockerfile)

Author: OnBrain AI
"""

from __future__ import annotations

import asyncio
import io
import logging
import os
import tempfile
from typing import TYPE_CHECKING

logger = logging.getLogger("onbrain-ai-bot")

# ── Optional imports (graceful degradation) ──────────────────────────────────
try:
    from pydub import AudioSegment
    from pydub.silence import split_on_silence
    PYDUB_AVAILABLE = True
except ImportError:
    PYDUB_AVAILABLE = False
    logger.warning("⚠️  pydub not installed — audio chunking disabled, using raw OGG")

# ── Constants ─────────────────────────────────────────────────────────────────
TARGET_SAMPLE_RATE = 16_000   # Hz — Whisper's native sample rate
TARGET_CHANNELS    = 1        # Mono
MAX_CHUNK_MS       = 45_000   # 45 seconds per chunk (Whisper limit is 25 MB / ~10 min)
MIN_SILENCE_MS     = 500      # Minimum silence duration to split on
SILENCE_THRESH_DB  = -40      # dBFS — anything quieter is considered silence
MAX_WHISPER_BYTES  = 24 * 1024 * 1024  # 24 MB safety margin (Whisper limit is 25 MB)
WHISPER_RETRIES    = 2        # Number of retry attempts per chunk


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1: OGG → WAV conversion
# ─────────────────────────────────────────────────────────────────────────────

def convert_ogg_to_wav(ogg_bytes: bytes) -> bytes:
    """Convert Telegram OGG/Opus audio bytes to 16kHz mono WAV bytes.

    Uses pydub + ffmpeg under the hood. Falls back to raw bytes if pydub
    is unavailable (Whisper can handle OGG directly, just less accurately).

    Args:
        ogg_bytes: Raw bytes of a Telegram voice message (.ogg/Opus format)

    Returns:
        WAV bytes at 16kHz mono, normalized to -20 dBFS.
    """
    if not PYDUB_AVAILABLE:
        logger.warning("⚠️  pydub unavailable — skipping OGG→WAV conversion")
        return ogg_bytes

    try:
        # Load OGG from bytes — pydub uses ffmpeg internally
        audio = AudioSegment.from_file(io.BytesIO(ogg_bytes), format="ogg")

        # Resample: 16 kHz mono (Whisper's optimal input format)
        audio = audio.set_frame_rate(TARGET_SAMPLE_RATE)
        audio = audio.set_channels(TARGET_CHANNELS)

        # Normalize loudness to -20 dBFS so quiet recordings are audible
        # This significantly improves accuracy for soft-spoken users
        target_dBFS = -20.0
        change_dBFS = target_dBFS - audio.dBFS
        if abs(change_dBFS) > 1:  # only adjust if meaningful difference
            audio = audio.apply_gain(change_dBFS)

        # Export as WAV
        buf = io.BytesIO()
        audio.export(buf, format="wav")
        wav_bytes = buf.getvalue()

        logger.info(
            f"🎵 Audio converted: {len(ogg_bytes):,}B OGG → {len(wav_bytes):,}B WAV "
            f"({audio.duration_seconds:.1f}s, {TARGET_SAMPLE_RATE}Hz mono)"
        )
        return wav_bytes

    except Exception as exc:
        logger.warning(f"⚠️  OGG→WAV conversion failed: {exc} — falling back to raw OGG")
        return ogg_bytes


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: Split audio into chunks
# ─────────────────────────────────────────────────────────────────────────────

def split_audio(
    audio_bytes: bytes,
    fmt: str = "wav",
    max_chunk_ms: int = MAX_CHUNK_MS,
) -> list[tuple[bytes, str]]:
    """Split audio into chunks of at most max_chunk_ms milliseconds.

    First tries to split at natural silence points. If the audio has no
    detectable silence (loud continuous speech), falls back to hard splitting
    by time.

    Args:
        audio_bytes: Audio data in WAV (or OGG if conversion failed).
        fmt: Format string for pydub ("wav" or "ogg").
        max_chunk_ms: Maximum chunk length in milliseconds.

    Returns:
        List of (chunk_bytes, format) tuples ready for Whisper.
        Returns [(audio_bytes, fmt)] (single chunk) if audio is short enough.
    """
    if not PYDUB_AVAILABLE:
        return [(audio_bytes, fmt)]

    try:
        audio = AudioSegment.from_file(io.BytesIO(audio_bytes), format=fmt)
        total_ms = len(audio)

        # Short audio — no need to split
        if total_ms <= max_chunk_ms and len(audio_bytes) < MAX_WHISPER_BYTES:
            logger.info(f"🎵 Audio {total_ms/1000:.1f}s — no splitting needed")
            return [(audio_bytes, fmt)]

        logger.info(f"🎵 Splitting {total_ms/1000:.1f}s audio into chunks...")

        # Try silence-based splitting first (more natural sentence boundaries)
        chunks = split_on_silence(
            audio,
            min_silence_len=MIN_SILENCE_MS,
            silence_thresh=SILENCE_THRESH_DB,
            keep_silence=250,  # keep 250ms of silence at edges for context
        )

        if not chunks:
            # No silence found — use raw audio as single chunk
            chunks = [audio]

        # Merge small chunks and hard-split oversized ones
        merged: list[AudioSegment] = _merge_and_split(chunks, max_chunk_ms)

        # Serialize each chunk to bytes
        result: list[tuple[bytes, str]] = []
        for i, chunk in enumerate(merged):
            buf = io.BytesIO()
            chunk.export(buf, format="wav")
            chunk_bytes = buf.getvalue()
            logger.info(
                f"  Chunk {i+1}/{len(merged)}: {len(chunk)/1000:.1f}s, "
                f"{len(chunk_bytes):,}B"
            )
            result.append((chunk_bytes, "wav"))

        return result

    except Exception as exc:
        logger.warning(f"⚠️  Audio splitting failed: {exc} — using single chunk")
        return [(audio_bytes, fmt)]


def _merge_and_split(
    chunks: list["AudioSegment"],
    max_ms: int,
) -> list["AudioSegment"]:
    """Merge short silence-split chunks and hard-split any that exceed max_ms."""
    merged: list[AudioSegment] = []
    current = AudioSegment.empty()

    for chunk in chunks:
        # If adding this chunk would exceed max_ms, flush current first
        if len(current) + len(chunk) > max_ms and len(current) > 0:
            merged.append(current)
            current = AudioSegment.empty()

        # If a single chunk is already over max_ms, hard-split it
        if len(chunk) > max_ms:
            # Flush what we have
            if len(current) > 0:
                merged.append(current)
                current = AudioSegment.empty()
            # Hard-split the oversized chunk
            for start in range(0, len(chunk), max_ms):
                merged.append(chunk[start: start + max_ms])
        else:
            current += chunk

    if len(current) > 0:
        merged.append(current)

    return merged


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3: Transcribe a single chunk via Whisper
# ─────────────────────────────────────────────────────────────────────────────

async def transcribe_chunk(
    client: "AsyncOpenAI",  # type: ignore[name-defined]
    chunk_bytes: bytes,
    fmt: str = "wav",
    chunk_index: int = 0,
    total_chunks: int = 1,
) -> str | None:
    """Transcribe one audio chunk using OpenAI Whisper.

    Retries up to WHISPER_RETRIES times on transient errors.
    Returns None if transcription fails after all retries.

    Uzbek accuracy improvements applied:
      - prompt: primes Whisper with Uzbek vocabulary and writing style
      - response_format="text": avoids JSON parsing overhead
      - No language= param (Uzbek not officially listed; auto-detect + prompt works better)

    Args:
        client: Async OpenAI client instance.
        chunk_bytes: WAV or OGG bytes for this chunk.
        fmt: Audio format ("wav" or "ogg").
        chunk_index: 0-based index for logging.
        total_chunks: Total number of chunks for logging.

    Returns:
        Transcribed text string, or None on failure.
    """
    # Prompt in Uzbek to steer Whisper's language detection and output style.
    # According to OpenAI docs, whisper-1 matches the writing style of the prompt.
    UZBEK_PROMPT = (
        "O'zbek tilida so'zlashuv. "
        "Ismlar: Yodgorbek, Moxizoda, Jasurbek, Dilnoza, Sarvinoz. "
        "Savollar: ball necha? umumiy bali qancha? fanidan necha ball olgan?"
    )

    filename = f"chunk_{chunk_index}.{fmt}"

    for attempt in range(WHISPER_RETRIES + 1):
        try:
            result = await client.audio.transcriptions.create(
                model="whisper-1",
                file=(filename, io.BytesIO(chunk_bytes), f"audio/{fmt}"),
                response_format="text",
                prompt=UZBEK_PROMPT,
            )
            text = result.strip() if isinstance(result, str) else str(result).strip()
            logger.info(
                f"  ✅ Chunk {chunk_index + 1}/{total_chunks} transcribed "
                f"({len(chunk_bytes):,}B): {text[:60]!r}"
            )
            return text

        except Exception as exc:
            err_str = str(exc)
            if attempt < WHISPER_RETRIES:
                wait = 2 ** attempt  # exponential backoff: 1s, 2s
                logger.warning(
                    f"  ⚠️  Chunk {chunk_index + 1} attempt {attempt + 1} failed: "
                    f"{err_str[:80]} — retrying in {wait}s"
                )
                await asyncio.sleep(wait)
            else:
                logger.error(
                    f"  ❌ Chunk {chunk_index + 1} failed after "
                    f"{WHISPER_RETRIES + 1} attempts: {err_str[:120]}"
                )
                return None


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: Transcribe all chunks (main entry point)
# ─────────────────────────────────────────────────────────────────────────────

async def transcribe_audio(
    ogg_bytes: bytes,
    openai_api_key: str,
) -> tuple[str | None, str]:
    """Full pipeline: OGG bytes → transcribed Uzbek text.

    Orchestrates all steps:
        1. Convert OGG → 16kHz mono WAV
        2. Split into ≤45s chunks
        3. Transcribe each chunk (with retry)
        4. Merge results

    Args:
        ogg_bytes: Raw Telegram voice message bytes.
        openai_api_key: OpenAI API key for Whisper.

    Returns:
        Tuple of (transcribed_text | None, error_reason).
        transcribed_text is None on failure; error_reason is empty string on success.
    """
    # Import here to avoid circular dependency at module level
    try:
        from openai import AsyncOpenAI
    except ImportError:
        return None, "openai_not_installed"

    # ── Step 1: Convert OGG → WAV ────────────────────────────────────────────
    wav_bytes = await asyncio.to_thread(convert_ogg_to_wav, ogg_bytes)
    audio_fmt = "wav" if wav_bytes != ogg_bytes else "ogg"

    # ── Step 2: Split into chunks ─────────────────────────────────────────────
    chunks = await asyncio.to_thread(split_audio, wav_bytes, audio_fmt)
    total = len(chunks)
    logger.info(f"🎤 Transcribing {total} chunk(s)")

    # ── Step 3: Transcribe each chunk ─────────────────────────────────────────
    client = AsyncOpenAI(api_key=openai_api_key)
    texts: list[str] = []
    failed = 0

    for i, (chunk_bytes, fmt) in enumerate(chunks):
        text = await transcribe_chunk(client, chunk_bytes, fmt, i, total)
        if text:
            texts.append(text)
        else:
            failed += 1

    # ── Step 4: Merge results ─────────────────────────────────────────────────
    if not texts:
        if failed == total:
            return None, "all_chunks_failed"
        return None, "no_speech_detected"

    merged = merge_transcriptions(texts)

    if not merged.strip():
        return None, "empty_transcription"

    logger.info(f"✅ Final transcription ({len(merged)} chars): {merged[:100]!r}")
    return merged, ""


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5: Merge chunk transcriptions
# ─────────────────────────────────────────────────────────────────────────────

def merge_transcriptions(texts: list[str]) -> str:
    """Join chunk transcriptions into a single coherent string.

    Handles:
    - Removes duplicate words at chunk boundaries (Whisper overlap artifacts)
    - Proper spacing between chunks
    - Strips leading/trailing whitespace

    Args:
        texts: List of per-chunk transcription strings.

    Returns:
        Single merged transcription string.
    """
    if not texts:
        return ""
    if len(texts) == 1:
        return texts[0].strip()

    merged_parts: list[str] = [texts[0].strip()]

    for chunk_text in texts[1:]:
        chunk_text = chunk_text.strip()
        if not chunk_text:
            continue

        prev = merged_parts[-1]

        # Check for overlap: last word of prev == first word of next
        # (Whisper sometimes repeats the last word when chunking mid-sentence)
        prev_words = prev.split()
        next_words = chunk_text.split()

        if prev_words and next_words and prev_words[-1].lower() == next_words[0].lower():
            # Skip the duplicate leading word
            chunk_text = " ".join(next_words[1:])

        if chunk_text:
            merged_parts.append(chunk_text)

    return " ".join(merged_parts)
