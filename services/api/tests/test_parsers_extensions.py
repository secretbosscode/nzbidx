import pytest

from nzbidx_ingest.config import AUDIO_EXTENSIONS, BOOK_EXTENSIONS
from nzbidx_ingest.parsers import (
    extract_book_tags,
    extract_music_tags,
    normalize_subject,
)


@pytest.mark.parametrize("ext", ["AAC", "M4A", "WAV", "OGG", "WMA"])
def test_music_extensions_tagged(ext: str) -> None:
    subject = f"Artist-Album-2024-{ext}"
    tags = extract_music_tags(subject)
    assert tags["format"] == ext

    _, tag_list = normalize_subject(subject, with_tags=True)
    assert ext.lower() in tag_list
    assert ext in AUDIO_EXTENSIONS


@pytest.mark.parametrize("ext", ["AZW3", "CBZ", "CBR"])
def test_book_extensions_tagged(ext: str) -> None:
    subject = f"Author-Title-2024-{ext}"
    tags = extract_book_tags(subject)
    assert tags["format"] == ext

    _, tag_list = normalize_subject(subject, with_tags=True)
    assert ext.lower() in tag_list
    assert ext in BOOK_EXTENSIONS
