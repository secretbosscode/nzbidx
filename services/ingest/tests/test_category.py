import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from nzbidx_ingest.main import _infer_category, CATEGORY_MAP


def test_infer_movies():
    assert _infer_category("Awesome Film [movies]") == CATEGORY_MAP["movies"]
    assert (
        _infer_category("Some.Movie.2024.1080p.BluRay") == CATEGORY_MAP["movies_bluray"]
    )


def test_infer_tv():
    assert _infer_category("Great.Show.S01E02.720p.HDTV") == CATEGORY_MAP["tv_hd"]
    assert _infer_category("Another Series [tv]") == CATEGORY_MAP["tv"]


def test_infer_audio():
    assert _infer_category("Cool.Album.FLAC") == CATEGORY_MAP["audio_lossless"]
    assert _infer_category("Another.Track.mp3") == CATEGORY_MAP["audio_mp3"]


def test_infer_books():
    assert _infer_category("Interesting.Book.2023.EPUB") == CATEGORY_MAP["ebook"]
    assert _infer_category("Batman.Comic.CBZ") == CATEGORY_MAP["comics"]


def test_infer_xxx():
    assert _infer_category("Saucy.Movie.xxx.xvid") == CATEGORY_MAP["xxx_xvid"]
    assert _infer_category("Adult Flick [xxx]") == CATEGORY_MAP["xxx"]


def test_group_based_inference():
    assert _infer_category("Random", group="alt.binaries.movies") == CATEGORY_MAP["movies"]
    assert _infer_category("Random", group="alt.binaries.tv") == CATEGORY_MAP["tv"]
    assert _infer_category("Random", group="alt.binaries.music") == CATEGORY_MAP["audio"]
    assert _infer_category("Random", group="alt.binaries.ebooks") == CATEGORY_MAP["ebook"]
    assert _infer_category("Random", group="alt.binaries.xxx") == CATEGORY_MAP["xxx"]
