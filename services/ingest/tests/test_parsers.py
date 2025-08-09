import pytest

from nzbidx_ingest.parsers import (
    extract_book_tags,
    extract_music_tags,
    extract_xxx_tags,
    normalize_subject,
)


@pytest.mark.parametrize(
    "subject,expected",
    [
        ("Some.Movie.2021.REPOST.yEnc (1/25)", "Some Movie 2021"),
        ("Another_Sample_File__yEnc__[01/20]", "Another File"),
        (
            "Cool.Show.S01E02.720p.HDTV.x264-Group [01/15]",
            "Cool Show S01E02 720p HDTV x264-Group",
        ),
        ("REPOST__Another.Movie.Part1.yEnc (05/05)", "Another Movie Part1"),
        ("My.File.Name_[12345/12346] yEnc", "My File Name"),
    ],
)
def test_normalize_subject(subject: str, expected: str) -> None:
    norm, tags = normalize_subject(subject, with_tags=True)
    assert norm == expected
    assert isinstance(tags, list)


@pytest.mark.parametrize(
    "subject,expected_norm,expected_tags",
    [
        (
            "Metallica-BlackAlbum-1991-FLAC",
            "Metallica-BlackAlbum-1991-FLAC",
            {"artist": "Metallica", "album": "BlackAlbum", "year": "1991", "format": "FLAC"},
        ),
        (
            "Nirvana-Nevermind-1991-MP3-320",
            "Nirvana-Nevermind-1991-MP3-320",
            {
                "artist": "Nirvana",
                "album": "Nevermind",
                "year": "1991",
                "format": "MP3",
                "bitrate": "320",
            },
        ),
        (
            "Radiohead-KidA-2000-MP3-128",
            "Radiohead-KidA-2000-MP3-128",
            {
                "artist": "Radiohead",
                "album": "KidA",
                "year": "2000",
                "format": "MP3",
                "bitrate": "128",
            },
        ),
    ],
)
def test_extract_music_tags(subject: str, expected_norm: str, expected_tags: dict) -> None:
    norm, tags = normalize_subject(subject, with_tags=True)
    assert norm == expected_norm
    assert extract_music_tags(subject) == expected_tags
    assert set(tags) == {v.lower() for v in expected_tags.values()}


@pytest.mark.parametrize(
    "subject,expected_norm,expected_tags",
    [
        (
            "GeorgeOrwell-1984-1949-EPUB",
            "GeorgeOrwell-1984-1949-EPUB",
            {
                "author": "GeorgeOrwell",
                "title": "1984",
                "year": "1949",
                "format": "EPUB",
            },
        ),
        (
            "JRRRTolkien-TheHobbit-1937-MOBI",
            "JRRRTolkien-TheHobbit-1937-MOBI",
            {
                "author": "JRRRTolkien",
                "title": "TheHobbit",
                "year": "1937",
                "format": "MOBI",
            },
        ),
        (
            "AuthorName-SampleBook-2015-PDF-9781234567897",
            "AuthorName-SampleBook-2015-PDF-9781234567897",
            {
                "author": "AuthorName",
                "title": "SampleBook",
                "year": "2015",
                "format": "PDF",
                "isbn": "9781234567897",
            },
        ),
    ],
)
def test_extract_book_tags(subject: str, expected_norm: str, expected_tags: dict) -> None:
    norm, tags = normalize_subject(subject, with_tags=True)
    assert norm == expected_norm
    assert extract_book_tags(subject) == expected_tags
    assert set(tags) == {v.lower() for v in expected_tags.values()}


@pytest.mark.parametrize(
    "subject,expected_norm,expected_tags",
    [
        (
            "Brazzers.HotScene.2022.1080p",
            "Brazzers HotScene 2022 1080p",
            {
                "studio": "Brazzers HotScene",
                "date": "2022",
                "resolution": "1080p",
            },
        ),
        (
            "OnlyFans.SomeModel.2023.07.12",
            "OnlyFans SomeModel 2023 07 12",
            {
                "site": "OnlyFans SomeModel",
                "date": "2023.07.12",
            },
        ),
        (
            "RealityKings.AmazingShow.2021.720p",
            "RealityKings AmazingShow 2021 720p",
            {
                "studio": "RealityKings AmazingShow",
                "date": "2021",
                "resolution": "720p",
            },
        ),
    ],
)
def test_extract_xxx_tags(subject: str, expected_norm: str, expected_tags: dict) -> None:
    norm, tags = normalize_subject(subject, with_tags=True)
    assert norm == expected_norm
    assert extract_xxx_tags(subject) == expected_tags
    assert set(tags) == {v.lower() for v in expected_tags.values()}
