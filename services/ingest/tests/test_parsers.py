import pytest

from nzbidx_ingest.parsers import normalize_subject


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
    assert normalize_subject(subject) == expected
