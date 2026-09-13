from datetime import date

from jobs import bronze_ingestion as module


def test_parse_secret_json():
    assert module.parse_secret_string('{"WISTIA_API_TOKEN":"abc123"}') == "abc123"


def test_parse_secret_plain_text():
    assert module.parse_secret_string("abc123") == "abc123"


def test_initial_window_is_inclusive():
    start, end = module.derive_date_window(
        media_id="m1",
        watermarks={},
        start_override=None,
        end_override=None,
        initial_lookback_days=7,
        overlap_days=1,
        today=date(2026, 9, 10),
    )
    assert start == "2026-09-04"
    assert end == "2026-09-10"


def test_watermark_window_re_reads_last_successful_day():
    start, end = module.derive_date_window(
        media_id="m1",
        watermarks={
            "events": {"m1": {"last_successful_end_date": "2026-09-09"}}
        },
        start_override=None,
        end_override="2026-09-10",
        initial_lookback_days=90,
        overlap_days=1,
        today=date(2026, 9, 10),
    )
    assert start == "2026-09-09"
    assert end == "2026-09-10"


def test_visitor_filename_does_not_expose_key():
    visitor_key = "visitor-secret-123"
    name = module.visitor_filename(visitor_key)
    assert visitor_key not in name
    assert len(name) == 24
