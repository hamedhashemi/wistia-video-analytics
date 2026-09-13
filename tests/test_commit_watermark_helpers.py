import importlib.util
from pathlib import Path


def _load_module():
    path = Path(__file__).parents[1] / "jobs" / "commit_watermark.py"
    spec = importlib.util.spec_from_file_location("commit_watermark", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_merge_watermarks_preserves_other_media():
    module = _load_module()
    current = {
        "events": {
            "old_media": {"last_successful_end_date": "2026-09-01"},
            "8hunphufxp": {"last_successful_end_date": "2026-09-10"},
        }
    }
    candidate = {
        "events": {
            "8hunphufxp": {"last_successful_end_date": "2026-09-12"},
            "9k4tbcdfg0": {"last_successful_end_date": "2026-09-12"},
        }
    }
    result = module.merge_watermarks(current, candidate, "run-123", "2026-09-12T12:00:00+00:00")
    assert result["events"]["old_media"]["last_successful_end_date"] == "2026-09-01"
    assert result["events"]["8hunphufxp"]["last_successful_end_date"] == "2026-09-12"
    assert result["events"]["9k4tbcdfg0"]["last_successful_end_date"] == "2026-09-12"
    assert result["last_pipeline_run_id"] == "run-123"


def test_parse_iso_z_suffix():
    module = _load_module()
    value = module.parse_iso("2026-09-12T12:00:00Z")
    assert value.utcoffset().total_seconds() == 0
