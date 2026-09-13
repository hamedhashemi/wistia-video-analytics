from scripts.api_exploration import find_first_value, record_count, schema_paths


def test_schema_paths_reports_types_not_values():
    payload = {"id": "abc", "metrics": {"plays": 3}, "items": [{"x": True}]}
    schema = schema_paths(payload)
    assert schema["$.id"] == "string"
    assert schema["$.metrics.plays"] == "integer"
    assert schema["$.items"] == "array"
    assert "abc" not in schema.values()


def test_record_count_for_list_wrapper():
    assert record_count({"events": [{}, {}, {}]}) == 3


def test_find_first_value_recursive():
    payload = {"events": [{"visitor_key": "visitor-123"}]}
    assert find_first_value(payload, {"visitor_key"}) == "visitor-123"
