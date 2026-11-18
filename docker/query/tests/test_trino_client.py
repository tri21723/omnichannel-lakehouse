import importlib.util
import sys
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location("trino_client", Path("docker/query/trino_client.py"))
client = importlib.util.module_from_spec(spec)
sys.modules["trino_client"] = client
spec.loader.exec_module(client)


def test_collect_folds_pages_and_keeps_column_order():
    pages = [
        {"nextUri": "x"},
        {"columns": [{"name": "a"}, {"name": "b"}], "data": [[1, "x"]], "nextUri": "y"},
        {"columns": [{"name": "a"}, {"name": "b"}], "data": [[2, "y"]]},
    ]
    result = client.collect(pages)
    assert result.columns == ["a", "b"]
    assert result.dicts() == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]


def test_error_page_raises_with_access_denied_flag():
    error = {
        "errorName": "PERMISSION_DENIED",
        "errorType": "USER_ERROR",
        "message": "Access Denied: Cannot insert into table iceberg.gold.dim_channel",
    }
    with pytest.raises(client.TrinoQueryError) as info:
        client.collect([{"columns": []}, {"error": error}])
    assert info.value.access_denied and info.value.error_name == "PERMISSION_DENIED"
    other = client.TrinoQueryError({"errorName": "TABLE_NOT_FOUND", "message": "missing"})
    assert not other.access_denied
