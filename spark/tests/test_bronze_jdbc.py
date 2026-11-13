from unittest.mock import Mock

import pytest

from spark.lib.bronze_jdbc import rollback_close


def test_close_even_when_rollback_loses_connection():
    conn = Mock()
    conn.isClosed.return_value = False
    conn.rollback.side_effect = ConnectionError("connection lost")
    with pytest.raises(ConnectionError):
        rollback_close(conn)
    conn.close.assert_called_once()


def test_closed_connection_does_not_mask_original_failure():
    conn = Mock()
    conn.isClosed.return_value = True
    rollback_close(conn)
    conn.rollback.assert_not_called()
    conn.close.assert_called_once()
