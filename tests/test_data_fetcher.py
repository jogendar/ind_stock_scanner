import math
import unittest
from unittest.mock import Mock, patch

from data_fetcher import (
    SCREENER_FAILURE_LIMIT,
    SCREENER_TIMEOUT_SECONDS,
    _reset_screener_circuit_breaker,
    fetch_data_from_screener,
)


class ScreenerCircuitBreakerTests(unittest.TestCase):
    def setUp(self) -> None:
        _reset_screener_circuit_breaker()

    def tearDown(self) -> None:
        _reset_screener_circuit_breaker()

    def test_repeated_timeouts_open_circuit_and_skip_more_requests(self) -> None:
        session = Mock()
        session.get.side_effect = TimeoutError("test timeout")

        with patch("builtins.print"):
            results = [
                fetch_data_from_screener("TEST.NS", session)
                for _ in range(SCREENER_FAILURE_LIMIT + 2)
            ]

        self.assertEqual(session.get.call_count, SCREENER_FAILURE_LIMIT)
        self.assertEqual(
            session.get.call_args_list[0].kwargs["timeout"],
            SCREENER_TIMEOUT_SECONDS,
        )
        self.assertTrue(all(math.isnan(item["promoter_holding"]) for item in results))

    def test_non_retryable_http_errors_do_not_open_circuit(self) -> None:
        response = Mock(status_code=404)
        response.raise_for_status.side_effect = RuntimeError("not found")
        session = Mock()
        session.get.return_value = response

        with patch("builtins.print"):
            for _ in range(SCREENER_FAILURE_LIMIT + 2):
                fetch_data_from_screener("MISSING.NS", session)

        self.assertEqual(session.get.call_count, SCREENER_FAILURE_LIMIT + 2)


if __name__ == "__main__":
    unittest.main()
