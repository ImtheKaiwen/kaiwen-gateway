"""Testing utilities and reusable conformance harnesses for Kaiwen Gateway.

External adapter authors and test suites can import these utilities to verify
provider adapters against the canonical Kaiwen Gateway port specifications.
"""

from __future__ import annotations

from kaiwen_gateway.testing.conformance import (
    RealtimeAdapterConformanceSuite,
    SyntheticUpstreamFailingAIAdapter,
    create_test_frame,
)

__all__ = [
    "RealtimeAdapterConformanceSuite",
    "SyntheticUpstreamFailingAIAdapter",
    "create_test_frame",
]
