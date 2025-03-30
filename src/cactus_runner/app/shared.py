from aiohttp import web
from cactus_test_definitions import TestProcedures

from cactus_runner.models import Aggregator, RunnerState

# aiohttp AppKeys are used to share global state between request handlers
APPKEY_TEST_PROCEDURES = web.AppKey("test-procedures", TestProcedures)
APPKEY_RUNNER_STATE = web.AppKey("runner-state", RunnerState)
APPKEY_AGGREGATOR = web.AppKey("aggregator", Aggregator)
