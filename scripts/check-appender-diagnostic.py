#!/usr/bin/env python3
"""Accept only the complete, known 2.0 continuation-after-error diagnostic."""
import pathlib
import re
import sys

name = "TestDuckDBAppenderRetainsAutomaticFlushFailure"
command = (
    "go test -tags duckdb_driver_diagnostics -v -count=1 -p 1 ./server "
    f"-run '^{name}$'\n"
)
expected = (
    re.escape(command + f"=== RUN   {name}\n")
    + r"    duckdb_appender_contract_test\.go:\d+: "
    + re.escape("Close silently accepted previous automatic flush failure (1 AppendRow errors)\n")
    + r"    duckdb_appender_contract_test\.go:\d+: "
    + re.escape("AppendRow failures=1, Close error=<nil>\n")
    + re.escape(f"--- FAIL: {name} (")
    + r"\d+\.\d+s\)\nFAIL\n"
    + re.escape("FAIL\tgithub.com/posthog/duckgres/server\t")
    + r"\d+\.\d+s\nFAIL\n"
    + r"error: recipe `test-appender-contract` failed on line \d+ with exit code 1\n"
)
output = pathlib.Path(sys.argv[1]).read_text()
if not re.fullmatch(expected, output):
    sys.exit("Unexpected driver diagnostic output; refusing the known-failure exception")
print("Confirmed exact known continuation-after-error failure; no additional output")
