# Client CSIP-AUS Test Harness

## Test Procedures

The test procedure definition/config file is located in `config/test_procedures.yaml`.

## Logging

Logging is configured in the `config/logging/config.json` file.

In the current configurtion, debug and info messages are written to `stdout`. Warning and error messages are written to `stderr`.

A persistent log is written to `logs/test_harness.jsonl`. All messages to the persistent log are written in a structured [JSONL](https://jsonlines.org/) format for easy (machine) searching/parsing.
