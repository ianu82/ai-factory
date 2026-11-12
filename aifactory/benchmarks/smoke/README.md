# Offline eval smoke suite

The canonical suite and UTF-8 fixtures are package data in
`src/aifactory/eval_data/smoke/`, loaded with
`aifactory.benchmarks.load_eval_suite("smoke")` using importlib.resources.
This keeps installed wheels and deployment images independent of a source
checkout. No network, scorer service, credentials or delivery records are needed.
The starting implementation deliberately fails; an isolated worker must implement
addition and provide ordered command/exit-code evidence to checks-v1.

This fixture format is separate from the delivered-task export format in
`benchmarks/example/`. No real repository or PR is involved.
