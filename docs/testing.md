# Test environments

The Git checkout is the authoritative environment for the full controller suite.
GitHub CI runs the controller tests there; the separate sandbox job enables the
real Docker integration checks with a digest-pinned test image.

Product agents receive source exports with Git metadata removed. Two controller
integration tests require that metadata: tracked-path inventory and recording the
controller runtime SHA before a failed target fetch. They explicitly report this
missing capability as a skip in source-only exports. Their assertions remain
unchanged and run in the normal Git checkout and CI.

Docker-dependent tests similarly require the explicit pinned-image environment.
A source-only run must report its skips; it does not establish full controller or
Docker validation. Release verification runs the complete suite in a Git checkout
with real Docker enabled and records the resulting test and skip counts.
