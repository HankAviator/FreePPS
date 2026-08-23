# Changelog

## v1.8.0 — 2026-08-23

- Improve Qualcomm automatic protocol detection during charger and power-bank swaps.
- Preserve Xiaomi protocol priority for native-capable chargers.
- Reset the public-PPS retry budget for each new upstream charger session.
- Bound automatic public-PPS retries and recover cleanly from failed negotiations.
- Ignore transient reconnect states and require confirmed USB-offline evidence before treating a change as a charger swap.
- Arm swap detection for native Xiaomi sessions as well as public-PPS sessions.

## v1.7.0

- Added automatic Xiaomi/public PPS protocol selection on Qualcomm devices.
- Moved monitoring paths to event-driven wakeups where supported.
