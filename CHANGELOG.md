# Changelog

## v1.8.1 — 2026-09-18

- Fix Qualcomm Auto mode falling back from an available public-PPS contract to fixed-voltage PD.
- Enable public PPS in place after the Xiaomi-first detection window instead of simulating a cable reconnect with `input_suspend`.
- Verify both the PD verification node and negotiated USB type before reporting Auto-mode success.

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
