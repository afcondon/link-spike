# link-spike

Minimal Ableton Link client. Joins a Link session over Wi-Fi, fires OSC `/cv/trig` to a sibling [`cv-router`](../cv-router) on each integer beat at 50%-of-beat duty (configurable). Pulse width derived from live tempo, so it tracks tempo changes from any peer (Ableton Live, Intellijel ML-2, AUM, etc.).

The two-binary architecture (link-spike → OSC → cv-router) is deliberate: it firewalls the GPL contamination from rusty_link and keeps cv-router permissive. See [`LICENSE-NOTICE.md`](LICENSE-NOTICE.md).

## Build & run

```
cargo build --release
./target/release/link-spike            # bus 8 (panel jack 1), 50% duty
./target/release/link-spike 12 0.05    # bus 12, 5% duty (short trigger)
```

Requires `cv-router` running and listening on UDP `127.0.0.1:57120`.

## Sibling repos

- [`cv-router`](../cv-router) — receives OSC, drives the ES-9
- [`purerl-tidal`](../purescript-ports/purerl-tidal) — TidalCycles port (also Link-aware in future)

## License

GPLv2-or-later, inherited via `rusty_link`'s FFI to Ableton Link. See `LICENSE` and `LICENSE-NOTICE.md`.
