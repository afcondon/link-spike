# License Notice

This program (`link-spike`) is licensed under the **GNU General Public License,
version 2 or any later version (GPLv2-or-later, SPDX: `GPL-2.0-or-later`)**.

The full GPLv2 text is in `LICENSE`.

## Why GPL?

`link-spike` depends on the [`rusty_link`](https://crates.io/crates/rusty_link)
Rust crate, which provides FFI bindings to Ableton's official Link C++ library.
Ableton Link is dual-licensed under GPLv2-or-later and a separate commercial
license, so any program statically linked against it must comply with one of
those terms. We choose GPLv2-or-later.

This applies only to `link-spike` itself, not to programs that communicate with
it over the network protocol. `cv-router` (MIT-licensed sibling) receives OSC
messages from `link-spike` but is not statically linked against it, so its
license is unaffected.

Copyright (c) 2026 Andrew Condon.
