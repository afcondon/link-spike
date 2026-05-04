// link-spike: rig-wide Link bridge + MIDI dispatcher.
//
// Responsibilities:
//
// 1. Link timeline tracking. Joins an Ableton Link session via rusty_link
//    and is the rig's authoritative beat clock.
//
// 2. (Opt-in, --debug-beat.) Per-beat CV gate marker for calibration
//    runs. On each integer beat, fires /cv/trig/at <bus> <value>
//    <duration_ms> <delay_ms> to cv-router on UDP 127.0.0.1:57120.
//    Originally Phase 1B test scaffolding for proving Link → cv-router
//    → ES-9 timing was solid; off by default in normal sessions because
//    it gates panel jack 1 every beat regardless of any registered
//    pattern, which gates whatever is patched downstream. Pair with the
//    matching per-beat stdout log gated behind the same flag.
//
// 3. /link/anchor publication. ~10 Hz on UDP 127.0.0.1:57121, carrying
//    the affine map (unix_micros_at_anchor, beat_at_anchor, tempo,
//    quantum). Downstream schedulers (purerl-tidal etc.) compute
//    beat-at-local-time without participating in Link multicast
//    themselves.
//
// 4. CoreMIDI dispatch. Listens on UDP 127.0.0.1:57122 for OSC messages
//    /midi/note/at and /midi/cc/at, resolves a destination by name with
//    a per-process cache, schedules kernel-timestamped CoreMIDI delivery.
//    Replaces purerl-tidal's previous os:cmd sendmidi spawn-per-event
//    path (which exhibited 28+ ms of subprocess jitter) with a
//    consistent ~2.6 ms jitter floor.
//
// 5. Optional test-mode beat marker (--test-midi). Emits Note 62 ch 1
//    to "Patterning 3" on every integer beat — the original Phase 1B
//    measurement target. Off by default since it pollutes Patterning
//    Track 2 in any normal session.
//
// OSC message formats this binary RECEIVES (on UDP 57122):
//
//   /midi/note/at  ,siiiih  port_name(s)  channel(i, 1-16)  note(i, 0-127)
//                          velocity(i, 0-127)  duration_ms(i)  unix_micros_at(h)
//
//   /midi/cc/at    ,siiih   port_name(s)  channel(i, 1-16)  cc(i, 0-127)
//                          value(i, 0-127)  unix_micros_at(h)
//
// Usage: link-spike [--debug-beat] [--test-midi] [bus] [duty]
//   --debug-beat  enable per-beat /cv/trig/at + stdout log (off by default)
//   --test-midi   enable per-beat MIDI note 62 → "Patterning 3"
//   bus           OSC bus for the --debug-beat CV gate, default 8 (= ES-9 panel jack 1)
//   duty          fraction of beat the CV gate is on, default 0.5

use coremidi::{Client, Destination, Destinations, OutputPort, PacketBuffer};
use mach2::mach_time::{mach_absolute_time, mach_timebase_info, mach_timebase_info_data_t};
use rosc::{OscMessage, OscPacket, OscType, decoder, encoder};
use rusty_link::{AblLink, SessionState};
use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::UdpSocket;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const INITIAL_TEMPO: f64 = 120.0;
const QUANTUM: f64 = 4.0;
const POLL_INTERVAL: Duration = Duration::from_millis(1);
const CV_ROUTER_ADDR: &str = "127.0.0.1:57120";
const ANCHOR_TARGET: &str = "127.0.0.1:57121";
const MIDI_RX_ADDR: &str = "127.0.0.1:57122";
const ANCHOR_PERIOD: Duration = Duration::from_millis(100);
const GATE_VALUE: f32 = 0.5;

/// Schedule beats this far ahead in the Link timeline. Should exceed
/// cv-router's audio buffer (~8 ms) plus OSC RTT (sub-ms over loopback).
const LOOKAHEAD_MS: f64 = 100.0;

// Test-mode (--test-midi) beat-marker constants — Phase 1B legacy.
const TEST_MIDI_PORT: &str = "Patterning 3";
const TEST_MIDI_NOTE: u8 = 62;
const TEST_MIDI_CHANNEL_1IDX: u8 = 1;
const TEST_MIDI_VELOCITY: u8 = 100;
const TEST_MIDI_GATE_US: u64 = 80_000;

/// Convert a count of microseconds-of-elapsed-real-time into mach ticks.
///
/// `mach_timebase_info` gives (numer, denom) such that
/// `nanoseconds = mach_ticks * numer / denom`. Inversely:
/// `mach_ticks = micros * 1000 * denom / numer`.
/// On Intel: (1, 1) → ticks = ns. On Apple Silicon: (125, 3) → 24 ticks/μs.
///
/// CRUCIAL: this converts a DURATION, not an absolute time. Mach absolute
/// time is "ticks since boot"; Unix microseconds is "since 1970". They
/// share the same RATE but have different epochs — bridging the two
/// requires sampling both clocks "now" and using the difference to
/// translate. See `unix_micros_to_mach`.
fn duration_micros_to_mach(micros: u64, tb: &mach_timebase_info_data_t) -> u64 {
    micros
        .wrapping_mul(1000)
        .wrapping_mul(tb.denom as u64)
        / (tb.numer as u64)
}

/// Convert an absolute Unix-microseconds timestamp into a mach-absolute-time
/// host-time value suitable for CoreMIDI scheduling.
///
/// Unix epoch is 1970; mach epoch is "this boot." We bridge by sampling
/// both clocks now and computing the future timestamp as
/// `mach_now + (unix_at − unix_now)` converted to mach ticks. If the
/// requested time is in the past, returns mach_now (= "fire ASAP").
fn unix_micros_to_mach(unix_micros_at: i64, tb: &mach_timebase_info_data_t) -> u64 {
    let mach_now = unsafe { mach_absolute_time() };
    let unix_now_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64;
    let delta_us = unix_micros_at - unix_now_us;
    if delta_us <= 0 {
        return mach_now;
    }
    let delta_mach = duration_micros_to_mach(delta_us as u64, tb);
    mach_now.wrapping_add(delta_mach)
}

/// Owns the CoreMIDI output port plus a destination-name cache. Lookup is
/// O(1) after the first send to a given port name; first send pays one
/// linear scan of `Destinations` and inserts the result.
struct MidiDispatch {
    port: OutputPort,
    cache: HashMap<String, Destination>,
    tb: mach_timebase_info_data_t,
}

impl MidiDispatch {
    fn new(client: &Client, tb: mach_timebase_info_data_t) -> Self {
        let port = client
            .output_port("link-spike-out")
            .expect("create CoreMIDI output port");
        Self { port, cache: HashMap::new(), tb }
    }

    fn resolve(&mut self, name: &str) -> Option<Destination> {
        if let Some(d) = self.cache.get(name) {
            return Some(d.clone());
        }
        let dest = Destinations
            .into_iter()
            .find(|d| d.display_name().as_deref() == Some(name))?;
        self.cache.insert(name.to_string(), dest.clone());
        Some(dest)
    }

    fn schedule_note(
        &mut self,
        port_name: &str,
        channel_1idx: u8,
        note: u8,
        velocity: u8,
        duration_ms: u32,
        unix_micros_at: i64,
    ) {
        let Some(dest) = self.resolve(port_name) else {
            eprintln!("midi/note/at: unknown destination '{}'", port_name);
            return;
        };
        let on_ts = unix_micros_to_mach(unix_micros_at, &self.tb);
        let off_us = unix_micros_at.saturating_add((duration_ms as i64) * 1000);
        let off_ts = unix_micros_to_mach(off_us, &self.tb);
        let ch = channel_1idx.saturating_sub(1) & 0x0F;
        let on_pkt = PacketBuffer::new(on_ts, &[0x90 | ch, note & 0x7F, velocity & 0x7F]);
        let off_pkt = PacketBuffer::new(off_ts, &[0x80 | ch, note & 0x7F, 0]);
        let _ = self.port.send(&dest, &on_pkt);
        let _ = self.port.send(&dest, &off_pkt);
    }

    fn schedule_cc(
        &mut self,
        port_name: &str,
        channel_1idx: u8,
        cc: u8,
        value: u8,
        unix_micros_at: i64,
    ) {
        let Some(dest) = self.resolve(port_name) else {
            eprintln!("midi/cc/at: unknown destination '{}'", port_name);
            return;
        };
        let ts = unix_micros_to_mach(unix_micros_at, &self.tb);
        let ch = channel_1idx.saturating_sub(1) & 0x0F;
        let pkt = PacketBuffer::new(ts, &[0xB0 | ch, cc & 0x7F, value & 0x7F]);
        let _ = self.port.send(&dest, &pkt);
    }
}

// Debug helper: descriptive single-line summary of an OscType variant.
fn osc_type_name(t: &OscType) -> &'static str {
    match t {
        OscType::Int(_) => "Int",
        OscType::Float(_) => "Float",
        OscType::String(_) => "String",
        OscType::Long(_) => "Long",
        OscType::Double(_) => "Double",
        OscType::Blob(_) => "Blob",
        OscType::Time(_) => "Time",
        OscType::Char(_) => "Char",
        OscType::Color(_) => "Color",
        OscType::Midi(_) => "Midi",
        OscType::Bool(_) => "Bool",
        OscType::Array(_) => "Array",
        OscType::Nil => "Nil",
        OscType::Inf => "Inf",
    }
}

/// Decode a single OSC message into a MidiDispatch call. Bundles are
/// flattened by the caller before reaching here.
fn handle_osc_message(msg: &OscMessage, dispatch: &mut MidiDispatch) {
    match msg.addr.as_str() {
        "/midi/note/at" => {
            if msg.args.len() != 6 {
                eprintln!("/midi/note/at: expected 6 args, got {}", msg.args.len());
                return;
            }
            let port = match &msg.args[0] {
                OscType::String(s) => s,
                t => { eprintln!("/midi/note/at[0] String expected, got {}", osc_type_name(t)); return; }
            };
            let channel = match &msg.args[1] {
                OscType::Int(i) => *i as u8,
                t => { eprintln!("/midi/note/at[1] Int expected, got {}", osc_type_name(t)); return; }
            };
            let note = match &msg.args[2] {
                OscType::Int(i) => *i as u8,
                t => { eprintln!("/midi/note/at[2] Int expected, got {}", osc_type_name(t)); return; }
            };
            let velocity = match &msg.args[3] {
                OscType::Int(i) => *i as u8,
                t => { eprintln!("/midi/note/at[3] Int expected, got {}", osc_type_name(t)); return; }
            };
            let duration_ms = match &msg.args[4] {
                OscType::Int(i) => *i as u32,
                t => { eprintln!("/midi/note/at[4] Int expected, got {}", osc_type_name(t)); return; }
            };
            let unix_us = match &msg.args[5] {
                OscType::Long(l) => *l,
                t => { eprintln!("/midi/note/at[5] Long expected, got {}", osc_type_name(t)); return; }
            };
            dispatch.schedule_note(port, channel, note, velocity, duration_ms, unix_us);
        }
        "/midi/cc/at" => {
            if msg.args.len() != 5 {
                eprintln!("/midi/cc/at: expected 5 args, got {}", msg.args.len());
                return;
            }
            let port = match &msg.args[0] {
                OscType::String(s) => s,
                t => { eprintln!("/midi/cc/at[0] String expected, got {}", osc_type_name(t)); return; }
            };
            let channel = match &msg.args[1] {
                OscType::Int(i) => *i as u8,
                t => { eprintln!("/midi/cc/at[1] Int expected, got {}", osc_type_name(t)); return; }
            };
            let cc = match &msg.args[2] {
                OscType::Int(i) => *i as u8,
                t => { eprintln!("/midi/cc/at[2] Int expected, got {}", osc_type_name(t)); return; }
            };
            let value = match &msg.args[3] {
                OscType::Int(i) => *i as u8,
                t => { eprintln!("/midi/cc/at[3] Int expected, got {}", osc_type_name(t)); return; }
            };
            let unix_us = match &msg.args[4] {
                OscType::Long(l) => *l,
                t => { eprintln!("/midi/cc/at[4] Long expected, got {}", osc_type_name(t)); return; }
            };
            dispatch.schedule_cc(port, channel, cc, value, unix_us);
        }
        // Unknown addresses silently ignored — OSC-spec compliant, and
        // keeps stdout clean if other consumers/senders share the bus.
        _ => {}
    }
}

fn handle_osc_packet(packet: &OscPacket, dispatch: &mut MidiDispatch) {
    match packet {
        OscPacket::Message(msg) => handle_osc_message(msg, dispatch),
        OscPacket::Bundle(bundle) => {
            for inner in &bundle.content {
                handle_osc_packet(inner, dispatch);
            }
        }
    }
}

fn main() {
    // ─── CLI parsing — flags first, then positional ───────────────────
    let raw_args: Vec<String> = std::env::args().skip(1).collect();
    let test_midi = raw_args.iter().any(|a| a == "--test-midi");
    let debug_beat = raw_args.iter().any(|a| a == "--debug-beat");
    let positional: Vec<&String> = raw_args.iter().filter(|a| !a.starts_with("--")).collect();
    let bus: i32 = positional.first().and_then(|s| s.parse().ok()).unwrap_or(8);
    let duty: f32 = positional.get(1).and_then(|s| s.parse().ok()).unwrap_or(0.5);

    println!(
        "link-spike: starting at {} BPM. MIDI dispatch on UDP {}{}{}",
        INITIAL_TEMPO,
        MIDI_RX_ADDR,
        if debug_beat {
            format!(". --debug-beat enabled (per-beat /cv/trig/at on bus {}, duty {:.2})", bus, duty)
        } else {
            String::new()
        },
        if test_midi { ". --test-midi enabled (per-beat note 62 → Patterning 3)" } else { "" }
    );

    // ─── Outbound OSC for cv-router and anchor ────────────────────────
    let osc_out = UdpSocket::bind("127.0.0.1:0").expect("bind ephemeral UDP socket");
    let send_gate_at = |b: i32, dur_ms: f32, delay_ms: f32| -> std::io::Result<()> {
        let msg = OscPacket::Message(OscMessage {
            addr: "/cv/trig/at".into(),
            args: vec![
                OscType::Int(b),
                OscType::Float(GATE_VALUE),
                OscType::Float(dur_ms),
                OscType::Float(delay_ms),
            ],
        });
        let packet = encoder::encode(&msg).expect("OSC encode failed");
        osc_out.send_to(&packet, CV_ROUTER_ADDR).map(|_| ())
    };

    let send_anchor = |unix_micros: i64, beat: f64, tempo: f64| -> std::io::Result<()> {
        let msg = OscPacket::Message(OscMessage {
            addr: "/link/anchor".into(),
            args: vec![
                OscType::Long(unix_micros),
                OscType::Double(beat),
                OscType::Double(tempo),
                OscType::Double(QUANTUM),
            ],
        });
        let packet = encoder::encode(&msg).expect("OSC encode failed");
        osc_out.send_to(&packet, ANCHOR_TARGET).map(|_| ())
    };

    // ─── Inbound OSC for MIDI dispatch (non-blocking) ─────────────────
    let midi_rx = UdpSocket::bind(MIDI_RX_ADDR).expect("bind MIDI dispatch socket");
    midi_rx
        .set_nonblocking(true)
        .expect("set MIDI dispatch socket non-blocking");
    println!("MIDI dispatch listener on UDP {}", MIDI_RX_ADDR);

    // ─── CoreMIDI ─────────────────────────────────────────────────────
    let mut tb = mach_timebase_info_data_t { numer: 0, denom: 0 };
    unsafe {
        mach_timebase_info(&mut tb);
    }
    println!("mach timebase: numer={} denom={}", tb.numer, tb.denom);

    let midi_client = Client::new("link-spike").expect("create CoreMIDI client");
    let mut dispatch = MidiDispatch::new(&midi_client, tb);

    // Pre-warm the test destination so any "destination not found" surfaces
    // at startup rather than at the first beat.
    if test_midi {
        match dispatch.resolve(TEST_MIDI_PORT) {
            Some(_) => println!("test-midi destination found: {}", TEST_MIDI_PORT),
            None => {
                eprintln!(
                    "warning: --test-midi requested but '{}' not found. Available:",
                    TEST_MIDI_PORT
                );
                for d in Destinations.into_iter() {
                    eprintln!(
                        "   - {}",
                        d.display_name().unwrap_or_else(|| "(unnamed)".into())
                    );
                }
            }
        }
    }

    // ─── Link ─────────────────────────────────────────────────────────
    let link = AblLink::new(INITIAL_TEMPO);
    link.enable(true);
    link.enable_start_stop_sync(true);

    let mut state = SessionState::new();
    let mut last_scheduled_beat: i64 = i64::MIN;
    let mut last_peers: u64 = u64::MAX;
    let mut last_anchor_send = Instant::now() - ANCHOR_PERIOD;
    let mut last_logged_tempo: f64 = -1.0;
    let lookahead_micros = (LOOKAHEAD_MS * 1000.0) as i64;

    println!(
        "Link enabled. Quantum = {}. CV→{}  Anchor→{}  MIDI rx←{}",
        QUANTUM, CV_ROUTER_ADDR, ANCHOR_TARGET, MIDI_RX_ADDR
    );
    println!("(start something else on the network — Live, ML-2, link_hut — to see peers > 0)");
    println!();

    // Reusable receive buffer for the inbound socket. 4 KB is plenty for
    // any reasonable OSC packet — the typical /midi/note/at message is
    // <80 bytes including padding.
    let mut rx_buf = [0u8; 4096];

    loop {
        link.capture_app_session_state(&mut state);
        let now_micros = link.clock_micros();

        let peers = link.num_peers();
        if peers != last_peers {
            println!("[peers] {} peer(s) on session", peers);
            last_peers = peers;
        }

        // ─── 1. Drain inbound MIDI dispatch socket ────────────────────
        // Loop until WouldBlock so a burst of packets all get handled in
        // one tick (purerl-tidal's scheduler can fire many events per
        // tick on dense patterns / CC streams).
        loop {
            match midi_rx.recv_from(&mut rx_buf) {
                Ok((n, _src)) => match decoder::decode_udp(&rx_buf[..n]) {
                    Ok((_, packet)) => handle_osc_packet(&packet, &mut dispatch),
                    Err(e) => eprintln!("midi-rx: OSC decode failed: {:?}", e),
                },
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) => {
                    eprintln!("midi-rx: recv error: {}", e);
                    break;
                }
            }
        }

        // ─── 2. CV/Gate scheduling for upcoming integer beats ─────────
        let beat_at_lookahead = state.beat_at_time(now_micros + lookahead_micros, QUANTUM);
        let last_beat_in_lookahead = beat_at_lookahead.floor() as i64;

        if last_scheduled_beat == i64::MIN {
            let beat_now = state.beat_at_time(now_micros, QUANTUM);
            last_scheduled_beat = beat_now.floor() as i64;
        }

        while last_scheduled_beat < last_beat_in_lookahead {
            let next_beat = last_scheduled_beat + 1;
            let beat_time_micros = state.time_at_beat(next_beat as f64, QUANTUM);
            let delay_micros = beat_time_micros - now_micros;
            if delay_micros >= 0 {
                let bpm = state.tempo();
                let dur_ms = (60_000.0 / bpm as f32) * duty;
                let delay_ms = (delay_micros as f64 / 1000.0) as f32;
                if debug_beat {
                    if let Err(e) = send_gate_at(bus, dur_ms, delay_ms) {
                        eprintln!("OSC send failed: {}", e);
                    }
                }
                if test_midi {
                    // Translate Link-clock microseconds (= mach time
                    // expressed in μs) into Unix microseconds, since
                    // schedule_note now bridges Unix → mach internally.
                    let unix_now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_micros() as i64;
                    let unix_at = unix_now + (beat_time_micros - now_micros);
                    dispatch.schedule_note(
                        TEST_MIDI_PORT,
                        TEST_MIDI_CHANNEL_1IDX,
                        TEST_MIDI_NOTE,
                        TEST_MIDI_VELOCITY,
                        (TEST_MIDI_GATE_US / 1_000) as u32,
                        unix_at,
                    );
                }
                if debug_beat {
                    let in_bar = next_beat.rem_euclid(QUANTUM as i64) + 1;
                    println!(
                        "scheduled beat {:>6}  bar-pos {}/{:.0}  bpm {:6.2}  +{:.1}ms (cv-dur {:.1}ms)",
                        next_beat, in_bar, QUANTUM, bpm, delay_ms, dur_ms
                    );
                }
            }
            last_scheduled_beat = next_beat;
        }

        // ─── 3. Anchor publication (~10 Hz) ───────────────────────────
        let now_instant = Instant::now();
        if now_instant.duration_since(last_anchor_send) >= ANCHOR_PERIOD {
            let unix_micros = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as i64;
            let beat_now = state.beat_at_time(now_micros, QUANTUM);
            let tempo = state.tempo();
            if let Err(e) = send_anchor(unix_micros, beat_now, tempo) {
                eprintln!("anchor send failed: {}", e);
            }
            if (tempo - last_logged_tempo).abs() > 0.01 {
                println!("[anchor] tempo {:.3} bpm  beat {:.3}", tempo, beat_now);
                last_logged_tempo = tempo;
            }
            last_anchor_send = now_instant;
        }

        thread::sleep(POLL_INTERVAL);
    }
}
