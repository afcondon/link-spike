// link-spike: minimal Ableton Link client.
//
// Existing role: looks ahead by LOOKAHEAD_MS into the Link timeline, schedules
// each upcoming integer beat as a sample-accurate /cv/trig/at to cv-router
// on UDP 127.0.0.1:57120 (cv-router fires the gate on the matching audio
// frame inside its callback, eliminating audio-buffer jitter).
//
// New role (Phase 1A measurement spike): also emits a MIDI Note On/Off pair
// via CoreMIDI to a named virtual destination at each integer beat. The
// CoreMIDI packet is timestamped at the beat's mach-absolute-time so the
// kernel delivers it sample-accurately — no userspace sleep_until jitter.
// Used to measure Link → MIDI jitter against a drum-machine's own
// internally-sequenced track on a different audio channel.
//
// Usage: link-spike [bus] [duty]
//   bus    cpal output bus, default 8 (= ES-9 panel jack 1 on Andrew's rig)
//   duty   fraction of beat the gate is on, default 0.5
//
// MIDI sink is hardcoded for the spike (Patterning 3, ch 1, note 62, vel 100,
// 80ms gate). Edit the constants below to retarget.

use coremidi::{Client, Destinations, OutputPort, PacketBuffer};
use mach2::mach_time::{mach_timebase_info, mach_timebase_info_data_t};
use rosc::{OscMessage, OscPacket, OscType, encoder};
use rusty_link::{AblLink, SessionState};
use std::net::UdpSocket;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const INITIAL_TEMPO: f64 = 120.0;
const QUANTUM: f64 = 4.0;
const POLL_INTERVAL: Duration = Duration::from_millis(1);
const CV_ROUTER_ADDR: &str = "127.0.0.1:57120";
const GATE_VALUE: f32 = 0.5;

// ─── Phase 2A: /link/anchor publication ──────────────────────────────────
// Anchor consumers (Erlang scheduler etc.) listen here. Each message carries
// the affine map (unix_micros_at_anchor, beat_at_anchor, tempo, quantum) so
// the consumer can compute beat_at(local_time) using its own clock without
// host-time / mach-time translation.
const ANCHOR_TARGET: &str = "127.0.0.1:57121";
const ANCHOR_PERIOD: Duration = Duration::from_millis(100); // ~10 Hz

/// Schedule beats this far ahead in the Link timeline. Should exceed cv-router's
/// audio buffer (~8 ms) plus OSC RTT (sub-ms over loopback) and CoreMIDI's own
/// scheduling horizon, but short enough that a tempo change can't drift the
/// scheduled events past their target by more than this many ms.
const LOOKAHEAD_MS: f64 = 100.0;

// ─── MIDI sink configuration ──────────────────────────────────────────────
const MIDI_PORT_NAME: &str = "Patterning 3";
const MIDI_NOTE: u8 = 62; // C major D4 = Track 2 trigger in our Patterning map
const MIDI_CHANNEL_1IDX: u8 = 1; // 1-indexed: ch 1 = status nibble 0x0
const MIDI_VELOCITY: u8 = 100;
const MIDI_GATE_US: u64 = 80_000;

/// Convert microseconds in Link's clock (= mach time converted via timebase)
/// to a CoreMIDI host-time timestamp (mach absolute time units).
///
/// `mach_timebase_info` gives a (numer, denom) ratio such that
/// `nanoseconds = mach_ticks * numer / denom`. So inversely,
/// `mach_ticks = micros * 1000 * denom / numer`.
/// On Intel: (1, 1) → ticks = ns. On Apple Silicon: (125, 3) → 24 ticks/μs.
fn micros_to_mach(micros: u64, tb: &mach_timebase_info_data_t) -> u64 {
    micros
        .wrapping_mul(1000)
        .wrapping_mul(tb.denom as u64)
        / (tb.numer as u64)
}

fn main() {
    let cli: Vec<String> = std::env::args().collect();
    let bus: i32 = cli.get(1).and_then(|s| s.parse().ok()).unwrap_or(8);
    let duty: f32 = cli.get(2).and_then(|s| s.parse().ok()).unwrap_or(0.5);

    println!(
        "link-spike: starting at {} BPM. CV bus {} (duty {:.2}). MIDI → {} (ch {} note {} vel {}).",
        INITIAL_TEMPO, bus, duty, MIDI_PORT_NAME, MIDI_CHANNEL_1IDX, MIDI_NOTE, MIDI_VELOCITY
    );

    // ─── OSC ──────────────────────────────────────────────────────────
    let socket = UdpSocket::bind("127.0.0.1:0").expect("failed to bind ephemeral UDP socket");
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
        socket.send_to(&packet, CV_ROUTER_ADDR).map(|_| ())
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
        socket.send_to(&packet, ANCHOR_TARGET).map(|_| ())
    };

    // ─── CoreMIDI ─────────────────────────────────────────────────────
    let mut tb = mach_timebase_info_data_t { numer: 0, denom: 0 };
    unsafe {
        mach_timebase_info(&mut tb);
    }
    println!("mach timebase: numer={} denom={}", tb.numer, tb.denom);

    let midi_client = Client::new("link-spike").expect("create CoreMIDI client");
    let midi_port: OutputPort = midi_client
        .output_port("link-spike-out")
        .expect("create output port");

    let dest = Destinations
        .into_iter()
        .find(|d| d.display_name().as_deref() == Some(MIDI_PORT_NAME));
    let midi_dest = match dest {
        Some(d) => d,
        None => {
            eprintln!(
                "error: MIDI destination '{}' not found. Available destinations:",
                MIDI_PORT_NAME
            );
            for d in Destinations.into_iter() {
                eprintln!(
                    "   - {}",
                    d.display_name().unwrap_or_else(|| "(unnamed)".into())
                );
            }
            std::process::exit(1);
        }
    };
    println!(
        "MIDI destination found: {}",
        midi_dest.display_name().unwrap_or_default()
    );

    let status_on = 0x90 | (MIDI_CHANNEL_1IDX - 1);
    let status_off = 0x80 | (MIDI_CHANNEL_1IDX - 1);

    let send_midi_at = |beat_time_micros: i64| {
        let on_ts = micros_to_mach(beat_time_micros.max(0) as u64, &tb);
        let off_ts = micros_to_mach(
            (beat_time_micros + MIDI_GATE_US as i64).max(0) as u64,
            &tb,
        );
        let on_packet = PacketBuffer::new(on_ts, &[status_on, MIDI_NOTE, MIDI_VELOCITY]);
        let off_packet = PacketBuffer::new(off_ts, &[status_off, MIDI_NOTE, 0]);
        let _ = midi_port.send(&midi_dest, &on_packet);
        let _ = midi_port.send(&midi_dest, &off_packet);
    };

    // ─── Link ─────────────────────────────────────────────────────────
    let link = AblLink::new(INITIAL_TEMPO);
    link.enable(true);
    link.enable_start_stop_sync(true);

    let mut state = SessionState::new();
    let mut last_scheduled_beat: i64 = i64::MIN;
    let mut last_peers: u64 = u64::MAX;
    let mut last_anchor_send = Instant::now() - ANCHOR_PERIOD; // fire on first iteration
    let mut last_logged_tempo: f64 = -1.0;
    let lookahead_micros = (LOOKAHEAD_MS * 1000.0) as i64;

    println!(
        "Link enabled. Quantum = {}. CV→{} MIDI→{} Anchor→{}.",
        QUANTUM, CV_ROUTER_ADDR, MIDI_PORT_NAME, ANCHOR_TARGET
    );
    println!("(start something else on the network — Live, ML-2, link_hut — to see peers > 0)");
    println!();

    loop {
        link.capture_app_session_state(&mut state);
        let now_micros = link.clock_micros();

        let peers = link.num_peers();
        if peers != last_peers {
            println!("[peers] {} peer(s) on session", peers);
            last_peers = peers;
        }

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
                if let Err(e) = send_gate_at(bus, dur_ms, delay_ms) {
                    eprintln!("OSC send failed: {}", e);
                }
                send_midi_at(beat_time_micros);
                let in_bar = next_beat.rem_euclid(QUANTUM as i64) + 1;
                println!(
                    "scheduled beat {:>6}  bar-pos {}/{:.0}  bpm {:6.2}  +{:.1}ms (cv-dur {:.1}ms)",
                    next_beat, in_bar, QUANTUM, bpm, delay_ms, dur_ms
                );
            }
            last_scheduled_beat = next_beat;
        }

        // ─── Anchor publication (~10 Hz) ──────────────────────────────
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
            // Log tempo changes only (keep stdout quiet most of the time)
            if (tempo - last_logged_tempo).abs() > 0.01 {
                println!("[anchor] tempo {:.3} bpm  beat {:.3}", tempo, beat_now);
                last_logged_tempo = tempo;
            }
            last_anchor_send = now_instant;
        }

        thread::sleep(POLL_INTERVAL);
    }
}
