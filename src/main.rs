// link-spike: minimal Ableton Link client for the purerl-tidal CV path.
//
// On each integer Link beat, fire a gate pulse on the configured cpal bus by
// sending OSC /cv/trig to cv-router on UDP 127.0.0.1:57120. Pulse width is
// derived from the live Link tempo on each beat: dur_ms = 60000 / bpm * duty.
//
// Usage: link-spike [bus] [duty]
//   bus    cpal output bus, default 8 (= ES-9 panel jack 1 on Andrew's rig)
//   duty   fraction of beat the gate is on, default 0.5 (50% square)
//          0.5 produces a square wave matching ML-2's 50%-duty clock-out.
//          0.05 ish gives a short trigger if you want one.

use rosc::{OscMessage, OscPacket, OscType, encoder};
use rusty_link::{AblLink, SessionState};
use std::net::UdpSocket;
use std::thread;
use std::time::Duration;

const INITIAL_TEMPO: f64 = 120.0;
const QUANTUM: f64 = 4.0;
const POLL_INTERVAL: Duration = Duration::from_millis(1);
const CV_ROUTER_ADDR: &str = "127.0.0.1:57120";
const GATE_VALUE: f32 = 0.5; // matches cv-router's SAFETY_SCALE → ~+5V at ES-9 output

fn main() {
    let cli: Vec<String> = std::env::args().collect();
    let bus: i32 = cli.get(1).and_then(|s| s.parse().ok()).unwrap_or(8);
    let duty: f32 = cli.get(2).and_then(|s| s.parse().ok()).unwrap_or(0.5);

    println!("link-spike: starting at {} BPM, gating cpal bus {} (duty {:.2})",
             INITIAL_TEMPO, bus, duty);

    let socket = UdpSocket::bind("127.0.0.1:0")
        .expect("failed to bind ephemeral UDP socket");
    let send_gate = |b: i32, dur: f32| -> std::io::Result<()> {
        let msg = OscPacket::Message(OscMessage {
            addr: "/cv/trig".into(),
            args: vec![
                OscType::Int(b),
                OscType::Float(GATE_VALUE),
                OscType::Float(dur),
            ],
        });
        let packet = encoder::encode(&msg).expect("OSC encode failed");
        socket.send_to(&packet, CV_ROUTER_ADDR).map(|_| ())
    };

    let link = AblLink::new(INITIAL_TEMPO);
    link.enable(true);
    link.enable_start_stop_sync(true);

    let mut state = SessionState::new();
    let mut last_beat: i64 = i64::MIN;
    let mut last_peers: u64 = u64::MAX;

    println!("Link enabled. Polling for beats. Quantum = {}.", QUANTUM);
    println!("(start something else on the network — Live, ML-2, link_hut — to see peers > 0)");
    println!("OSC target: {}", CV_ROUTER_ADDR);
    println!();

    loop {
        link.capture_app_session_state(&mut state);
        let now = link.clock_micros();
        let beat = state.beat_at_time(now, QUANTUM);
        let int_beat = beat.floor() as i64;

        let peers = link.num_peers();
        if peers != last_peers {
            println!("[peers] {} peer(s) on session", peers);
            last_peers = peers;
        }

        if int_beat != last_beat && last_beat != i64::MIN {
            let bpm = state.tempo();
            let phase = state.phase_at_time(now, QUANTUM);
            let in_bar = int_beat.rem_euclid(QUANTUM as i64) + 1;
            let dur_ms = (60_000.0 / bpm as f32) * duty;
            if let Err(e) = send_gate(bus, dur_ms) {
                eprintln!("OSC send failed: {}", e);
            }
            println!(
                "beat {:>6}  bar-pos {}/{:.0}  bpm {:6.2}  phase {:5.2}  peers {}  → /cv/trig {} {} {:.1}ms",
                int_beat, in_bar, QUANTUM, bpm, phase, peers, bus, GATE_VALUE, dur_ms
            );
        }
        if int_beat != last_beat {
            last_beat = int_beat;
        }

        thread::sleep(POLL_INTERVAL);
    }
}
