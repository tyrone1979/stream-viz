# StreamViz

A domain-neutral research prototype for **low-latency visualisation and
replay of continuous streaming numeric data**. It implements a multi-threaded
ingest → clean → buffer → display → replay pipeline with an interactive
browser GUI. Two interchangeable data sources are provided: a synthetic
generator for smoke tests, and a **real, openly licensed ECG recording** for
measurements that can be cited and independently reproduced.

> The repository ships **no third-party signal files** and contains only
> generic transport fields. It carries no affiliation, no personnel
> information, no application-specific protocol, and no restricted or
> proprietary material. See [Desensitisation](#desensitisation) below.

## What it does

```
UDP sender --> [S1] receiver thread --> raw source FIFO
                        |
                        v
             [S2] parser thread: validate / clean / classify
                        |                    |
              spectrum / waveform      audio samples
                        v                    v
                 display FIFO        bounded audio buffer
                        |                    |
              cache-full signal      flush -> WAV segments
                        v                    v
             [S3] timer-driven view   [S4] live playback
                                      [S5] timestamp-seek replay
```

Four rolling views are produced: a spectrogram, a per-channel energy heatmap,
a waveform, and a spectrum. End-to-end latency is metered from source
timestamp to render, with p50/p95/p99 reported.

## Quick start

```bash
python -m pip install -r requirements.txt

# self-tests (headless, no browser needed)
python -m unittest discover -s tests -v

# interactive dashboard at http://127.0.0.1:8765/
python examples/run_gui.py
python examples/run_gui.py --source mitdb   # real ECG instead of synthetic data

# one-off download of the open dataset (verified by SHA-256)
python tools/fetch_dataset.py

# REAL-DATA measurement -> output/real_benchmark/real_benchmark.json + PNGs
python examples/run_real_benchmark.py

# end-to-end synthetic demo (writes output/demo_report.json + PNGs)
python examples/run_demo.py --duration 4 --rate 4

# throughput / buffer sweep -> output/benchmark/benchmark.json
python examples/benchmark.py

# reproducible GUI figures for a manuscript
python tools/capture_gui.py                                # synthetic
python tools/capture_gui.py --source mitdb --rate 2 \
    --output output/figures/gui_dashboard_real.png       # real ECG

# pre-publication term scan
python tools/desensitize_check.py
```

`numpy` and `matplotlib` are the only third-party dependencies. The GUI and
the HTTP API use the Python standard library exclusively.

## Interactive GUI

Software-article reviewers need to *run* the tool, not just read about it, and
an illustrative example needs a real interface rather than a schematic. The
dashboard therefore exposes the production pipeline directly:

- **Choose the data source**: `Synthetic (smoke test)` or
  `MIT-BIH ECG — real open data`. The latter streams a checksum-verified
  PhysioNet recording and displays its provenance in the API state.
- **Start / stop** the stream and set target rate, display FIFO capacity,
  refresh frequency and the fraction of deliberately malformed datagrams.
- **Four live views**, redrawn on every state change from a shared
  `ViewState` — the same object the headless PNG renderer uses, so the two
  front-ends cannot disagree.
- **Live metrics**: wire throughput, received/parsed datagrams, FIFO overflow,
  archived WAV segments, rendered frames, ingest p95 and end-to-end p50/p95.
- **Audio replay**: play the newest archived segment (S6) or drag the source
  timestamp slider and play from that point (S7). The server returns a WAV
  stream built from the timestamp-indexed archive.
- **Export the current view** as a four-panel PNG, and an **event log** of
  session actions.

Run `python examples/run_gui.py` and open the printed URL. `--no-browser`
serves only (useful over SSH or in CI); `--idle` starts without a stream.

Architecture: `GuiConsumer` is injected into `StreamPipeline` through
`display_factory=`, so the GUI is a front-end on the real ingest/parser/buffer
threads. The consumer keeps a `ViewState` fresh on its own timer; the browser
only reads snapshots, never drives the pipeline timing.

| Endpoint | Method | Purpose |
| --- | --- | --- |
| `/` | GET | dashboard (HTML/CSS/JS in `webui/`) |
| `/api/state` | GET | views, metrics, audio index, event log |
| `/api/control` | POST | `{"action": "start"\|"stop"\|"figure", ...}` |
| `/api/audio` | GET | `?mode=live` or `?mode=seek&ts=<source_ms>` → `audio/wav` |
| `/api/figure` | POST | export the current four-panel view as PNG |

The server binds to `127.0.0.1` only. `tools/capture_gui.py` starts the same
server plus a short stream and screenshots it with a locally installed
headless Chrome/Edge, producing a genuine capture of the running application
for the illustrative-examples section. No browser automation package is
needed. Metrics are cached for 250 ms so a fast-polling browser cannot steal
CPU from the display consumer and distort the latency measurements.

## Package layout

| Path | Purpose |
| --- | --- |
| `src/protocol.py` | generic binary packet format + validation/cleaning rules |
| `src/buffers.py` | bounded FIFO, datagram wrapper, cache-full signal |
| `src/receiver.py` | UDP ingestion thread |
| `src/parser.py` | decode, sequence-gap accounting, routing, notification |
| `src/player.py` | WAV archiving, paced live playback, timestamp seek |
| `src/synthetic.py` | synthetic spectrum/waveform/audio generator |
| `src/open_dataset.py` | WFDB 212 decoder, MIT-BIH provenance/checksums, real-data UDP replay |
| `src/viewstate.py` | shared, renderer-agnostic four-view state + JSON encoding |
| `src/visualizer.py` | timer-driven four-view PNG renderer + renderer-free consumer |
| `src/gui.py` | GUI consumer, session control, localhost HTTP API |
| `src/pipeline.py` | lifecycle, configuration, metrics report |
| `webui/` | dashboard HTML, CSS and Canvas front-end |
| `examples/` | GUI launcher, demo, synthetic sweep, real-data benchmark |
| `tests/` | protocol / buffer / end-to-end / GUI / open-dataset tests |
| `tools/` | dataset download+verify, term scan, reproducible GUI capture |

## Outputs

- `output/demo_report.json` — throughput, counters, ingress and end-to-end
  latency percentiles, audio segment count, replay results
- `output/figures/view_*.png` — up to six rendered snapshots from the headless
  demo (cleared and rewritten on every run, produced at shutdown)
- `output/figures/gui_dashboard.png` — headless-browser capture of the GUI
- `output/figures/gui_dashboard_real.png` — same GUI running the real ECG
- `output/gui/gui_view.png` — four-panel figure exported from the GUI
- `output/gui/gui_report.json` — metrics of the GUI session
- `output/audio/segment_*.wav` — archived sample blocks with timestamp index
- `output/benchmark/benchmark.json` — synthetic sweep results
- `output/real_benchmark/real_benchmark.json` — **real-data** measurements,
  provenance, SHA-256 and payload-authenticity checks
- `output/real_benchmark/run_4mbps/figures/view_*.png` — four-view figures
  computed from the recorded ECG

## Real open-data measurements

The synthetic generator is for smoke tests only. Published performance numbers
come from a **real, openly licensed recording**:

| | |
| --- | --- |
| Dataset | MIT-BIH Arrhythmia Database, v1.0.0, record `100` (PhysioNet) |
| DOI | [10.13026/C2F305](https://doi.org/10.13026/C2F305) |
| Licence | Open Data Commons Attribution License v1.0 (ODC-By 1.0) |
| Signal | 30.09 min, two synchronous leads (MLII, V5), 360 Hz, 11-bit |
| Files | `100.hea` + `100.dat` (1,950,000 bytes), fetched by `tools/fetch_dataset.py` |
| SHA-256 (`100.dat`) | `b2ea3c250e56e48f4b7b90697832b8ecd1afa1e0bb31f2dcfea4ed6e1075a639` |

Cite: Moody GB, Mark RG. *The impact of the MIT-BIH Arrhythmia Database.*
IEEE Engineering in Medicine and Biology Magazine. 2001;20(3):45-50.

**No signal value is synthesised.** Every datagram payload is derived from the
recorded samples, and `run_real_benchmark.py` asserts this on every run:

- `WAVEFORM` = 256 consecutive recorded samples of one lead;
- `SPECTRUM` = magnitude of a Hann-windowed, DC-removed FFT of a 512-sample
  window of that same lead, scaled by a fixed record-wide reference;
- `AUDIO` = **audification** of the recorded samples (time-compressed to
  16 kHz int16). It is a rendering of the real signal, not a separate audio
  recording and not a synthesised tone.

Only zero-centring and per-lead robust amplitude normalisation are applied,
because the two leads differ in amplitude by roughly an order of magnitude.
The original ADC/mV values remain available on `RealRecord.signals_mv`.

The sender runs in a **separate OS process**, representing an external
acquisition device; a Python sender in the same process would compete with the
receiver, parser and display threads for the GIL and cap the wire rate well
below the pipeline's real capacity. Packet payloads are pre-encoded from the
recording, and only the sequence number and wall-clock timestamp are rewritten
in the pacing loop.

Reference run on a Windows laptop, 6 s per rate, display FIFO 64
(`python examples/run_real_benchmark.py`):

| Target (Mbit/s) | Measured (Mbit/s) | Parsed / received | E2E p50 / p95 (ms) | FIFO drops |
| --- | --- | --- | --- | --- |
| 1 | 1.00 | 816 / 816 | 26 / 47 | 0 |
| 2 | 2.00 | 1631 / 1631 | 27 / 49 | 0 |
| 4 | 4.00 | 3261 / 3261 | 28 / 51.5 | 0 |
| 8 | 8.00 | 6522 / 6522 | 30 / 52 | 0 |

No malformed datagrams are injected in this run, so delivery is complete. The
verified record is looped to sustain each rate; `distinct_signal_seconds` in
the report states how much of the 30-minute recording was consumed (up to the
full 1803 s at 4 and 8 Mbit/s). These are measurements of a replay workload on
one machine, **not** claims about live acquisition hardware.

## Measured behaviour (synthetic smoke test)

Reference run on a Windows laptop, 3 s of synthetic traffic at a 4 Mbit/s
target (`python examples/run_demo.py --duration 3 --rate 4`):

| Metric | Value |
| --- | --- |
| Measured wire throughput | 3.42 Mbit/s against a 4 Mbit/s target (2.9 on a busier host) |
| Parsed / received datagrams | 1744 / 1761 (rest are deliberately corrupted) |
| Ingest latency p95 | 3 ms |
| End-to-end latency p50 / p95 / p99 | 33 / 59 / 63 ms |
| Display FIFO overflow | 0 |
| WAV segments + timestamp seek | 55 segments, seek returns 3936 samples |

The sender paces itself with a coarse sleep plus a short spin, so the achieved
wire rate depends on host scheduling (other CPU-bound processes on the same
machine lower it). All numbers above come from the scripts in this repository
and are printed in full in `output/demo_report.json`; treat them as one
reference run, not a performance guarantee.

An interactive GUI session at 2–4 Mbit/s reports the same order of magnitude
(e.g. p50/p95 ≈ 30/56 ms with zero FIFO overflow in the captured real-data
figure), which is the point of sharing one `ViewState` and one metering path
between the two front-ends.

The sweep in `examples/benchmark.py` covers 1/2/4/8 Mbit/s against display
buffer capacities of 16 and 64. Delivery ratio stays at 0.94–0.98 (2% of
datagrams are intentionally malformed), and the smaller buffer gives equal or
lower p95 latency at higher rates (e.g. 29 ms vs 52 ms at 8 Mbit/s),
illustrating the batching/latency trade-off. Headless-demo figures are
rendered once at shutdown from buffered view states so image encoding never
stalls the consumer; the interactive GUI instead keeps arrays in memory and
lets the browser draw them.

## Desensitisation

`tools/sensitive_terms.txt` holds **public, domain-neutral** identifiers that
must not appear in an openly distributed repository. Organisation names,
personnel names and location identifiers are kept in
`.workbuddy/sensitive_terms.local.txt`, which is excluded from version control;
`tools/desensitize_check.py` merges both lists, scans file contents *and*
paths, and exits non-zero on a hit. Wire the check into CI before tagging a
release. A public clone therefore contains none of the private identifiers
even though a local scan still covers them.

Directories excluded from the scan are `.git`, `.workbuddy`, `output`, `data`,
`__pycache__` and virtual environments — **do not commit raw working material
or downloaded third-party signal files**, and never publish anything extracted
from them. `data/` is git-ignored: the ODC-By dataset is downloaded and
checksum-verified on demand rather than redistributed here.

The wire protocol deliberately exposes only `magic / version / type /
channel / sequence / timestamp / payload length`. All payloads are plain
numeric vectors; no acquisition geometry, platform parameters or
domain-specific units are present.

## Known limitations

- UDP is used on loopback; at high packet rates the operating system may drop
  datagrams. The benchmark reports delivery ratio and sequence gaps rather
  than hiding losses.
- Timing figures are host-clock measurements on one machine, not a
  distributed-system evaluation.
- The GUI needs a local browser. Headless machines can still run every test,
  the demo, the benchmark, the HTTP API and the Agg PNG renderer; only the
  browser screenshot step requires Chrome/Edge.
- Audio playback uses a null sink in the library API; the GUI streams WAV to
  the browser's own audio element instead.
- The real-data workload is an offline **replay** of one public ECG record,
  replayed faster than real time to reach a target bit rate. The audio channel
  is audification of that ECG, not a microphone recording. Per-lead amplitude
  normalisation is applied for display; physical units remain available.
- A single Python sender process tops out near 8 Mbit/s on the reference
  machine. Higher rates need multiple sender processes or a compiled sender.

## Before any public release

1. Replace the copyright holder in `LICENSE` with the actual rights holder
   and obtain the institution's written approval for releasing the code.
2. Complete the institution's review procedure for external publication.
3. Add a persistent repository URL, a released version tag, and a
   reproducibility capsule. Include `tools/fetch_dataset.py` in the capsule
   instructions and keep the MIT-BIH attribution/DOI in any figure caption
   derived from `gui_dashboard_real.png` or the real-benchmark figures.
4. Add at least one independent reuse case; a software-article reviewer will
   weigh demonstrated impact more heavily than the implementation itself.

## License

MIT — see `LICENSE`.
