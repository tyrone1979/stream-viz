# StreamViz

A domain-neutral research prototype for **low-latency visualisation and
replay of continuous streaming numeric data**. It implements a multi-threaded
ingest → clean → buffer → display → replay pipeline, ships with an interactive
browser GUI and a synthetic UDP traffic generator, so the whole system can be
reproduced, measured and reviewed on an ordinary laptop without external
hardware or datasets.

> This repository contains **only synthetic data and generic transport fields**.
> It carries no affiliation, no personnel information, no application-specific
> protocol, and no restricted or proprietary material. See
> [Desensitisation](#desensitisation) below.

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

# end-to-end demo (writes output/demo_report.json + PNGs)
python examples/run_demo.py --duration 4 --rate 4

# throughput / buffer sweep -> output/benchmark/benchmark.json
python examples/benchmark.py

# reproducible GUI figure for a manuscript -> output/figures/gui_dashboard.png
python tools/capture_gui.py

# pre-publication term scan
python tools/desensitize_check.py
```

`numpy` and `matplotlib` are the only third-party dependencies. The GUI and
the HTTP API use the Python standard library exclusively.

## Interactive GUI

Software-article reviewers need to *run* the tool, not just read about it, and
an illustrative example needs a real interface rather than a schematic. The
dashboard therefore exposes the production pipeline directly:

- **Start / stop** a synthetic stream and set target rate, display FIFO
  capacity, refresh frequency and the fraction of deliberately malformed
  datagrams.
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
| `src/viewstate.py` | shared, renderer-agnostic four-view state + JSON encoding |
| `src/visualizer.py` | timer-driven four-view PNG renderer + renderer-free consumer |
| `src/gui.py` | GUI consumer, session control, localhost HTTP API |
| `src/pipeline.py` | lifecycle, configuration, metrics report |
| `webui/` | dashboard HTML, CSS and Canvas front-end |
| `examples/` | synthetic generator shim, GUI launcher, demo, benchmark sweep |
| `tests/` | protocol / buffer / end-to-end / GUI tests (stdlib `unittest`) |
| `tools/` | desensitisation term scan, reproducible GUI capture |

## Outputs

- `output/demo_report.json` — throughput, counters, ingress and end-to-end
  latency percentiles, audio segment count, replay results
- `output/figures/view_*.png` — up to six rendered snapshots from the headless
  demo (cleared and rewritten on every run, produced at shutdown)
- `output/figures/gui_dashboard.png` — headless-browser capture of the GUI
- `output/gui/gui_view.png` — four-panel figure exported from the GUI
- `output/gui/gui_report.json` — metrics of the GUI session
- `output/audio/segment_*.wav` — archived sample blocks with timestamp index
- `output/benchmark/benchmark.json` — sweep results for manuscript tables

## Measured behaviour

Reference run on a Windows laptop, 3 s of synthetic traffic at a 4 Mbit/s
target (`python examples/run_demo.py --duration 3 --rate 4`):

| Metric | Value |
| --- | --- |
| Measured wire throughput | ~2.9 Mbit/s against a 4 Mbit/s target |
| Parsed / received datagrams | ~1476 / ~1487 (rest are deliberately corrupted) |
| Ingest latency p95 | ~5 ms |
| End-to-end latency p50 / p95 / p99 | ~33 / ~59 / ~65 ms |
| Display FIFO overflow | 0 |
| WAV segments + timestamp seek | 46 segments, seek returns 3936 samples |

The sender paces itself with a coarse sleep plus a short spin, so the achieved
wire rate depends on host scheduling (other CPU-bound processes on the same
machine lower it). All numbers above come from the scripts in this repository
and are printed in full in `output/demo_report.json`; treat them as one
reference run, not a performance guarantee.

An interactive GUI session at 4 Mbit/s reports the same order of magnitude
(e.g. p50/p95 ≈ 32/59 ms with zero FIFO overflow in the captured figure),
which is the point of sharing one `ViewState` and one metering path between
the two front-ends.

The sweep in `examples/benchmark.py` covers 1/2/4/8 Mbit/s against display
buffer capacities of 16 and 64. Delivery ratio stays at 0.94–0.98 (2% of
datagrams are intentionally malformed), and the smaller buffer gives equal or
lower p95 latency at higher rates (e.g. ~45 ms vs ~55 ms at 8 Mbit/s),
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

Directories excluded from the scan are `.git`, `.workbuddy`, `output`,
`__pycache__` and virtual environments — **do not commit raw working material
into those directories**, and never publish anything extracted from them.

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

## Before any public release

1. Replace the copyright holder in `LICENSE` with the actual rights holder
   and obtain the institution's written approval for releasing the code.
2. Complete the institution's review procedure for external publication.
3. Add a persistent repository URL, a released version tag, and a
   reproducibility capsule.
4. Add at least one independent reuse case; a software-article reviewer will
   weigh demonstrated impact more heavily than the implementation itself.

## License

MIT — see `LICENSE`.
