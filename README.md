# StreamViz

A domain-neutral research prototype for **low-latency visualisation and
replay of continuous streaming numeric data**. It implements a multi-threaded
ingest → clean → buffer → display → replay pipeline with an interactive
browser GUI. Data arrives through pluggable adapters: a synthetic generator
for smoke tests and **two real, openly licensed recordings from unrelated
scientific domains** (biomedical ECG and gravitational-wave strain) for
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

# one-off download of BOTH open datasets (verified by SHA-256)
python tools/fetch_dataset.py

# interactive dashboard at http://127.0.0.1:8765/
python examples/run_gui.py                        # synthetic smoke test
python examples/run_gui.py --source mitdb        # real ECG
python examples/run_gui.py --source gwosc        # real LIGO strain

# CROSS-DOMAIN measurement, both datasets -> output/cross_domain/
python examples/run_real_benchmark.py --dataset all
python examples/run_real_benchmark.py --dataset gwosc   # one domain only

# end-to-end synthetic demo (writes output/demo_report.json + PNGs)
python examples/run_demo.py --duration 4 --rate 4

# throughput / buffer sweep -> output/benchmark/benchmark.json
python examples/benchmark.py

# reproducible GUI figures for a manuscript
python tools/capture_gui.py                                     # synthetic
python tools/capture_gui.py --source mitdb --rate 4 \
    --output output/figures/gui_dashboard_real.png            # real ECG
python tools/capture_gui.py --source gwosc --rate 4 \
    --output output/figures/gui_dashboard_gwosc.png           # real strain

# pre-publication term scan
python tools/desensitize_check.py
```

`numpy` and `matplotlib` are the only third-party dependencies. The GUI and
the HTTP API use the Python standard library exclusively.

## Cross-domain data adapters

The pipeline never knows which scientific domain produced the samples. It
only sees three generic payload types (`SPECTRUM`, `WAVEFORM`, `AUDIO`).
`src/datasets.py` holds a registry; each entry declares upstream URLs,
SHA-256 checksums, license, DOI, citation, acknowledgement, physical units
and preprocessing, plus one loader function that returns a `RealRecord`
(a `(samples, channels)` float32 array plus provenance). **Adding a third
domain requires one loader and one registry entry** — no change to the
transport, buffers, views, GUI, benchmark or tests. The GUI dropdown, CLI
`--source`/`--dataset`, download tool and figure captions all read the
registry, so they stay in sync automatically.

| Key | Domain | Recording | Rate × channels | Licence / DOI |
| --- | --- | --- | --- | --- |
| `mitdb` | Biomedical / electrophysiology | MIT-BIH Arrhythmia DB v1.0.0, record `100` (PhysioNet): 30.09 min two-lead ECG | 360 Hz × 2 (MLII, V5) | ODC-By 1.0 · [10.13026/C2F305](https://doi.org/10.13026/C2F305) |
| `gwosc` | Gravitational-wave astronomy | GWTC-1 GW150914 strain, 4 kHz R1, 32 s (LIGO H1/L1) | 4096 Hz × 2 (H1, L1) | CC BY 4.0 · [10.7935/82H3-HH23](https://doi.org/10.7935/82H3-HH23) |

**Attribution is mandatory for both.** Cite Moody GB, Mark RG, *The impact of
the MIT-BIH Arrhythmia Database*, IEEE Eng Med Biol Mag 2001;20(3):45-50,
DOI `10.13026/C2F305`; and Abbott R et al., *Open data from the first and
second observing runs of Advanced LIGO and Advanced Virgo*, **SoftwareX**
2021;13:100658, DOI `10.1016/j.softx.2021.100658`, dataset DOI
`10.7935/82H3-HH23`. GWOSC also requests an acknowledgement of the LIGO
Scientific Collaboration, Virgo Collaboration and KAGRA (full text in
`REGISTRY["gwosc"].acknowledgement`). Put the relevant DOI in **every figure
caption** derived from that dataset.

**No signal value is synthesised.** For both datasets every payload is derived
from recorded samples, and `run_real_benchmark.py` re-asserts this per run:
`WAVEFORM` = 256 consecutive samples, `SPECTRUM` = magnitude of a
Hann-windowed DC-removed FFT of a 512-sample window, `AUDIO` =
**audification** of the recorded samples (time-compressed to 16 kHz int16),
not a separate recording or a tone. Original physical-unit values remain on
`RealRecord.signals_physical`. Preprocessing is linear and declared per
dataset: per-channel median removal + robust amplitude scaling for the ECG
(the two leads differ by ~10×); an additional 35–350 Hz zero-phase FFT
band-pass for detector strain, the standard preparation for transient
searches. Bins above that passband are exactly zero — an honest domain
signature, deliberately unlike the ECG's continuous low-frequency spectrum.
A wider passband lets one instrumental calibration line dominate the whole
view, which is why it is not used.

The sender runs in a **separate OS process**, representing an external
acquisition device; an in-process sender competes with the receiver, parser
and display threads for the GIL and caps the wire rate far below the
pipeline's real capacity. Payloads are pre-encoded from the recording; only
the sequence number and wall-clock timestamp are rewritten in the pacing
loop.

## Interactive GUI

Software-article reviewers need to *run* the tool, not just read about it, and
an illustrative example needs a real interface rather than a schematic.

- **Choose the data source** from a registry-driven dropdown: synthetic, real
  ECG, or real gravitational-wave strain. The active dataset's label, domain,
  licence and provenance appear in the badge and API state.
- **Start / stop** the stream and set target rate, display FIFO capacity,
  refresh frequency and the fraction of deliberately malformed datagrams.
- **Four live views**, redrawn on every state change from a shared
  `ViewState` — the same object the headless PNG renderer uses. Both
  renderers now also share one adaptive colour scale, because ECG energy and
  band-passed strain differ by an order of magnitude.
- **Live metrics**: wire throughput, received/parsed datagrams, FIFO overflow,
  archived WAV segments, rendered frames, ingest p95 and end-to-end p50/p95.
- **Audio replay**: play the newest archived segment (S6) or drag the source
  timestamp slider and play from that point (S7).
- **Export the current view** as a four-panel PNG, and an **event log**.

`GuiConsumer` is injected into `StreamPipeline` through `display_factory=`,
so the GUI is a front-end on the real ingest/parser/buffer threads. The
browser only reads snapshots; it never drives pipeline timing.

| Endpoint | Method | Purpose |
| --- | --- | --- |
| `/` | GET | dashboard (HTML/CSS/JS in `webui/`) |
| `/api/state` | GET | views, metrics, audio index, event log, dataset registry |
| `/api/control` | POST | `{"action": "start"\|"stop"\|"figure", "source": "...", ...}` |
| `/api/audio` | GET | `?mode=live` or `?mode=seek&ts=<source_ms>` → `audio/wav` |
| `/api/figure` | POST | export the current four-panel view as PNG |

The server binds to `127.0.0.1` only. Metrics are cached for 250 ms, view
snapshots by version, and `/api/state` responses for 50 ms, so a fast-polling
browser cannot steal CPU from the display consumer. `tools/capture_gui.py`
starts the same server plus a stream and screenshots it with a locally
installed headless Chrome/Edge. **GUI screenshots illustrate the interface;
quantitative claims come from the offline benchmark JSON** — starting a
headless browser on the same host can inflate the p95 shown in a screenshot.

## Package layout

| Path | Purpose |
| --- | --- |
| `src/protocol.py` | generic binary packet format + validation/cleaning rules |
| `src/buffers.py` | bounded FIFO, datagram wrapper, cache-full signal |
| `src/receiver.py` | UDP ingestion thread |
| `src/parser.py` | decode, sequence-gap accounting, routing, shutdown drain |
| `src/player.py` | WAV archiving, paced live playback, timestamp seek |
| `src/synthetic.py` | synthetic spectrum/waveform/audio generator |
| `src/datasets.py` | **pluggable dataset registry + two domain adapters** |
| `src/open_dataset.py` | generic real-data UDP replay + compatibility re-exports |
| `src/viewstate.py` | shared four-view state, colour scales, JSON encoding |
| `src/visualizer.py` | timer-driven four-view PNG renderer + renderer-free consumer |
| `src/gui.py` | GUI consumer, session control, localhost HTTP API |
| `src/pipeline.py` | lifecycle, configuration, metrics report |
| `webui/` | dashboard HTML, CSS and Canvas front-end |
| `examples/` | GUI launcher, demo, synthetic sweep, cross-domain benchmark |
| `tests/` | protocol / buffer / end-to-end / GUI / dataset-registry tests |
| `tools/` | dataset download+verify, term scan, reproducible GUI capture |

## Outputs

- `output/demo_report.json` — synthetic demo counters and latency percentiles
- `output/figures/view_*.png` — up to six headless demo snapshots
- `output/figures/gui_dashboard*.png` — GUI captures (synthetic / ECG / strain)
- `output/gui/gui_view.png`, `output/gui/gui_report.json` — GUI export + session metrics
- `output/benchmark/benchmark.json` — synthetic sweep results
- `output/cross_domain/cross_domain_summary.json` — **two-domain comparison index**
- `output/cross_domain/<key>/real_benchmark.json` — per-dataset measurements,
  provenance, SHA-256 and payload-authenticity checks
- `output/cross_domain/<key>/run_4mbps/figures/view_*.png` — four-view figures
  computed from that domain's recorded samples

## Real open-data measurements (cross-domain)

`python examples/run_real_benchmark.py --dataset all --rates 1,2,4,8`, 6 s
per run, display FIFO 64, no malformed injection. Parsed = received in every
run; FIFO overflow is zero throughout.

| Target (Mbit/s) | ECG measured | ECG E2E p50/p95 (ms) | Strain measured | Strain E2E p50/p95 (ms) |
| --- | --- | --- | --- | --- |
| 1 | 1.00 | 29 / 54 | 1.00 | 30.5 / 55 |
| 2 | 2.00 | 38 / 95 | 2.00 | 35 / 61 |
| 4 | 3.70 | 49 / 131 | 4.00 | 34 / 63 |
| 8 | 7.45 | 34 / 60 | 8.00 | 34 / 61 |

The two domains produce **the same packet shapes, the same delivery
behaviour and the same latency regime**, which is the point of the adapter
layer. The ECG run at 4 and 8 Mbit/s fell slightly short of target during
this particular eight-run sweep on a busy host; a single isolated 8 Mbit/s
run reaches 8.00 Mbit/s with 6522/6522 datagrams for both datasets. Treat
these as one reference run on one machine, not a performance guarantee, and
always report the host load alongside the numbers.

These are measurements of an **offline replay workload**, replayed faster
than real time to reach a target bit rate — not live acquisition hardware.
The verified record is looped to sustain each rate;
`distinct_signal_seconds` in the report states how much of the recording was
actually consumed.

## Measured behaviour (synthetic smoke test)

Reference run, 3 s at a 4 Mbit/s target
(`python examples/run_demo.py --duration 3 --rate 4`):

| Metric | Value |
| --- | --- |
| Measured wire throughput | 4.00 Mbit/s |
| Parsed / received datagrams | 2045 / 2061 (2% deliberately corrupted) |
| Ingest latency p95 | 3 ms |
| End-to-end latency p50 / p95 / p99 | 35 / 59 / 69 ms |
| Display FIFO overflow | 0 |
| WAV segments + timestamp seek | 64 segments, seek returns 3936 samples |

The sweep in `examples/benchmark.py` covers 1/2/4/8 Mbit/s against display
buffer capacities of 16 and 64. Delivery ratio stays at 0.96–0.99 (2% of
datagrams are intentionally malformed) and the smaller buffer generally
gives lower p95 at higher rates (e.g. 42 ms vs 63 ms at 8 Mbit/s),
illustrating the batching/latency trade-off. Headless figures are rendered
once at shutdown from buffered view states so image encoding never stalls
the consumer.

## Desensitisation

`tools/sensitive_terms.txt` holds **public, domain-neutral** identifiers that
must not appear in an openly distributed repository. Organisation names,
personnel names and location identifiers are kept in
`.workbuddy/sensitive_terms.local.txt`, which is excluded from version
control; `tools/desensitize_check.py` merges both lists, scans file contents
*and* paths, and exits non-zero on a hit. Wire the check into CI before
tagging a release.

Directories excluded from the scan are `.git`, `.workbuddy`, `output`, `data`,
`__pycache__` and virtual environments — **do not commit raw working material
or downloaded third-party signal files**, and never publish anything
extracted from them. `data/` is git-ignored: both datasets are downloaded and
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
  distributed-system evaluation; a concurrently running browser materially
  affects p95.
- The GUI needs a local browser. Headless machines can still run every test,
  the demo, the benchmark, the HTTP API and the Agg PNG renderer; only the
  browser screenshot step requires Chrome/Edge.
- Audio playback uses a null sink in the library API; the GUI streams WAV to
  the browser's own audio element instead.
- Real-data workloads are offline replays of public recordings. The audio
  channel is audification of those samples, not a microphone recording.
  Display normalisation is applied; physical units remain available.
- A single Python sender process tops out near 8 Mbit/s on the reference
  machine. Higher rates need multiple sender processes or a compiled sender.
- SoftwareX's Impact section still needs an independent external reuse case;
  two internal benchmark domains demonstrate generality, not adoption.

## Before any public release

1. Replace the copyright holder in `LICENSE` with the actual rights holder
   and obtain the institution's written approval for releasing the code.
2. Complete the institution's review procedure for external publication.
3. Add a persistent repository URL, a released version tag, and a
   reproducibility capsule. Include `tools/fetch_dataset.py` in the capsule
   instructions and keep **both** DOI/citation statements and the GWOSC
   acknowledgement in every figure caption derived from real-data figures.
4. Add at least one independent reuse case; a software-article reviewer will
   weigh demonstrated impact more heavily than the implementation itself.

## License

MIT — see `LICENSE`.
