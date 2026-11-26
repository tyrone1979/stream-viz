# StreamViz

**An instrumented, domain-agnostic testbed for measuring latency–freshness
trade-offs in bounded-buffer stream visualisation.**

Every real-time display of a continuous data stream eventually has to answer one
architectural question: *when the producer outruns the renderer and the buffer
fills, which end do you throw away?* StreamViz treats that question as an
experimental variable instead of a fixed implementation detail. It provides

1. a **switchable admission policy** on the display buffer
   (`drop_oldest` / `drop_newest` / `block` / `unbounded`),
2. a **per-stage conservation chain** — `sent → received → parsed → displayed`,
   with every discard attributed to a stage and a reason, and
3. **frame-visibility staleness** — the age, at the moment a frame becomes
   visible, of the newest sample that frame contains,

so the trade-off can be measured rather than asserted, on any domain that can be
expressed as one loader function.

> **What this software is not.** It is not a new visualisation algorithm, not a
> faster plotter, and not a live acquisition system. The four views and the
> browser GUI are illustrative examples: they are the object under test, not the
> contribution. Timing numbers here are measurements of an **offline replay
> workload** on **one host over loopback**, never a performance guarantee.

The repository ships **no third-party signal files** and contains only generic
transport fields. It carries no affiliation, no personnel information, no
application-specific protocol, and no restricted or proprietary material. See
[Desensitisation](#desensitisation).

## What the software lets you measure

```
UDP sender --> [S1] receiver thread --> raw source FIFO   (sent -> received)
                        |
                        v
             [S2] parser thread: validate / clean / classify  (-> parsed)
                        |                    |
              spectrum / waveform      audio samples
                        v                    v
                 display FIFO        bounded audio buffer
                        |                    |
              cache-full signal      flush -> WAV segments
                        v                    v
             [S3] timer-driven view   [S4] live playback
             (admission policy here)  [S5] timestamp-seek replay
                        |
                        v
              frame visible  (-> displayed, staleness stamped here)
```

Three questions the harness is built to answer:

| Question | Variable you set | What you read out |
| --- | --- | --- |
| Which end of a full buffer should be discarded? | `--policies` | staleness p95, discarded-item age, packets lost |
| How does a slower renderer change the answer? | `--consumer-delays` | staleness vs render budget, packets per frame |
| Does a domain change the answer? | `--dataset` | the same cells for ECG and gravitational-wave strain |

**The shipped default is `drop_oldest`** — in `BoundedFIFO` itself and in
`PipelineConfig`, so the GUI, the demo and the delivery benchmark all run it.
At the default capacity (64) and 30 Hz refresh the buffer does not fill at the
rates this reference sender reaches (≤8 Mbit/s), so the policy does not engage
in normal use; the ablation therefore over-subscribes on purpose (capacity 8,
20–80 ms render budget). Switching the policy is a constructor argument
(`PipelineConfig(overflow_policy="drop_newest")`) or a flag on the ablation
harness; it is not exposed in the GUI.

## Quick start

```bash
python -m pip install -r requirements.txt

# self-tests (headless, no browser needed)
python -m unittest discover -s tests -v

# one-off download of BOTH open datasets (verified by SHA-256)
python tools/fetch_dataset.py

# THE EXPERIMENT: overflow-policy ablation, both domains, median of 3
python examples/run_policy_ablation.py --dataset all \
    --policies drop_oldest,drop_newest,block,unbounded \
    --rates 8 --consumer-delays 20,40,80 --repeat 3 \
    --packets 3000 --capacity 8 --output output/policy_ablation

# extreme-condition scenario: bursty multi-sensor array with link outages,
# median of 3 (a bursty profile has more run-to-run spread than a steady one)
python examples/run_extreme_scenario.py \
    --policies drop_oldest,drop_newest,block,unbounded \
    --capacity 16 --duration 12 --repeat 3 --output output/scenario

# interactive dashboard at http://127.0.0.1:8765/
python examples/run_gui.py                        # synthetic smoke test
python examples/run_gui.py --source mitdb        # real ECG
python examples/run_gui.py --source gwosc        # real LIGO strain

# delivery/latency sweep, both datasets -> output/cross_domain/
python examples/run_real_benchmark.py --dataset all

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

## The experiment: overflow-policy ablation

### Design

The display buffer is deliberately over-subscribed so that it *must* discard:
the sender pushes 3000 datagrams at 8 Mbit/s (≈1500 of them bound for the
display) while the consumer is limited to one frame per render budget. Every
policy then faces the same injected load and a full buffer; only its admission
decision differs.

| Knob | Value | Why |
| --- | --- | --- |
| `--policies` | `drop_oldest`, `drop_newest`, `block`, `unbounded` | the four defensible reactions to a full buffer |
| `--packets` | 3000 per run | fixed load, so cells are comparable; duration-based pacing made runs differ by 2.5× |
| `--consumer-delays` | 20, 40, 80 ms | the render budget is the design variable: how expensive is one frame? |
| `--capacity` | 8 | small on purpose, so the buffer binds |
| `--repeat` | 3 | a single run is a scheduling sample, not a measurement |
| `--recv-buf` | OS default | loss is not the variable being studied here |
| audio archiving | off | WAV writing puts file I/O on the parse thread and stalls parsing by hundreds of ms (see [Lessons](#two-engineering-lessons-worth-inheriting)) |

Two implementation details matter for the validity of the numbers:

- **The consumer folds every accepted packet into a real `ViewState`.** That
  fold costs ≈0.2 ms per packet on the reference machine (measured, not
  injected). A benchmark that skips it would make an unbounded buffer look
  free, because the whole cost of an unbounded buffer is the work per frame.
- **Every packet is accounted for.** `frame_buffer_in` is compared with
  `delivered + discarded + still_occupied + discarded_at_shutdown` on every run;
  a cell whose chain does not close is reported as `BROKEN` rather than quoted.

### Results

One cell = one policy at one render budget, on one domain, median of 3 runs.
Load is identical in every cell: 3000 datagrams at 8.00 Mbit/s, 1500 of them
bound for the display, `udp_delivery_ratio` 1.000 and zero sequence gaps in all
36 runs (a loopback property of these runs, not a protocol guarantee). Every
cell closed its conservation chain.

**Frame-visibility staleness, p95 in ms** (median of 3; full ranges in
`output/policy_ablation/policy_ablation.json`):

| Policy | Render budget | ECG | GW strain | Discarded (ECG / GW) | Packets per frame |
| --- | --- | --- | --- | --- | --- |
| `drop_oldest` | 20 ms | **21.0** | **19.0** | 772 / 762 | 8.0 |
| `drop_oldest` | 40 ms | **20.0** | **19.0** | 1008 / 1004 | 8.0 |
| `drop_oldest` | 80 ms | **20.0** | **19.5** | 1252 / 1252 | 8.0 |
| `drop_newest` | 20 ms | 35.0 | 35.0 | 770 / 758 | 8.0 |
| `drop_newest` | 40 ms | 51.0 | 50.0 | 1012 / 1004 | 8.0 |
| `drop_newest` | 80 ms | 97.0 | 94.5 | 1252 / 1252 | 8.0 |
| `block` | 20 ms | 1374.8 | 1371.6 | 748 / 740 | 8.0 |
| `block` | 40 ms | 1791.0 | 1787.0 | 996 / 996 | 8.0 |
| `block` | 80 ms | 2217.0 | 2213.0 | 1236 / 1236 | 8.0 |
| `unbounded` | 20 ms | 20.0 | 20.5 | 0 / 0 | 16.3 / 16.5 |
| `unbounded` | 40 ms | 21.0 | 23.0 | 0 / 0 | 24.6 / 24.6 |
| `unbounded` | 80 ms | 27.0 | 98.8 † | 0 / 0 | 48.4 / 42.9 |

† the only cell with a wide spread: 54.5–129.7 ms across the three runs — see
finding 4.

### What the numbers say

1. **The two drop policies lose the same amount and show very different data.**
   At an 80 ms budget both discard 1252 of the 1500 display-bound packets (83%),
   yet `drop_oldest` shows a frame whose newest sample is 20 ms old and
   `drop_newest` shows one that is 97 ms old — a 4.9× difference in freshness
   for identical loss. At 20 ms the counts are 772 vs 770 and the freshness gap
   is still 1.7×. Choosing the wrong end of the buffer buys nothing.
2. **`drop_oldest` is the only policy whose staleness is invariant to the render
   budget.** 21 → 20 → 20 ms (ECG) and 19 → 19 → 19.5 ms (strain) across a 4×
   change in per-frame cost, while `drop_newest` tracks the budget almost exactly
   (35 → 51 → 97 ms; 35 → 50 → 94.5 ms). So *which policy* to use does not depend
   on how fast the renderer is; *how much data you throw away* does (772 → 1252).
3. **Blocking converts loss into latency and still loses the data.** `block`
   discards almost exactly as many packets as `drop_oldest` (1236 vs 1252 at
   80 ms) while staleness reaches 2.2 s — 110× worse. The packets it eventually
   discards are themselves 2.2 s old: back-pressure does not preserve the data,
   it postpones the discard until the data is worthless. This is the result that
   contradicts the intuition that "not dropping" is safer.
4. **`unbounded` is not free; it pays in work per frame.** Zero discards and
   staleness close to `drop_oldest` at a 20 ms budget, but the consumer folds
   16 → 25 → 48 packets per frame as the budget grows, and the memory it holds
   grows with the deficit. At an 80 ms budget on strain the three runs spread
   over 54.5–129.7 ms: the backlog → per-frame work → slower tick loop is
   starting to bite. The fold cost used here is linear and measured (≈0.2 ms per
   packet); a Canvas or Matplotlib repaint saturates faster, so this column is
   penalised *less* here than in a production front-end.
5. **Both domains agree on the shape, not on the constant.** Every ranking, the
   flatness of `drop_oldest`, and the near-equality of discard counts replicate
   across electrophysiology and gravitational-wave strain. Absolute values agree
   within 2 ms except where the load-dependent loop dominates (the cell marked
   †). That is the sense in which the harness is domain-neutral: the *conclusion*
   transfers, and a third domain can be added with one loader to test it again.

The practical reading: if the newest sample is what matters (live monitoring),
drop the oldest and accept that you will discard most of the stream under
overload; if sequence continuity is what matters (archival, offline analysis),
drop the newest and accept staleness that scales with your render budget; do not
block on the display path.

## Two engineering lessons worth inheriting

Both were found by measurement, and both are the kind of thing that is invisible
in an architecture diagram.

1. **A free-running consumer starves the thread it is measuring.** With a
   capacity-8 buffer under overload the ready signal is set continuously, so a
   consumer that re-checks immediately never blocks: it spins through the GIL and
   delays the parser enough that the measured latency is dominated by the
   measurement loop. The consumer now ticks at `max(1/refresh, render_budget)`
   and never faster than the work it just did (`spent` is subtracted from the
   period).
2. **File I/O on the parse thread is a latency bug, not an optimisation
   detail.** Writing WAV segments from the parser stalls it for hundreds of
   milliseconds on a host with real-time virus scanning; the display buffer then
   overflows in bursts. Disabling audio archiving moved the baseline staleness
   from ~1800 ms to ~26 ms on the same workload. Archiving is therefore an
   explicit `archive_audio` flag, off in every measurement that studies the
   display path.

Timestamps are also a measurement decision: staleness and packet age are
stamped when the state fold *finishes*, because a frame is only visible once it
has been folded, and the fold cost grows with the batch size. Stamping on
arrival credits a large backlog with latency it does not deliver.

## Measurement definitions

The chain is reported per stage so that a loss cannot hide behind an average:

```
sent                 datagrams the sender's sendto() accepted
 └ udp_received      datagrams the socket delivered (sequence gaps expose the rest)
    └ parsed         datagrams that decoded cleanly (parse_yield = parsed/received)
       └ displayed   packets the consumer folded into a frame
          ├ discarded (by stage: source FIFO / display FIFO / at shutdown)
          └ still buffered
```

- **`staleness`** — age of the newest packet contained in a frame, at the moment
  the frame becomes visible. This is the metric that distinguishes the policies;
  mean packet age cannot, because a frame that redraws a long backlog has a fresh
  newest sample and a very old average one.
- **`dropped_age`** — age of the items that were discarded. An age near
  end-to-end latency means stale data was thrown away; an age near zero means
  fresh data was.
- **`packets_per_frame`** — how much work each frame actually costs. A policy
  that discards nothing pays here instead of in `discarded`.
- **`udp_delivery_ratio`** — `received / sent`, and therefore includes kernel
  and transport loss. It is **not** a lossless guarantee: UDP has none. Every
  reported 1.000 means "no loss was detected on this loopback run at this rate",
  which is a property of the run, not of the protocol.

## Cross-domain data adapters

The pipeline never knows which scientific domain produced the samples. It only
sees three generic payload types (`SPECTRUM`, `WAVEFORM`, `AUDIO`).
`src/datasets.py` holds a registry; each entry declares upstream URLs, SHA-256
checksums, license, DOI, citation, acknowledgement, physical units and
preprocessing, plus one loader function that returns a `RealRecord` (a
`(samples, channels)` float32 array plus provenance). **Adding a third domain
requires one loader and one registry entry** — no change to the transport,
buffers, views, GUI, benchmark or tests. The GUI dropdown, CLI
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

### Adding your own domain

`third_party_adapter_template.py` is a runnable starting point rather than a
description: it contains a loader for a two-column text recording plus a JSON
sidecar, a fully-commented `DatasetSpec`, and a `--check` mode that validates a
loader's output against the same contract the built-in adapters satisfy
(`validate_record` in `src/datasets.py`: 2-D float32, finite, display-scaled to
`|x| <= 1.5`, matching channel names, positive spectrum reference, non-empty
checksums, and non-blank licence / DOI / citation fields).

```bash
cp third_party_adapter_template.py my_domain_adapter.py   # edit 3 places
python my_domain_adapter.py --check --data-dir path/to/data
```

The check is structural, not physical -- it cannot tell whether the numbers are
correct, which is why an adapter with a blank DOI is rejected rather than
warned about. Once the spec is registered, `--source my_domain` (GUI),
`--dataset my_domain` (ablation) and the download tool pick it up with no other
change. This is the extension claim in testable form: if it takes more than one
loader and one registry entry, the claim is false and the template is where
that shows up.

## Interactive GUI (illustrative example)

The GUI exists so a reviewer can *run* the pipeline rather than read about it,
and so a new adapter can be inspected immediately. It is a front-end on the
real ingest/parser/buffer threads — never a mock-up — but every quantitative
claim in a manuscript comes from the offline harness, not from a screenshot.

- **Choose the data source** from a registry-driven dropdown: synthetic, real
  ECG, or real gravitational-wave strain. The active dataset's label, domain,
  licence and provenance appear in the badge and API state.
- **Start / stop** the stream and set target rate, display FIFO capacity,
  **full-buffer policy**, refresh frequency and the fraction of deliberately
  malformed datagrams. Switching the policy while the stream runs is the
  point: the same load, the same buffer size, a different answer to "what does
  the operator see".
- **Four live views**, redrawn on every state change from a shared
  `ViewState` — the same object the headless consumer and the PNG renderer use.
  All renderers share one adaptive colour scale, because ECG energy and
  band-passed strain differ by an order of magnitude.
- **Live metrics**: wire throughput, received/parsed datagrams, per-stage
  discards, **frame staleness p95**, archived WAV segments, rendered frames,
  ingest p95 and end-to-end p50/p95.
- **Audio replay**: play the newest archived segment (S6) or drag the source
  timestamp slider and play from that point (S7).
- **Export the current view** as a four-panel PNG, and an **event log**.

| Endpoint | Method | Purpose |
| --- | --- | --- |
| `/` | GET | dashboard (HTML/CSS/JS in `webui/`) |
| `/api/state` | GET | views, metrics, audio index, event log, dataset registry |
| `/api/control` | POST | `{"action": "start"\|"stop"\|"figure", "source": "...", ...}` |
| `/api/audio` | GET | `?mode=live` or `?mode=seek&ts=<source_ms>` → `audio/wav` |
| `/api/figure` | POST | export the current four-panel view as PNG |

The server binds to `127.0.0.1` only. Metrics are cached for 250 ms, view
snapshots by version, and `/api/state` responses for 50 ms, so a fast-polling
browser cannot steal CPU from the display consumer — an earlier version of this
code measured a p95 inflation from ~60 ms to 1566–3017 ms purely from browser
polling. `tools/capture_gui.py` starts the same server plus a stream and
screenshots it with a locally installed headless Chrome/Edge.

The policy switch is visible in a pair of reproducible captures — same load
(real ECG, 8 Mbit/s), same buffer (8 frames), only the policy differs:

```bash
python tools/capture_gui.py --source mitdb --rate 8 --capacity 8 \
    --policy drop_oldest --wait 6 --output output/figures/gui_policy_drop_oldest.png
python tools/capture_gui.py --source mitdb --rate 8 --capacity 8 \
    --policy block --wait 6 --output output/figures/gui_policy_block.png
```

| Capture | Policy | Frame staleness p95 | FIFO drops | Parsed |
| --- | --- | --- | --- | --- |
| `gui_policy_drop_oldest.png` | `drop_oldest` | 143.8 ms | 2111 | 7616 |
| `gui_policy_block.png` | `block` | 5748.9 ms | 0 | 3201 |

**Read these as an illustration, not as a measurement**: a headless browser and
a screenshot share the host with the consumer, and both sessions ran for
different lengths. The quantitative version of this comparison is [the
ablation](#the-experiment-overflow-policy-ablation) above. The point of the pair
is that a reviewer can reproduce it in two commands and see a 40× difference in
the staleness readout with the policy as the only input that changed.

## Application scenarios

Two scenarios show what the harness is for. Both are documented with the
command that produces them and the JSON that results; neither is offered as
evidence of adoption.

### Case 1 — a third party reproduces the ablation on their own data

This is the reuse path the design is optimised for, and the one a reviewer can
verify without a collaboration:

1. write one loader returning a `RealRecord` and one `REGISTRY` entry
   (no change to transport, buffers, consumer, GUI or harness),
2. run `examples/run_policy_ablation.py --dataset <key> --repeat 3`,
3. read the `summary` cells: staleness p50/p95, discarded-item age, packets per
   frame, and the per-stage conservation chain.

Because the harness asserts the chain `sent → received → parsed → displayed`
for every cell, a third party cannot accidentally publish a number whose
packets went somewhere unaccounted. That is the reusable artefact: not our two
datasets, but the measurement contract around them. What it demonstrates is
*capability* — two unrelated domains, 36 cells, every chain closed — not
external adoption.

### Case 2 — a multi-sensor acoustic deployment under burst and outage

A field deployment of a multi-sensor acoustic array is the opposite of a steady
benchmark load: long quiet stretches, short broadband bursts when a transient
crosses the array, and outages when a sensor or a link drops. That is exactly
where the admission policy decides what an operator sees, and where an audible
channel matters as much as the four views.

The recordings that would make a faithful replay possible are
**access-restricted and cannot be redistributed**, so this repository ships no
field data for this scenario. Instead `src/scenarios.py` generates a *load
profile* and a *signal shape* that exercise the same regimes — narrowband lines
per sensor, impulsive transients, a noise floor, a burst gain and recurrent
outages — from a seeded generator. The report writes
`workload.data_status: "synthetic"` and the provenance string into the JSON, so
a number produced this way cannot be mistaken for a field measurement. It is
not a model of any particular sensor, array geometry or propagation
environment, and it must not be cited as measured data.

```bash
python examples/run_extreme_scenario.py \
    --policies drop_oldest,drop_newest,block,unbounded \
    --capacity 16 --duration 12 --rate 3 --repeat 3 \
    --sensors 6 --cycle 3 --burst 0.9 --outage 0.4 --burst-gain 3 \
    --output output/scenario
```

Load: 6 sensors, 3 Mbit/s base, ×3 burst for 0.9 s and a 0.4 s outage every
3 s; display buffer 16 frames at 20 Hz. Median of three runs, with the observed
range in brackets — a bursty profile has more run-to-run spread than a steady
one, and a single run would over-state the precision:

| Policy | Staleness p95 (ms) | Staleness max (ms) | Frame drops | Packets per frame |
| --- | --- | --- | --- | --- |
| `drop_oldest` | **33.0** [27–40] | 60.0 | 4270 | 15.8 |
| `drop_newest` | **70.0** [68–77] | 74.0 | 4459 | 15.9 |
| `block` | **7494.7** [6956–7513] | 7724.0 | 4004 | 16.0 |
| `unbounded` | **24.0** [23–25] | 55.0 | 0 | 42.6 |

All twelve runs closed their conservation chain (`sent ≈ 8120`, `received =
parsed`, `sequence_gaps = 0`). The burst amplifies the steady-load result:
`drop_newest` discards *more* than `drop_oldest` (4459 vs 4270) yet shows data
2.1× older; `block` reaches 7.5 s — longer than the burst itself — while
discarding almost as much as `drop_oldest`, so back-pressure bought nothing;
`unbounded` keeps everything and pays with 42.6 packets folded per frame
instead of 16.

The audio path is the same one the GUI exposes: audification of the sensor
content into 16 kHz WAV segments, live playback of the newest segment, and
timestamp-seek replay into the history. Pass `--audio` to archive segments
during the run; note that this puts file I/O on the parse thread, which is why
it is off in every timing run (see [Two engineering
lessons](#two-engineering-lessons-worth-inheriting)).

## Related work and positioning

Real-time signal display is a crowded space; the question a reviewer will ask is
*why not use one of these*. The honest answer is that they solve a different
problem, and the difference is not speed. Licences and versions below were
checked against project sources in November 2026; the claims in the last column
are deliberately narrow, because a wrong claim about a well-known tool is worse
than no comparison at all.

| Family | Representative tools (licence, version) | What they give you | What they do not give you |
| --- | --- | --- | --- |
| Desktop stream plotters | **PlotJuggler** 4.1.0 / 3.17.2 (MPL-2.0); **glScopeClient** (BSD-3, self-described alpha) | drag-and-drop plotting of live streams (MQTT, WebSocket, ZeroMQ, UDP, ROS topics) and recordings, with transform chains | the display buffer is an implementation detail, not a controllable object: no switchable admission policy, no per-stage discard attribution, no meter for the age of what is on screen. glScopeClient is bound to instrument transports (SCPI) rather than to an arbitrary packet stream |
| Robotics / ML log tooling | **Rerun** (MIT / Apache-2.0, SDK-first); **Foxglove** (current application commercial; Studio 1.x was open source) | multimodal logging and inspection, MCAP ecosystem, hosted collaboration | both assume you instrument the *producer* with their SDK or log format; neither treats the consumer's buffer as an experimental variable, and neither reports the freshness of a rendered frame |
| Dashboard stacks | **Grafana** (AGPL-3.0) over a time-series database | operational dashboards, alerting, retention | latency is dominated by the scrape or aggregation interval; the display-side buffer is not a first-class object |
| Dashboard libraries | **Bokeh** (BSD-3), **Dash** (MIT), **Streamlit** (Apache-2.0) | rendering and layout primitives | the drawing layer only; the pipeline, its buffering and its metering are left to the user |
| SDR / DSP flowgraphs | **GNU Radio** 3.x (GPL-3.0; the GR4 runtime is MIT) incl. its QT FFT/waterfall sinks; **inspectrum** 0.4.0 (GPL-3.0-or-later) | spectrum and waterfall views inside a signal-processing flowgraph; inspectrum is an offline analyser for large IQ captures | the visualisation is a sink inside the DSP toolchain and carries radio/IQ semantics; neither is a domain-agnostic replay harness, and neither measures display-side staleness |
| Robotics visualisers | **RViz2** (BSD-3) | topic visualisation with QoS settings | QoS is a *transport* property (reliability, history depth), not a display-admission policy; it does not tell you how old the rendered data is |
| Passive acoustic monitoring | **PAMGuard** 2.02.18 (GPL-3.0); **Triton** (MATLAB, freely distributed, last packaged 2016); **Ishmael** (Windows freeware, MATLAB plug-in interface); **Raven Pro** (proprietary, paid per machine) | the closest neighbours to the multi-sensor acoustic case: real-time acquisition, spectrograms, detectors, classifiers, event databases | they are domain instruments — the product is detection and classification, not the transport/display contract. PAMGuard does surface acquisition-level overruns, so the difference is *not* that it hides loss: loss is simply not attributed through a `sent → received → parsed → displayed` chain, and cannot be traded against freshness by switching a policy |
| Biosignal toolkits | **BrainFlow** 5.23.0 (MIT) + **OpenBCI GUI** (MIT) | a uniform acquisition SDK across boards, plus a device-bound GUI with real-time filtering | a library and a device dashboard, not a measurement harness: ring buffers are internal and the GUI is tied to one vendor's hardware |
| Stream-processing benchmarks | **Yahoo! Streaming Benchmark**, **NEXMark** (Flink / Storm) | throughput and end-to-end latency of the processing path under load | they measure the processing path, not the freshness of what a human is looking at after the display buffer has dropped data |

The gap StreamViz targets is narrow and specific: **a harness in which the
buffer admission policy is the independent variable, the discards are attributed
per stage, and staleness is defined at frame visibility** — while staying
domain-agnostic through a one-function adapter.

Two things we deliberately do **not** claim: that no tool does any part of this
(PlotJuggler plots faster than we do, PAMGuard has detectors we will never
have), and that the comparison above is a benchmark. It is a positioning
argument from documented capabilities. If a closer tool exists, the adapter
interface makes an honest head-to-head comparison cheap to run — which is the
point of shipping the harness rather than a screenshot.

## Package layout

| Path | Purpose |
| --- | --- |
| `src/protocol.py` | generic binary packet format + validation/cleaning rules |
| `src/buffers.py` | bounded FIFO with selectable overflow policy and discard-age metering |
| `src/receiver.py` | UDP ingestion thread, optional `SO_RCVBUF` |
| `src/parser.py` | decode, sequence-gap accounting, routing, shutdown drain |
| `src/player.py` | WAV archiving, paced live playback, timestamp seek |
| `src/synthetic.py` | synthetic spectrum/waveform/audio generator |
| `src/scenarios.py` | **burst/outage multi-sensor acoustic load profile (synthetic)** |
| `src/datasets.py` | **pluggable dataset registry + two domain adapters** |
| `src/open_dataset.py` | generic real-data UDP replay + compatibility re-exports |
| `src/viewstate.py` | shared four-view state, colour scales, staleness, JSON encoding |
| `src/visualizer.py` | timer-driven PNG renderer + renderer-free state-folding consumer |
| `src/gui.py` | GUI consumer, session control, localhost HTTP API |
| `src/pipeline.py` | lifecycle, configuration, conservation report |
| `webui/` | dashboard HTML, CSS and Canvas front-end |
| `examples/` | ablation harness, extreme scenario, GUI launcher, demo, synthetic sweep, benchmark |
| `tests/` | protocol / buffer / policy / scenario / adapter-contract / end-to-end / GUI / dataset-registry tests |
| `tools/` | dataset download+verify, term scan, reproducible GUI capture |
| `third_party_adapter_template.py` | **runnable new-domain adapter + `--check` contract validator** |

## Outputs

- `output/policy_ablation/policy_ablation.json` — **the ablation**: per-run
  rows, median-of-N `summary` cells with observed ranges, workload description
- `output/scenario/extreme_scenario.json` — **burst/outage scenario**: per-run
  rows, median-of-N `summary` cells with observed ranges, per-stage
  conservation, staleness distribution, packets per frame, `data_status`
- `output/scenario/<policy>/rep<N>/figures/view_*.png` — four views captured
  from the scenario's synthetic payloads
- `output/cross_domain/cross_domain_summary.json` — two-domain delivery and
  latency index
- `output/cross_domain/<key>/real_benchmark.json` — per-dataset measurements,
  provenance, SHA-256 and payload-authenticity checks
- `output/cross_domain/<key>/run_4mbps/figures/view_*.png` — four-view figures
  computed from that domain's recorded samples
- `output/demo_report.json` — synthetic demo counters and latency percentiles
- `output/figures/gui_dashboard*.png` — GUI captures (synthetic / ECG / strain)
- `output/benchmark/benchmark.json` — synthetic throughput/buffer sweep
- `paper/figures/fig{2,3,4,5}_*.png` — **the manuscript figures**, regenerated
  from the JSON above by `python examples/make_figures.py`; no figure is a
  hand-edited screenshot except Fig. 4, which `tools/capture_gui.py` captures
  headlessly

## Reference measurements (delivery path)

`python examples/run_real_benchmark.py --dataset all --rates 1,2,4,8`, 6 s per
run, display FIFO 64, no malformed injection, renderer-free consumer, no
policies engaged (the buffer does not fill at these rates with capacity 64).

| Target (Mbit/s) | ECG measured | ECG E2E p50/p95 (ms) | Strain measured | Strain E2E p50/p95 (ms) |
| --- | --- | --- | --- | --- |
| 1 | 1.00 | 29 / 54 | 1.00 | 30.5 / 55 |
| 2 | 2.00 | 38 / 95 | 2.00 | 35 / 61 |
| 4 | 3.70 | 49 / 131 | 4.00 | 34 / 63 |
| 8 | 7.45 | 34 / 60 | 8.00 | 34 / 61 |

The two domains produce the same packet shapes, the same delivery behaviour and
the same latency regime, which is what the adapter layer is for. The ECG run at
4 and 8 Mbit/s fell slightly short of target during this eight-run sweep on a
busy host; an isolated 8 Mbit/s run reaches 8.00 Mbit/s with 6522/6522
datagrams for both datasets. These are one reference run on one machine, not a
performance guarantee — always report host load alongside the numbers.

These are measurements of an **offline replay workload**, replayed faster than
real time to reach a target bit rate — not live acquisition hardware. The
verified record is looped to sustain each rate; `distinct_signal_seconds` in the
report states how much of the recording was actually consumed.

## Synthetic smoke test

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
datagrams are intentionally malformed) and the smaller buffer generally gives
lower p95 at higher rates (e.g. 42 ms vs 63 ms at 8 Mbit/s).

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
or downloaded third-party signal files**, and never publish anything extracted
from them. `data/` is git-ignored: both datasets are downloaded and
checksum-verified on demand rather than redistributed here.

The wire protocol deliberately exposes only `magic / version / type /
channel / sequence / timestamp / payload length`. All payloads are plain
numeric vectors; no acquisition geometry, platform parameters or
domain-specific units are present.

Recordings from restricted deployments are **not** in this repository and are
not reconstructed from: the burst/outage scenario in `src/scenarios.py`
generates its own payloads from a seeded generator and labels them
`data_status: "synthetic"`. That is a deliberate boundary — the load regime is
reproducible, the field data is not redistributed.

## Known limitations

- **The render cost model is linear in the batch size.** The consumer folds
  every accepted packet into a `ViewState`, which costs ≈0.2 ms per packet on
  the reference machine. A real Canvas/Matplotlib repaint grows faster than
  linear once the drawn area is saturated, so the `unbounded` column is
  penalised *less* here than it would be in a production front-end, not more.
- **One host, one clock, loopback.** p95 depends on host load; a concurrently
  running browser materially inflates it. Cells are median-of-3 with the
  observed range reported, but this is not a distributed-system evaluation.
- **8 Mbit/s is a ceiling for one Python sender process.** Higher rates need
  multiple senders or a compiled one.
- **`block` is bistable.** At a 4 ms/Mbit deficit it either stays near the
  render budget or jumps to ~1 s within the same cell; the median hides the
  bimodality, so the range must be quoted with it.
- **UDP provides no delivery guarantee.** The harness reports loss per stage;
  a 1.000 delivery ratio is a property of a loopback run, not of the protocol.
- **Real-data workloads are offline replays** of public recordings. The audio
  channel is audification of those samples, not a microphone recording.
- **The burst/outage scenario is synthetic by construction.** It reproduces a
  load profile and a transient-rich signal shape, not recorded field data; the
  field recordings behind that scenario are access-restricted. Do not quote its
  numbers as field measurements.
- **No independent external reuse yet.** Two internal benchmark domains
  demonstrate generality, not adoption; a software-article Impact statement
  needs the latter.
- The GUI needs a local browser. Headless machines can still run every test,
  the demo, the ablation, the HTTP API and the Agg PNG renderer.

## Before any public release

1. Replace the copyright holder in `LICENSE` with the actual rights holder and
   obtain the institution's written approval for releasing the code.
2. Complete the institution's review procedure for external publication.
3. Add a persistent repository URL, a released version tag, and a
   reproducibility capsule. Include `tools/fetch_dataset.py` in the capsule
   instructions and keep **both** DOI/citation statements and the GWOSC
   acknowledgement in every figure caption derived from real-data figures.
4. Add at least one independent reuse case — an adapter written and run by
   someone outside the group is the cheapest credible form.

## License

MIT — see `LICENSE`.
