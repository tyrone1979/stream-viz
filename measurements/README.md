# Archived measurements

Two files, nothing else. They are the exact artefacts behind Table 2, Table 3
and Figs. 2–5 of the paper, copied verbatim from the run directories
(`output/policy_ablation/`, `output/scenario/`) so that a reader who clones this
repository can check every published number without re-running a 40-minute
experiment.

| File | Produced by | Contents |
| --- | --- | --- |
| `policy_ablation.json` | `examples/run_policy_ablation.py --dataset all --policies drop_oldest,drop_newest,block,unbounded --rates 8 --consumer-delays 20,40,80 --repeat 3 --packets 3000 --capacity 8 --refresh-hz 50` | 72 per-run rows (24 cells × 3 repeats), median/min/max summary, workload description, dataset provenance |
| `extreme_scenario.json` | `examples/run_extreme_scenario.py --policies drop_oldest,drop_newest,block,unbounded --capacity 16 --duration 12 --rate 3 --sensors 6 --repeat 3` | 12 per-run rows (4 policies × 3 repeats), staleness distribution, `data_status: "synthetic"` |

Re-run the experiments themselves and compare: the numbers in these files are
medians over three runs on one host, and absolute values move with machine load.
What the paper claims to transfer is the ordering of the policies and the shape
of the curves, not the milliseconds.

The plotting script and the manuscript are not part of this repository's public
mirror, so the figures cannot be regenerated here; the schema is
self-describing (`summary` cells carry `median`/`min`/`max`, `runs` carry the
per-run rows) and the figure captions in the manuscript name the cells they
plot.

Note on provenance: `policy_ablation.json` is derived from two published
recordings — MIT-BIH Arrhythmia Database record 100 (ODC-By 1.0, DOI
10.13026/C2F305) and GW150914 strain from GWOSC (CC BY 4.0, DOI
10.7935/82H3-HH23). The recordings themselves are not redistributed here;
`tools/fetch_dataset.py` downloads and SHA-256-verifies them.
`extreme_scenario.json` is derived from synthetic payloads and says so in its
own `data_status` field.
