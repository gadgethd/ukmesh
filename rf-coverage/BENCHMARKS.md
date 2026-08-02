# HopReach UK release benchmark

Run `scripts/benchmark-hopreach.sh` with Go 1.23 or newer (or run it inside
the pinned `golang:1.23-bookworm` image). The gate runs the unmodified upstream
CPU row loop and the indexed implementation at identical dimensions for:

- 500 and 4,600 positioned repeater fixtures;
- Standard (128 × 144 representative tile) and Precision (256 × 256
  representative tile) workloads;
- three identical UK-4,600 measurements for the release decision.

Both UK fixtures must be at least 30% faster than the executable upstream
oracle. Results must also pass the exact-output tests at the documented
`1e-5 dB` tolerance. The terrain memory gate uses the exact integer
Web-Mercator tile accounting and float32 grid size used by `demgrid.Load`; the
maximum padded progressive tile must be at least 30% smaller than the former
whole-UK terrain grid at both Standard zoom 11 and Precision zoom 13.

The script writes raw benchmark output, allocation counts, separate upstream
and optimized CPU/heap profiles, the memory working-set report, and a Go trace
of progressive tile/checkpoint completion. Its default output is a unique
temporary directory; set `HOPREACH_PROFILE_DIR` to retain it as a CI artifact.

## Accepted local release measurement

The 2026-08-02 release-gate run used Go 1.23, `GOMAXPROCS=4`, and the container
reported `QEMU Virtual CPU version 2.5+`. Each time below is the mean of three
identical one-iteration UK-4,600 runs:

| Fixture | Indexed | Upstream oracle | Ratio | Reduction |
| --- | ---: | ---: | ---: | ---: |
| Standard | 1.551 s | 2.712 s | 0.572 | 42.8% |
| Precision tile | 6.143 s | 9.640 s | 0.637 | 36.3% |

The exact DEM working-set calculation, including unchanged 100 km range
padding around every publication tile, measured:

| Tier | Progressive peak | Upstream whole-region | Reduction |
| --- | ---: | ---: | ---: |
| Standard z11 | 1,189 MiB | 2,626 MiB | 54.7% |
| Precision z13 | 5,586 MiB | 41,389 MiB | 86.5% |

The run passed the reference parity suite and generated independent CPU and
heap profiles plus a progressive completion trace. CI recreates these
artifacts for every gate rather than accepting the values in this document.
