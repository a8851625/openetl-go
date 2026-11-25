# Candidate build, certification and promotion

The release path is build → certify → promote. GoReleaser 1.26.2 runs once with
`release --skip=publish` (and `--snapshot` for PRs), preserving default/extism
binaries and all ten platform archives. It does not support a publish-only
`--skip-build` release; the publishing job therefore loads the saved image and
uploads the saved archives directly.

`hack/release_candidate.py` captures source commit/input hashes, compiler/tool
versions, binary/archive SHA256 values and the actual image config ID. The image's
`/app/openetl-go` hash must equal its Linux archive executable. Reports live outside
the image, so adding a digest to a report cannot require another image build.

Every candidate consumes the two primary path scripts, all seven tagged path
artifacts, the complete 16-script connector matrix, the three P1 browser probes
and the external-readiness binding probe. Tagged tests use `OPENETL_BIN`,
`E2E_EXPECTED_BINARY_SHA256` and `E2E_CANDIDATE_REQUIRED=1`; a missing or changed
binary is an error, not permission to compile a substitute. Shell tests consume
the explicit immutable `IMAGE` with `E2E_SKIP_BUILD=1`. Each test source checkout is
isolated at the candidate commit.

The candidate ledger, command logs, per-case execution windows, fixture image
identities, connector manifest and tagged JSON artifacts are retained as CI
artifacts. Missing/failed/skipped/cancelled cases, expired evidence, changed
archives/logs, mismatched source or wrong executable hashes block promotion.
Both the existing source gate and exact-candidate gate must pass the PR aggregate.

The publication job contains no Go/frontend/container build. It loads the saved
Docker archive, tags and pushes that image, pulls the pushed reference and checks
its config ID and executable SHA256. Its receipt records immutable registry
digests. GitHub release archives are the same files checked during certification.
The beta workflow is a manual retry of an existing certified bundle; beta tag
pushes are handled once by the main release workflow.

## Runtime evidence without image rebuilding

Candidate builds stamp `internal/consts.BuildCommit` alongside Version. Embedded
connector records remain historical: they cannot certify another source or
executable. Set `ETL_CONNECTOR_EVIDENCE_FILE` to a read-only copy of the generated
`connector-evidence.json`, typically mounted outside the image. Its source and
`certified_binary_sha256` must match the running candidate; every record carries
the same binary SHA. Invalid/unreadable/mismatched external evidence is a missing
readiness gate and never falls back to a verified embedded record.

Legacy builds without a source stamp remain readable. An explicitly supplied
external certificate still must match their actual executable hash. This changes
readiness facts only; connector maturity, checkpoint, DLQ and runtime delivery
semantics are unchanged.

## Local verification

Use an isolated, clean source worktree, Go 1.24.13, Node/npm and the repository's
container selection. GoReleaser must be 1.26.2. On Podman hosts its Docker CLI calls
can use a task-owned shim that forwards to the selected CLI; all asset operations
still use `CONTAINER_CLI`.

    GORELEASER=/path/to/goreleaser CONTAINER_CLI=podman python3 hack/release_candidate.py build --dir /tmp/owned-candidate --snapshot --arch arm64
    python3 hack/release_candidate.py run --dir /tmp/owned-candidate --name hack/e2e.sh

Run every mandatory case, then:

    python3 hack/release_candidate.py emit-evidence --dir /tmp/owned-candidate
    python3 hack/release_candidate.py run --dir /tmp/owned-candidate --name readiness
    python3 hack/release_candidate.py collect --dir /tmp/owned-candidate
    python3 hack/release_candidate.py bundle --dir /tmp/owned-candidate

The promotion function can be verified against a task-owned local registry with
`--insecure` on Podman; it requires the same complete certificate as public
promotion. No public tag/release is necessary to verify this technical contract.

Actual candidate execution and promotion evidence for this P1 window is recorded
in the iteration's GA acceptance matrix. This document is the implementation
contract and does not claim that pending certification has passed.
