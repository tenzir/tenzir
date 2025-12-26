# Tenzir CI/CD Architecture

This document describes the GitHub Actions workflow architecture for the Tenzir
repository.

## Design Principles

1. **Native path filtering** - Use `on.push.paths` / `on.pull_request.paths`
2. **Self-contained workflows** - Each computes its own metadata locally
3. **Single-purpose** - One workflow per concern
4. **Workflow-agnostic caching** - Cache keys survive workflow renames

## Architecture

```
                    ┌──────────────────────────────────────────────────┐
                    │              GitHub Events                        │
                    │   push | pull_request | release | workflow_dispatch│
                    └─────────────────────┬────────────────────────────┘
                                          │
        ┌─────────────────────────────────┼─────────────────────────────┐
        │                                 │                             │
        ▼                                 ▼                             ▼
┌───────────────────┐           ┌─────────────────┐             ┌───────────────┐
│     nix.yaml      │           │  docker.yaml    │             │ python.yaml   │
│                   │           │                 │             │               │
│ paths:            │           │ paths:          │             │ paths:        │
│  libtenzir/       │           │  Dockerfile     │             │  python/      │
│  plugins/         │           │  libtenzir/     │             └───────────────┘
│  nix/             │           │  scripts/debian │
│                   │           └────────┬────────┘
│ ┌───────────────┐ │                    │
│ │ static-arm64  │ │                    │
│ │ static-x86_64 │ │                    │ workflow_run
│ │ static-darwin │ │                    ▼
│ │      ↓        │ │           ┌─────────────────┐
│ │ push-manifest │ │           │ regression.yaml │
│ └───────────────┘ │           └────────┬────────┘
└───────────────────┘                    │
                                         │ workflow_run (main only)
                                         ▼
                                ┌─────────────────┐
                                │ sync-openapi.yaml│
                                └────────┬────────┘
                                         │
        ─────────────────────────────────┼──────────────────────────────
                                         │
                                         ▼
                                ┌─────────────────┐
                                │   gate.yaml     │
                                │ (branch protect)│
                                └─────────────────┘
```

## Workflow Files

| File                | Purpose                           | Trigger                                        |
| ------------------- | --------------------------------- | ---------------------------------------------- |
| `nix.yaml`          | Nix static builds + push-manifest | paths: libtenzir/, plugins/, nix/              |
| `docker.yaml`       | Docker image builds               | paths: Dockerfile, libtenzir/, scripts/debian/ |
| `regression.yaml`   | Regression tests matrix           | workflow_run: Docker                           |
| `sync-openapi.yaml` | Docs repo sync                    | workflow_run: Docker (main only)               |
| `macos.yaml`        | macOS C++ build                   | paths: libtenzir/, plugins/, tenzir/           |
| `python.yaml`       | Python tests                      | paths: python/                                 |
| `gate.yaml`         | Branch protection aggregator      | workflow_run: all                              |
| `release.yaml`      | Release workflow                  | release: published                             |

### Reusable Workflows

| File                  | Purpose                      |
| --------------------- | ---------------------------- |
| `_docker.yaml`        | Reusable Docker build logic  |
| `_nix-build.yaml`     | Reusable Nix build logic     |
| `_push-manifest.yaml` | Reusable manifest push logic |

Convention: `_prefix.yaml` indicates a reusable workflow (called via
`workflow_call`).

## Caching Strategy

| Cache             | Mechanism                                 | Cross-Workflow |
| ----------------- | ----------------------------------------- | -------------- |
| **Nix binaries**  | Cachix (`tenzir` namespace)               | Yes            |
| **Docker layers** | Registry (`ghcr.io/tenzir/*-build-cache`) | Yes            |
| **ccache**        | GitHub Actions cache                      | No             |
| **pip**           | `setup-python` managed                    | No             |

### Cache Key Pattern

ccache uses workflow-agnostic keys:

```yaml
key: ccache-${{ matrix.name }}-${{ matrix.compiler }}-${{ github.ref_name }}-${{ github.sha }}
restore-keys: |
  ccache-${{ matrix.name }}-${{ matrix.compiler }}-${{ github.ref_name }}
  ccache-${{ matrix.name }}-${{ matrix.compiler }}-main
  ccache-${{ matrix.name }}-${{ matrix.compiler }}
```

## Current Status

**Branch:** `topic/ci-revamp`

All workflow files have been created and the monolithic `tenzir.yaml` has been
replaced. The branch is ready for testing and review.

### What Changed

- **Deleted:** `tenzir.yaml`, `configure_helpers.bash`
- **Created:** `nix.yaml`, `docker.yaml`, `macos.yaml`, `python.yaml`,
  `regression.yaml`, `sync-openapi.yaml`, `gate.yaml`
- **Modified:** `docker-manual.yaml` (updated to use `_docker.yaml`)

### Next Steps

1. Create a PR from `topic/ci-revamp` to `main`
2. Verify workflows trigger correctly on the PR
3. Update branch protection rules in GitHub settings after merge
4. Monitor for edge cases during initial runs

## Migration Checklist

Track progress of the CI modernization:

### Phase 1: Setup

- [x] Create this README.md
- [x] Rename reusable workflows (`docker.yaml` → `_docker.yaml`, etc.)

### Phase 2: Core Workflows

- [x] Create `nix.yaml` - extract static build jobs + push-manifest-slim
- [x] Create `docker.yaml` - extract docker build job
- [x] Create `regression.yaml` - workflow_run after Docker
- [x] Create `sync-openapi.yaml` - workflow_run after Docker (main only)
- [x] Create `macos.yaml` - extract tenzir job
- [x] Create `python.yaml` - extract python job

### Phase 3: Finalization

- [x] Create `gate.yaml` - branch protection aggregator
- [x] Update ccache keys to be workflow-agnostic
- [x] Delete `tenzir.yaml`
- [x] Delete `configure_helpers.bash`
- [ ] Update branch protection rules in GitHub settings
- [ ] Verify all workflows trigger correctly
- [ ] Document any edge cases discovered

## Testing

| Change            | Workflows Triggered             |
| ----------------- | ------------------------------- |
| `python/` only    | python                          |
| `libtenzir/` only | nix, docker, macos → regression |
| `Dockerfile` only | docker → regression             |
| `nix/` only       | nix → manifest                  |
| Push to main      | All workflows                   |
| Release           | All workflows                   |
