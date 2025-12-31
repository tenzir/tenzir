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
                       ┌─────────────────────────────────────────────┐
                       │               GitHub Events                 │
                       │     push | pull_request | release           │
                       └──────────────────────┬──────────────────────┘
                                              │
        ┌─────────────┬─────────────┬─────────┴───────┬─────────────┐
        │             │             │                 │             │
        ▼             ▼             ▼                 ▼             ▼
  ┌───────────┐ ┌───────────┐ ┌───────────┐   ┌───────────┐ ┌───────────┐
  │ nix.yaml  │ │docker.yaml│ │macos.yaml │   │python.yaml│ │release.yml│
  │           │ │           │ │           │   │           │ │           │
  │  paths:   │ │  paths:   │ │  paths:   │   │  paths:   │ │    on:    │
  │ libtenzir │ │ Dockerfile│ │ libtenzir │   │  python/  │ │  release  │
  │  plugins  │ │ libtenzir │ │  plugins  │   └───────────┘ └───────────┘
  │    nix    │ │  scripts  │ │  tenzir   │
  │     │     │ └─────┬─────┘ └───────────┘
  │     ▼     │       │
  │  manifest │       │ workflow_run (on Docker success)
  └───────────┘       │
                      ├───────────────────┐
                      │                   │
                      ▼                   ▼
              ┌────────────────┐  ┌────────────────┐
              │regression.yaml │  │sync-openapi.yml│
              │                │  │                │
              │ version matrix │  │ triggers docs  │
              │   v5.16.0+     │  │  repo update   │
              │                │  │                │
              │  (main/v*)     │  │  (main only)   │
              └────────────────┘  └────────────────┘
```

## Workflow Files

| File                | Purpose                           | Trigger                                        |
| ------------------- | --------------------------------- | ---------------------------------------------- |
| `nix.yaml`          | Nix static builds + push-manifest | paths: libtenzir/, plugins/, nix/              |
| `docker.yaml`       | Docker image builds               | paths: Dockerfile, libtenzir/, scripts/debian/ |
| `macos.yaml`        | macOS C++ build                   | paths: libtenzir/, plugins/, tenzir/           |
| `python.yaml`       | Python tests and packages         | paths: python/                                 |
| `regression.yaml`   | Regression tests matrix           | workflow_run: Docker (main/v\* only)           |
| `sync-openapi.yaml` | Docs repo sync                    | workflow_run: Docker (main only)               |
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

ccache uses workflow-agnostic keys so they survive workflow renames:

```yaml
key: ccache-${{ matrix.name }}-${{ matrix.compiler }}-${{ github.ref_name }}-${{ github.sha }}
restore-keys: |
  ccache-${{ matrix.name }}-${{ matrix.compiler }}-${{ github.ref_name }}
  ccache-${{ matrix.name }}-${{ matrix.compiler }}-main
  ccache-${{ matrix.name }}-${{ matrix.compiler }}
```

## Branch Protection

Configure GitHub branch protection to require the following status checks:

- `Build / Build (amd64)`
- `Build / Build (arm64)`
- `Nix (x86_64-linux) / tenzir-static-x86_64-linux`
- `Nix (aarch64-linux) / tenzir-static-aarch64-linux`
- `macOS`
- `Python Tests (3.12)`
- `Style Check`

## Path Filtering Examples

| Change            | Workflows Triggered          |
| ----------------- | ---------------------------- |
| `python/` only    | python                       |
| `libtenzir/` only | nix, docker, macos           |
| `Dockerfile` only | docker                       |
| `nix/` only       | nix                          |
| Push to main      | All path-triggered workflows |
| Release           | All workflows                |

Note: `regression.yaml` and `sync-openapi.yaml` run via `workflow_run` after
Docker completes, not directly from path triggers.
