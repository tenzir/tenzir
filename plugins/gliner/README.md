# GLiNER plugin

Provides the `ai::entities` operator: zero-shot named entity recognition
(e.g., PII detection) using GLiNER span-level models via ONNX Runtime.
Inference runs in-process on the CPU; no data leaves the node.

## Usage

```tql
from {message: "Alice logged in from 203.0.113.4 using alice@example.com"}
ai::entities field=message,
             model="/var/lib/tenzir/models/gliner-pii",
             labels=["name", "email address", "ip address"],
             threshold=0.5,
             into=pii
```

Output: `pii` is a list of records `{text, label, start, end, score}` with
UTF-8 byte offsets into the input string.

Events are processed in batches of `batch_size` (default: 16) per inference
call; inference runs on the blocking thread pool so pipeline scheduling is
never stalled.

## Metrics

The operator emits `tenzir.metrics.ai_entities` records with `events`,
`chars`, `entities`, `truncated`, `errors` counters and the cumulative
`inference_time` per interval.

## Model setup

Download a span-level GLiNER v1 model directory. The recommended model is
[knowledgator/gliner-pii-base-v1.0](https://huggingface.co/knowledgator/gliner-pii-base-v1.0)
(Apache 2.0):

```sh
hf download knowledgator/gliner-pii-base-v1.0 \
  onnx/model.onnx spm.model added_tokens.json gliner_config.json \
  --local-dir /var/lib/tenzir/models/gliner-pii
```

The model directory must contain:

- `onnx/model.onnx` (or `model.onnx`) — span-level (markerV0) GLiNER export
- `spm.model` — SentencePiece tokenizer (DeBERTa-based models)
- `added_tokens.json` — IDs of the `<<ENT>>`/`<<SEP>>` marker tokens
- `gliner_config.json` — model configuration

For best results use the label names the model was trained on (for
knowledgator models: `name`, `email address`, `ip address`, `username`,
`password`, `credit card`, `phone number`, ...); zero-shot labels work but
score lower.

## Build requirements

ONNX Runtime and SentencePiece. The plugin disables itself with a warning
when either is missing. Both are provided by the Nix devshell.

Static builds do not include this plugin: neither ONNX Runtime nor
SentencePiece currently build in `pkgsStatic`.

## ASan note

In sanitizer builds, set `ASAN_OPTIONS=detect_container_overflow=0` when
running pipelines that use `ai::entities`. SentencePiece from Nix is not
ASan-instrumented, so its writes into `std::vector` trigger container-overflow
false positives (same mixed-instrumentation caveat as the `to_iceberg`
plugin).

## Testing

Unit-level validation is done against golden data captured from the
reference Python implementation (see `plan.md` in the repository root while
this feature is in development). Integration tests require a local model:

```sh
hf download knowledgator/gliner-pii-base-v1.0 \
  onnx/model.onnx spm.model added_tokens.json gliner_config.json \
  --local-dir /tmp/gliner-pii
tenzir 'from {m: "mail bob@example.org"}
        ai::entities field=m, model="/tmp/gliner-pii", labels=["email address"]
        write_json'
```
