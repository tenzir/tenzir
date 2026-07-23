"""Generate the tiny GLiNER fixture model for ai::entities integration tests.

The ONNX graph has the exact I/O signature of a span-level GLiNER v1 export
but trivial, deterministic behavior: every word receives a single-word span
(width 1) with logit 3.0 (prob ~0.9526) for every label; all other widths get
logit -10. With the operator's greedy overlap resolution this yields exactly
one span per word, labeled with the FIRST requested label.

Usage: python make_fixture_model.py <output_dir>
"""

import io
import json
import sys
from pathlib import Path

import onnx
import sentencepiece as spm
from onnx import TensorProto, helper

ENT_ID = 128001
SEP_ID = 128002
MAX_WIDTH = 12
HIGH = 3.0  # sigmoid(3.0) = 0.95257
LOW = -10.0

out = Path(sys.argv[1])
out.mkdir(parents=True, exist_ok=True)

# --- tiny sentencepiece model (vocab 64, trained on a mini corpus) ---------
corpus = "\n".join(
    [
        "alice bob carol dave visited berlin paris london",
        "login failed for user from host and port",
        "email address ip name username password key",
        "the quick brown fox jumps over the lazy dog",
        "0 1 2 3 4 5 6 7 8 9 . : @ - _ / =",
    ]
)
model_writer = io.BytesIO()
spm.SentencePieceTrainer.train(
    sentence_iterator=iter(corpus.splitlines()),
    model_writer=model_writer,
    vocab_size=64,
    model_type="unigram",
    hard_vocab_limit=False,
)
(out / "spm.model").write_bytes(model_writer.getvalue())

# --- ONNX graph -------------------------------------------------------------
i64, f32, b8 = TensorProto.INT64, TensorProto.FLOAT, TensorProto.BOOL
inputs = [
    helper.make_tensor_value_info("input_ids", i64, ["batch", "seq"]),
    helper.make_tensor_value_info("attention_mask", i64, ["batch", "seq"]),
    helper.make_tensor_value_info("words_mask", i64, ["batch", "seq"]),
    helper.make_tensor_value_info("text_lengths", i64, ["batch", 1]),
    helper.make_tensor_value_info("span_idx", i64, ["batch", "spans", 2]),
    helper.make_tensor_value_info("span_mask", b8, ["batch", "spans"]),
]
output = helper.make_tensor_value_info(
    "logits", f32, ["batch", "words", MAX_WIDTH, "classes"]
)
base = helper.make_tensor(
    "base_values", f32, [1, 1, MAX_WIDTH, 1], [HIGH] + [LOW] * (MAX_WIDTH - 1)
)
nodes = [
    # B: Shape(input_ids)[0:1]
    helper.make_node("Shape", ["input_ids"], ["ids_shape"]),
    helper.make_node("Slice", ["ids_shape", "zero", "one_v", "zero"], ["b_vec"]),
    # T: Shape(span_idx)[1:2] / MAX_WIDTH (spans = words * MAX_WIDTH)
    helper.make_node("Shape", ["span_idx"], ["span_shape"]),
    helper.make_node("Slice", ["span_shape", "one_v", "two_v", "zero"], ["s_vec"]),
    helper.make_node("Div", ["s_vec", "width_v"], ["t_vec"]),
    # C: count <<ENT>> occurrences in the first row (the labels prompt is
    # identical across the batch).
    helper.make_node("Slice", ["input_ids", "zero", "one_v", "zero"], ["row0"]),
    helper.make_node("Equal", ["row0", "ent_scalar"], ["is_ent"]),
    helper.make_node("Cast", ["is_ent"], ["is_ent_i64"], to=i64),
    helper.make_node(
        "ReduceSum", ["is_ent_i64"], ["c_raw"], keepdims=0, noop_with_empty_axes=0
    ),
    helper.make_node("Reshape", ["c_raw", "neg_one"], ["c_vec"]),
    # target shape (B, T, MAX_WIDTH, C)
    helper.make_node(
        "Concat", ["b_vec", "t_vec", "width_v", "c_vec"], ["shape"], axis=0
    ),
    helper.make_node("Expand", ["base_values", "shape"], ["logits"]),
]
graph = helper.make_graph(
    nodes,
    "gliner_fixture",
    inputs,
    [output],
    initializer=[
        base,
        helper.make_tensor("zero", i64, [1], [0]),
        helper.make_tensor("one_v", i64, [1], [1]),
        helper.make_tensor("two_v", i64, [1], [2]),
        helper.make_tensor("width_v", i64, [1], [MAX_WIDTH]),
        helper.make_tensor("neg_one", i64, [1], [-1]),
        helper.make_tensor("ent_scalar", i64, [], [ENT_ID]),
    ],
)
model = helper.make_model(
    graph, opset_imports=[helper.make_opsetid("", 17)], ir_version=8
)
onnx.checker.check_model(model)
onnx.save(model, out / "model.onnx")

# --- config files -----------------------------------------------------------
(out / "gliner_config.json").write_text(
    json.dumps(
        {
            "model_type": "gliner",
            "span_mode": "markerV0",
            "max_width": MAX_WIDTH,
            "max_len": 2048,
            "ent_token": "<<ENT>>",
            "sep_token": "<<SEP>>",
            "words_splitter_type": "whitespace",
        },
        indent=2,
    )
    + "\n"
)
(out / "added_tokens.json").write_text(
    json.dumps({"<<ENT>>": ENT_ID, "<<SEP>>": SEP_ID}, indent=2) + "\n"
)

for f in sorted(out.iterdir()):
    print(f"{f.name}: {f.stat().st_size} bytes")
