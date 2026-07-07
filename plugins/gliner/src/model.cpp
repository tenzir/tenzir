//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// GLiNER span-level (markerV0) inference, validated bit-exactly against the
// reference Python implementation (see plan.md, "Smoke test result"). The
// pipeline is:
//
//   words <- regex word split (offsets recorded)
//   input <- [CLS] <<ENT>> label ... <<SEP>> word-subwords... [SEP]
//   logits <- ONNX session (input_ids, attention_mask, words_mask,
//             text_lengths, span_idx, span_mask)
//   spans <- sigmoid + threshold + greedy non-overlap, word indices mapped
//            back to byte offsets via the recorded word positions

#include "gliner/model.hpp"

#include "gliner/detail.hpp"

#include <tenzir/detail/assert.hpp>

#include <caf/error.hpp>
#include <caf/sec.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <re2/re2.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <fstream>
#include <onnxruntime_cxx_api.h>
#include <ranges>
#include <sentencepiece_processor.h>
#include <simdjson.h>
#include <sstream>

namespace tenzir::plugins::gliner {

namespace detail {

auto split_words(std::string_view text) -> std::vector<Word> {
  static const auto pattern
    = re2::RE2{R"(([\pL\pN_]+(?:[-_][\pL\pN_]+)*|[^\s]))"};
  auto words = std::vector<Word>{};
  auto input = re2::StringPiece{text.data(), text.size()};
  auto match = re2::StringPiece{};
  while (re2::RE2::FindAndConsume(&input, pattern, &match)) {
    auto begin = static_cast<size_t>(match.data() - text.data());
    words.push_back({std::string{match}, begin, begin + match.size()});
  }
  return words;
}

auto decode_spans(std::span<const float> logits, int64_t num_words,
                  int64_t max_width, size_t num_labels, double threshold)
  -> std::vector<WordSpan> {
  auto candidates = std::vector<WordSpan>{};
  for (auto i = int64_t{0}; i < num_words; ++i) {
    for (auto w = int64_t{0}; w < max_width; ++w) {
      if (i + w >= num_words) {
        continue;
      }
      for (auto c = size_t{0}; c < num_labels; ++c) {
        auto logit
          = logits[(static_cast<size_t>(i * max_width + w)) * num_labels + c];
        auto prob = 1.0 / (1.0 + std::exp(-static_cast<double>(logit)));
        if (prob >= threshold) {
          candidates.push_back({i, i + w, c, prob});
        }
      }
    }
  }
  std::stable_sort(candidates.begin(), candidates.end(),
                   [](const WordSpan& a, const WordSpan& b) {
                     return a.score > b.score;
                   });
  auto kept = std::vector<WordSpan>{};
  for (const auto& candidate : candidates) {
    auto overlaps
      = std::any_of(kept.begin(), kept.end(), [&](const WordSpan& k) {
          return candidate.start_word <= k.end_word
                 and k.start_word <= candidate.end_word;
        });
    if (not overlaps) {
      kept.push_back(candidate);
    }
  }
  std::sort(kept.begin(), kept.end(), [](const WordSpan& a, const WordSpan& b) {
    return a.start_word < b.start_word;
  });
  return kept;
}

} // namespace detail

namespace {

auto read_file(const std::filesystem::path& path)
  -> caf::expected<std::string> {
  auto in = std::ifstream{path};
  if (not in) {
    return caf::make_error(caf::sec::runtime_error,
                           fmt::format("failed to open {}", path.string()));
  }
  auto ss = std::stringstream{};
  ss << in.rdbuf();
  return std::move(ss).str();
}

} // namespace

struct Model::Impl {
  Ort::Env env{ORT_LOGGING_LEVEL_ERROR, "tenzir-gliner"};
  std::unique_ptr<Ort::Session> session;
  sentencepiece::SentencePieceProcessor sp;
  int64_t ent_token_id = 0;
  int64_t sep_token_id = 0;
  int64_t cls_id = 1;
  int64_t eos_id = 2;
  int64_t max_width = 12;
  int64_t max_words = 2048;
};

Model::Model(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {
}

Model::~Model() = default;

auto Model::make(const std::filesystem::path& dir)
  -> caf::expected<std::unique_ptr<Model>> {
  auto impl = std::make_unique<Impl>();
  auto onnx = dir / "onnx" / "model.onnx";
  if (not std::filesystem::exists(onnx)) {
    onnx = dir / "model.onnx";
  }
  if (not std::filesystem::exists(onnx)) {
    return caf::make_error(
      caf::sec::runtime_error,
      fmt::format("no `onnx/model.onnx` or `model.onnx` in {}", dir.string()));
  }
  try {
    impl->session = std::make_unique<Ort::Session>(impl->env, onnx.c_str(),
                                                   Ort::SessionOptions{});
  } catch (const Ort::Exception& e) {
    return caf::make_error(caf::sec::runtime_error,
                           fmt::format("failed to load {}: {}", onnx.string(),
                                       e.what()));
  }
  // Pin the supported ONNX signature; anything else is a different GLiNER
  // architecture that we must not silently mis-run.
  auto expected_inputs
    = std::vector<std::string>{"input_ids",    "attention_mask", "words_mask",
                               "text_lengths", "span_idx",       "span_mask"};
  auto allocator = Ort::AllocatorWithDefaultOptions{};
  auto actual_inputs = std::vector<std::string>{};
  for (auto i = size_t{0}; i < impl->session->GetInputCount(); ++i) {
    actual_inputs.emplace_back(
      impl->session->GetInputNameAllocated(i, allocator).get());
  }
  if (actual_inputs != expected_inputs) {
    return caf::make_error(caf::sec::runtime_error,
                           fmt::format("unsupported ONNX signature (inputs: "
                                       "{}); expected a "
                                       "span-level GLiNER v1 model",
                                       fmt::join(actual_inputs, ", ")));
  }
  auto status = impl->sp.Load((dir / "spm.model").string());
  if (not status.ok()) {
    return caf::make_error(caf::sec::runtime_error,
                           fmt::format("failed to load {}: {}",
                                       (dir / "spm.model").string(),
                                       status.ToString()));
  }
  auto config_json = read_file(dir / "gliner_config.json");
  if (not config_json) {
    return std::move(config_json.error());
  }
  auto ent_token = std::string{"<<ENT>>"};
  auto sep_token = std::string{"<<SEP>>"};
  {
    auto parser = simdjson::dom::parser{};
    auto doc = parser.parse(simdjson::pad(*config_json));
    if (doc.error()) {
      return caf::make_error(caf::sec::runtime_error,
                             "failed to parse gliner_config.json");
    }
    if (auto v = doc["span_mode"].get_string(); not v.error()) {
      if (v.value() != "markerV0") {
        return caf::make_error(
          caf::sec::runtime_error,
          fmt::format("unsupported span_mode `{}`; only span-level "
                      "(markerV0) GLiNER models are supported",
                      std::string_view{v.value()}));
      }
    }
    if (auto v = doc["max_width"].get_int64(); not v.error()) {
      impl->max_width = v.value();
    }
    if (auto v = doc["max_len"].get_int64(); not v.error()) {
      impl->max_words = v.value();
    }
    if (auto v = doc["ent_token"].get_string(); not v.error()) {
      ent_token = std::string{v.value()};
    }
    if (auto v = doc["sep_token"].get_string(); not v.error()) {
      sep_token = std::string{v.value()};
    }
  }
  auto added_json = read_file(dir / "added_tokens.json");
  if (not added_json) {
    return std::move(added_json.error());
  }
  {
    auto parser = simdjson::dom::parser{};
    auto doc = parser.parse(simdjson::pad(*added_json));
    auto ent = doc[ent_token].get_int64();
    auto sep = doc[sep_token].get_int64();
    if (doc.error() or ent.error() or sep.error()) {
      return caf::make_error(caf::sec::runtime_error,
                             fmt::format("added_tokens.json does not define "
                                         "`{}` and `{}`",
                                         ent_token, sep_token));
    }
    impl->ent_token_id = ent.value();
    impl->sep_token_id = sep.value();
  }
  return std::unique_ptr<Model>{new Model{std::move(impl)}};
}

auto Model::detect(std::string_view text, std::span<const std::string> labels,
                   double threshold) -> caf::expected<DetectResult> {
  auto texts = std::array<std::string_view, 1>{text};
  auto results = detect_batch(texts, labels, threshold);
  if (not results) {
    return std::move(results.error());
  }
  TENZIR_ASSERT(results->size() == 1);
  return std::move(results->front());
}

auto Model::detect_batch(std::span<const std::string_view> texts,
                         std::span<const std::string> labels, double threshold)
  -> caf::expected<std::vector<DetectResult>> {
  auto results = std::vector<DetectResult>(texts.size());
  if (texts.empty() or labels.empty()) {
    return results;
  }
  // Split and truncate per row.
  auto batch_words = std::vector<std::vector<detail::Word>>{};
  batch_words.reserve(texts.size());
  for (auto row = size_t{0}; row < texts.size(); ++row) {
    auto words = detail::split_words(texts[row]);
    if (std::cmp_greater(words.size(), impl_->max_words)) {
      words.resize(static_cast<size_t>(impl_->max_words));
      results[row].truncated = true;
    }
    batch_words.push_back(std::move(words));
  }
  if (std::ranges::all_of(batch_words, [](const auto& words) {
        return words.empty();
      })) {
    return results;
  }
  // Encode the (identical) label prompt once, then each row's text.
  auto prompt_ids = std::vector<int64_t>{};
  auto subwords = std::vector<int>{};
  prompt_ids.push_back(impl_->cls_id);
  for (const auto& label : labels) {
    prompt_ids.push_back(impl_->ent_token_id);
    subwords.clear();
    impl_->sp.Encode(label, &subwords).IgnoreError();
    prompt_ids.insert(prompt_ids.end(), subwords.begin(), subwords.end());
  }
  prompt_ids.push_back(impl_->sep_token_id);
  auto row_ids = std::vector<std::vector<int64_t>>{};
  auto row_marks = std::vector<std::vector<int64_t>>{};
  row_ids.reserve(texts.size());
  row_marks.reserve(texts.size());
  for (const auto& words : batch_words) {
    auto ids = prompt_ids;
    auto marks = std::vector<int64_t>(prompt_ids.size(), 0);
    auto word_counter = int64_t{0};
    for (const auto& word : words) {
      subwords.clear();
      impl_->sp.Encode(word.text, &subwords).IgnoreError();
      ++word_counter;
      auto first = true;
      for (auto id : subwords) {
        ids.push_back(id);
        marks.push_back(first ? word_counter : 0);
        first = false;
      }
    }
    ids.push_back(impl_->eos_id);
    marks.push_back(0);
    row_ids.push_back(std::move(ids));
    row_marks.push_back(std::move(marks));
  }
  // Pad to batch tensors. DeBERTa uses pad id 0; padding is inert through
  // the attention mask.
  auto batch = static_cast<int64_t>(texts.size());
  auto seq = int64_t{0};
  auto max_words = int64_t{0};
  for (auto row = size_t{0}; row < texts.size(); ++row) {
    seq = std::max(seq, static_cast<int64_t>(row_ids[row].size()));
    max_words
      = std::max(max_words, static_cast<int64_t>(batch_words[row].size()));
  }
  auto num_spans = max_words * impl_->max_width;
  auto input_ids = std::vector<int64_t>{};
  auto attention_mask = std::vector<int64_t>{};
  auto words_mask = std::vector<int64_t>{};
  auto text_lengths = std::vector<int64_t>{};
  auto span_idx = std::vector<int64_t>{};
  auto span_mask = std::vector<uint8_t>{};
  input_ids.reserve(static_cast<size_t>(batch * seq));
  attention_mask.reserve(static_cast<size_t>(batch * seq));
  words_mask.reserve(static_cast<size_t>(batch * seq));
  span_idx.reserve(static_cast<size_t>(batch * num_spans * 2));
  span_mask.reserve(static_cast<size_t>(batch * num_spans));
  for (auto row = size_t{0}; row < texts.size(); ++row) {
    const auto& ids = row_ids[row];
    input_ids.insert(input_ids.end(), ids.begin(), ids.end());
    input_ids.resize(input_ids.size() + static_cast<size_t>(seq) - ids.size(),
                     0);
    attention_mask.insert(attention_mask.end(), ids.size(), 1);
    attention_mask.resize(
      attention_mask.size() + static_cast<size_t>(seq) - ids.size(), 0);
    const auto& marks = row_marks[row];
    words_mask.insert(words_mask.end(), marks.begin(), marks.end());
    words_mask.resize(
      words_mask.size() + static_cast<size_t>(seq) - marks.size(), 0);
    auto row_words = static_cast<int64_t>(batch_words[row].size());
    text_lengths.push_back(row_words);
    for (auto i = int64_t{0}; i < max_words; ++i) {
      for (auto w = int64_t{0}; w < impl_->max_width; ++w) {
        span_idx.push_back(i);
        span_idx.push_back(i + w);
        span_mask.push_back(i + w < row_words ? 1 : 0);
      }
    }
  }
  // Run inference.
  auto logits = std::vector<float>{};
  try {
    auto mem = Ort::MemoryInfo::CreateCpu(OrtArenaAllocator, OrtMemTypeDefault);
    auto make_i64 = [&](std::vector<int64_t>& data,
                        std::vector<int64_t> shape) {
      return Ort::Value::CreateTensor<int64_t>(mem, data.data(), data.size(),
                                               shape.data(), shape.size());
    };
    auto span_mask_shape = std::vector<int64_t>{batch, num_spans};
    auto values = std::vector<Ort::Value>{};
    values.push_back(make_i64(input_ids, {batch, seq}));
    values.push_back(make_i64(attention_mask, {batch, seq}));
    values.push_back(make_i64(words_mask, {batch, seq}));
    values.push_back(make_i64(text_lengths, {batch, 1}));
    values.push_back(make_i64(span_idx, {batch, num_spans, 2}));
    values.push_back(Ort::Value::CreateTensor(
      mem, reinterpret_cast<bool*>(span_mask.data()), span_mask.size(),
      span_mask_shape.data(), span_mask_shape.size(),
      ONNX_TENSOR_ELEMENT_DATA_TYPE_BOOL));
    constexpr const char* input_names[]
      = {"input_ids",    "attention_mask", "words_mask",
         "text_lengths", "span_idx",       "span_mask"};
    constexpr const char* output_names[] = {"logits"};
    auto outputs
      = impl_->session->Run(Ort::RunOptions{nullptr}, input_names,
                            values.data(), values.size(), output_names, 1);
    const auto* data = outputs[0].GetTensorData<float>();
    auto count = outputs[0].GetTensorTypeAndShapeInfo().GetElementCount();
    logits.assign(data, data + count);
  } catch (const Ort::Exception& e) {
    return caf::make_error(caf::sec::runtime_error,
                           fmt::format("inference failed: {}", e.what()));
  }
  // Decode per row: sigmoid, threshold, then greedy non-overlap (flat NER).
  auto row_stride = static_cast<size_t>(num_spans) * labels.size();
  TENZIR_ASSERT(logits.size() == static_cast<size_t>(batch) * row_stride);
  for (auto row = size_t{0}; row < texts.size(); ++row) {
    const auto& words = batch_words[row];
    auto row_logits
      = std::span<const float>{logits.data() + row * row_stride, row_stride};
    auto kept
      = detail::decode_spans(row_logits, static_cast<int64_t>(words.size()),
                             impl_->max_width, labels.size(), threshold);
    for (const auto& span : kept) {
      auto begin = words[static_cast<size_t>(span.start_word)].begin;
      auto end = words[static_cast<size_t>(span.end_word)].end;
      results[row].spans.push_back({
        static_cast<int64_t>(begin),
        static_cast<int64_t>(end),
        std::string{texts[row].substr(begin, end - begin)},
        labels[span.label],
        span.score,
      });
    }
  }
  return results;
}

} // namespace tenzir::plugins::gliner
