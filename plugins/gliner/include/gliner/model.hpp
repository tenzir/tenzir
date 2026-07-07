//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/expected.hpp>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace tenzir::plugins::gliner {

/// A detected entity within a string. Offsets are UTF-8 byte offsets into
/// the input.
struct EntitySpan {
  int64_t start = 0;
  int64_t end = 0;
  std::string text;
  std::string label;
  double score = 0.0;
};

struct DetectResult {
  std::vector<EntitySpan> spans;
  /// True if the input exceeded the model window and was truncated.
  bool truncated = false;
};

/// A loaded GLiNER span-level (markerV0) model: ONNX session, SentencePiece
/// tokenizer, and model configuration. Not thread-safe; use one instance per
/// operator.
class Model {
public:
  /// Loads a model directory containing `onnx/model.onnx` (or `model.onnx`),
  /// `spm.model`, `added_tokens.json`, and `gliner_config.json`.
  static auto make(const std::filesystem::path& dir)
    -> caf::expected<std::unique_ptr<Model>>;

  ~Model();
  Model(const Model&) = delete;
  auto operator=(const Model&) -> Model& = delete;

  /// Runs entity detection for one text against the given labels.
  auto detect(std::string_view text, std::span<const std::string> labels,
              double threshold) -> caf::expected<DetectResult>;

private:
  struct Impl;
  explicit Model(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

} // namespace tenzir::plugins::gliner
