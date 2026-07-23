//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace tenzir::plugins::gliner {

/// Minimal wrapper around the SentencePiece processor. Lives in its own
/// translation unit because the sentencepiece header aliases
/// `absl::string_view` to `std::string_view` and breaks (at compile and link
/// time) in any translation unit that also sees the real Abseil, which the
/// shared plugin precompiled header pulls in on some platforms.
class Tokenizer {
public:
  /// Loads an `spm.model` file. Returns an error message, or an empty string
  /// on success.
  static auto make(const std::string& model_path)
    -> std::pair<std::unique_ptr<Tokenizer>, std::string>;

  ~Tokenizer();
  Tokenizer(const Tokenizer&) = delete;
  auto operator=(const Tokenizer&) -> Tokenizer& = delete;

  /// Appends the subword IDs of `text` to `ids`.
  auto encode(std::string_view text, std::vector<int>& ids) const -> void;

private:
  struct Impl;
  explicit Tokenizer(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

} // namespace tenzir::plugins::gliner
