//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// This translation unit is excluded from the shared plugin precompiled
// header and must not include anything that declares the real Abseil: the
// sentencepiece header aliases `absl::string_view` to `std::string_view`,
// which is both ambiguous at compile time and an ABI mismatch at link time
// when the real Abseil is visible.

#include "gliner/tokenizer.hpp"

#include <sentencepiece_processor.h>
#include <utility>

namespace tenzir::plugins::gliner {

struct Tokenizer::Impl {
  sentencepiece::SentencePieceProcessor sp;
};

Tokenizer::Tokenizer(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {
}

Tokenizer::~Tokenizer() = default;

auto Tokenizer::make(const std::string& model_path)
  -> std::pair<std::unique_ptr<Tokenizer>, std::string> {
  auto impl = std::make_unique<Impl>();
  auto status = impl->sp.Load(model_path);
  if (not status.ok()) {
    return {nullptr, status.ToString()};
  }
  return {std::unique_ptr<Tokenizer>{new Tokenizer{std::move(impl)}}, {}};
}

auto Tokenizer::encode(std::string_view text, std::vector<int>& ids) const
  -> void {
  auto pieces = std::vector<int>{};
  impl_->sp.Encode(text, &pieces).IgnoreError();
  ids.insert(ids.end(), pieces.begin(), pieces.end());
}

} // namespace tenzir::plugins::gliner
