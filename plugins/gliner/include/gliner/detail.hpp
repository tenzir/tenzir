//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace tenzir::plugins::gliner::detail {

/// A word produced by the GLiNER word splitter, with byte offsets into the
/// original string.
struct Word {
  std::string text;
  size_t begin = 0;
  size_t end = 0;

  friend auto operator==(const Word&, const Word&) -> bool = default;
};

/// Splits text like the GLiNER reference implementation's whitespace
/// splitter (`\w+(?:[-_]\w+)*|\S`, Unicode-aware).
auto split_words(std::string_view text) -> std::vector<Word>;

/// A decoded candidate span in word coordinates (end word inclusive).
struct WordSpan {
  int64_t start_word = 0;
  int64_t end_word = 0;
  size_t label = 0;
  double score = 0.0;

  friend auto operator==(const WordSpan&, const WordSpan&) -> bool = default;
};

/// Decodes span logits of shape (num_words, max_width, num_labels):
/// sigmoid, threshold, greedy non-overlap resolution by descending score
/// (flat NER), sorted by start word. Matches the reference implementation.
auto decode_spans(std::span<const float> logits, int64_t num_words,
                  int64_t max_width, size_t num_labels, double threshold)
  -> std::vector<WordSpan>;

} // namespace tenzir::plugins::gliner::detail
