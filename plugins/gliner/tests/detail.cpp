//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "gliner/detail.hpp"

#include <tenzir/test/test.hpp>

#include <cmath>

using namespace tenzir::plugins::gliner::detail;

namespace {

auto logit(double prob) -> float {
  return static_cast<float>(std::log(prob / (1.0 - prob)));
}

} // namespace

// Expectations mirror the GLiNER reference word splitter
// (`\w+(?:[-_]\w+)*|\S`), validated bit-exactly against the Python
// implementation in the development smoke test.
TEST("split words: plain text with punctuation") {
  auto words = split_words("Accepted password for ameier!");
  REQUIRE_EQUAL(words.size(), size_t{5});
  CHECK_EQUAL(words[0], (Word{"Accepted", 0, 8}));
  CHECK_EQUAL(words[3], (Word{"ameier", 22, 28}));
  CHECK_EQUAL(words[4], (Word{"!", 28, 29}));
}

TEST("split words: IP addresses split at dots") {
  auto words = split_words("203.0.113.4");
  REQUIRE_EQUAL(words.size(), size_t{7});
  CHECK_EQUAL(words[0], (Word{"203", 0, 3}));
  CHECK_EQUAL(words[1], (Word{".", 3, 4}));
  CHECK_EQUAL(words[6], (Word{"4", 10, 11}));
}

TEST("split words: hyphens and underscores keep words together") {
  auto words = split_words("svc-backup deploy_bot");
  REQUIRE_EQUAL(words.size(), size_t{2});
  CHECK_EQUAL(words[0], (Word{"svc-backup", 0, 10}));
  CHECK_EQUAL(words[1], (Word{"deploy_bot", 11, 21}));
}

TEST("split words: unicode word characters, byte offsets") {
  auto words = split_words("Zoë päßt");
  REQUIRE_EQUAL(words.size(), size_t{2});
  // "Zoë" is 4 bytes in UTF-8.
  CHECK_EQUAL(words[0], (Word{"Zoë", 0, 4}));
  CHECK_EQUAL(words[1], (Word{"päßt", 5, 11}));
}

TEST("split words: empty and whitespace-only input") {
  CHECK(split_words("").empty());
  CHECK(split_words("   \t\n").empty());
}

TEST("decode spans: threshold filters and sigmoid applies") {
  // 2 words, max_width 2, 1 label; only span (0,0) above 0.5.
  auto logits = std::vector<float>{
    logit(0.9), // (0,0)
    logit(0.3), // (0,1)
    logit(0.2), // (1,1)
    logit(0.1), // (1,2) -> invalid, ignored
  };
  auto spans = decode_spans(logits, 2, 2, 1, 0.5);
  REQUIRE_EQUAL(spans.size(), size_t{1});
  CHECK_EQUAL(spans[0].start_word, int64_t{0});
  CHECK_EQUAL(spans[0].end_word, int64_t{0});
  CHECK(std::abs(spans[0].score - 0.9) < 1e-6);
}

TEST("decode spans: greedy non-overlap keeps the higher score") {
  // 2 words, max_width 2, 1 label; the two-word span (0,1) scores higher
  // than both single-word spans, so it wins and suppresses them.
  auto logits = std::vector<float>{
    logit(0.7), // (0,0)
    logit(0.9), // (0,1)
    logit(0.8), // (1,1)
    logit(0.1), // (1,2) -> invalid
  };
  auto spans = decode_spans(logits, 2, 2, 1, 0.5);
  REQUIRE_EQUAL(spans.size(), size_t{1});
  CHECK_EQUAL(spans[0].start_word, int64_t{0});
  CHECK_EQUAL(spans[0].end_word, int64_t{1});
}

TEST("decode spans: non-overlapping spans sorted by start") {
  // 3 words, max_width 1, 2 labels: word 0 -> label 1, word 2 -> label 0.
  auto logits = std::vector<float>{
    logit(0.2), logit(0.8), // word 0
    logit(0.1), logit(0.1), // word 1
    logit(0.9), logit(0.2), // word 2
  };
  auto spans = decode_spans(logits, 3, 1, 2, 0.5);
  REQUIRE_EQUAL(spans.size(), size_t{2});
  CHECK_EQUAL(spans[0].start_word, int64_t{0});
  CHECK_EQUAL(spans[0].label, size_t{1});
  CHECK_EQUAL(spans[1].start_word, int64_t{2});
  CHECK_EQUAL(spans[1].label, size_t{0});
}

TEST("decode spans: out-of-window spans never decoded") {
  // 1 word, max_width 3: only (0,0) is valid regardless of scores.
  auto logits = std::vector<float>{logit(0.99), logit(0.99), logit(0.99)};
  auto spans = decode_spans(logits, 1, 3, 1, 0.5);
  REQUIRE_EQUAL(spans.size(), size_t{1});
  CHECK_EQUAL(spans[0].end_word, int64_t{0});
}
