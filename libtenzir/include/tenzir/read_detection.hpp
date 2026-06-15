//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/operator_plugin.hpp"

#include <compare>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

namespace tenzir::read_detection {

/// The cross-format precedence policy for automatic input format detection.
///
/// Capability alone cannot pick a winner: permissive readers accept inputs
/// that more specific readers describe better, e.g., a GELF stream is also
/// valid NDJSON. When multiple candidates match the same probe, `read_auto`
/// selects the one with the highest specificity; matches with equal
/// specificity are ambiguous and abort detection. Keeping all rungs in one
/// place makes the precedence auditable without comparing magic numbers
/// scattered across reader plugins.
enum class specificity : uint8_t {
  /// Exact magic bytes or a distinctive header at the start of the input.
  magic = 90,
  /// Format-specific markers inside a generic carrier, e.g., GELF field names
  /// in a JSON object or the `CEF:` prefix in a text line.
  dialect = 80,
  /// Well-formed generic structured data, e.g., the JSON family.
  structured = 70,
  /// Line-oriented key-value assignments.
  keyed = 60,
  /// Tables with a stable explicit delimiter, e.g., CSV or TSV.
  delimited = 50,
  /// Line grammars whose markers also occur in free-form text, e.g., syslog.
  grammar = 40,
  /// Whole-document formats that accept a wide range of text, e.g., YAML.
  document = 30,
};

constexpr auto operator<=>(specificity lhs, specificity rhs) noexcept
  -> std::strong_ordering {
  return static_cast<uint8_t>(lhs) <=> static_cast<uint8_t>(rhs);
}

} // namespace tenzir::read_detection

namespace tenzir {

struct read_detection_input {
  std::string_view bytes;
  bool eof = false;
};

struct read_detection_result {
  enum class result_state {
    reject,
    need_more,
    match,
  };

  result_state state = result_state::reject;
};

/// A reader that `read_auto` can select when its detector matches the probed
/// input. The detector must be a pure function of the input that decides
/// whether the reader is *capable* of consuming the bytes, ideally by running
/// the reader's actual parser on them. Cross-format precedence is expressed
/// solely through `specificity`.
struct read_detection_candidate {
  read_detection_candidate(
    std::string pipeline, read_detection::specificity specificity,
    std::function<read_detection_result(read_detection_input)> detect);

  std::string pipeline;
  read_detection::specificity specificity;
  std::function<read_detection_result(read_detection_input)> detect;
};

class ReadOperatorPlugin : public virtual OperatorPlugin {
public:
  virtual auto read_detection_candidates() const
    -> std::vector<read_detection_candidate> {
    return {};
  }
};

} // namespace tenzir

namespace tenzir::read_detection {

auto reject() -> read_detection_result;

auto need_more() -> read_detection_result;

auto match() -> read_detection_result;

auto candidate(std::string pipeline, specificity specificity,
               std::function<read_detection_result(read_detection_input)> detect)
  -> read_detection_candidate;

/// A bounded sample of input lines for line-oriented detectors.
struct line_sample {
  /// Lines terminated by a newline, or by the end of input at EOF. Trailing
  /// carriage returns are trimmed.
  std::vector<std::string_view> complete;

  /// Trailing bytes of an unterminated line when more input may arrive.
  std::string_view partial;
};

/// Samples up to `max_lines` complete lines from the front of the probe.
/// @warning The lifetime of the returned substrings is bound to the input.
auto sample_lines(read_detection_input input, size_t max_lines) -> line_sample;

auto magic_prefix(read_detection_input input, std::string_view magic)
  -> read_detection_result;

} // namespace tenzir::read_detection
