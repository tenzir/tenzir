//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/operator_plugin.hpp"

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
namespace specificity {

/// Exact magic bytes or a distinctive header at the start of the input.
inline constexpr auto magic = uint64_t{90};

/// Format-specific markers inside a generic carrier, e.g., GELF field names
/// in a JSON object or the `CEF:` prefix in a text line.
inline constexpr auto dialect = uint64_t{80};

/// Well-formed generic structured data, e.g., the JSON family.
inline constexpr auto structured = uint64_t{70};

/// Line-oriented key-value assignments.
inline constexpr auto keyed = uint64_t{60};

/// Tables with a stable explicit delimiter, e.g., CSV or TSV.
inline constexpr auto delimited = uint64_t{50};

/// Line grammars whose markers also occur in free-form text, e.g., syslog.
inline constexpr auto grammar = uint64_t{40};

/// Whole-document formats that accept a wide range of text, e.g., YAML.
inline constexpr auto document = uint64_t{30};

} // namespace specificity

auto reject(std::string reason = {}) -> read_detection_result;

auto need_more(std::string reason = {}) -> read_detection_result;

auto match(std::string reason = {}) -> read_detection_result;

auto candidate(std::string format_name, std::string operator_name,
               std::string pipeline, uint64_t specificity,
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

auto json_object(read_detection_input input) -> read_detection_result;

auto json_array(read_detection_input input) -> read_detection_result;

auto ndjson(read_detection_input input) -> read_detection_result;

auto json_field(read_detection_input input, std::string_view field)
  -> read_detection_result;

auto gelf(read_detection_input input) -> read_detection_result;

auto magic_prefix(read_detection_input input, std::string_view magic,
                  std::string reason = {}) -> read_detection_result;

} // namespace tenzir::read_detection
