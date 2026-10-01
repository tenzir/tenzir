//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/box.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/nova/event_builder.hpp"
#include "tenzir/nova/events.hpp"

#include <cstddef>
#include <simdjson.h>
#include <string>
#include <string_view>
#include <vector>

namespace tenzir::nova {

struct JsonDecodingOptions {
  /// Keeps only the first occurrence of a key instead of collecting repeats.
  bool first_duplicate_wins = false;
  /// Rejects integers outside the 64-bit range instead of storing their token.
  bool reject_oversized_integers = false;
  /// Rejects integers that require the unsigned half of the 64-bit range.
  bool reject_unsigned_integers = false;
  /// Rejects repeated object keys instead of collecting their values.
  bool reject_duplicate_keys = false;
  /// Maximum depth, counting the complete document's root as zero.
  size_t max_depth = defaults::max_recursion;
};

/// Appends an already framed object to an event builder. Malformed values
/// produce warnings and nulls. Returns whether the object was well-formed.
auto parse_json_object(simdjson::ondemand::object object,
                       EventBuilder::Record row, diagnostic_handler& dh,
                       std::string_view source = {}) -> bool;

/// Appends a document's value to a builder field, including scalar and list
/// values. Returns whether it was well-formed; malformed values produce
/// warnings and nulls. The caller owns document framing and trailing-content
/// validation.
auto parse_json_value(simdjson::ondemand::document& document,
                      EventBuilder::Field field, diagnostic_handler& dh,
                      std::string_view source = {}) -> bool;

/// Decodes complete JSON documents or frames into event batches. Framing
/// belongs to the caller: a frame may span multiple lines.
class JsonParser {
public:
  struct Settings {
    EventBuilder::Settings builder;
    JsonDecodingOptions decoding;
    size_t batch_size = defaults::import::table_slice_size;
    location origin = location::unknown;
  };

  /// Fails if the builder settings are invalid. The handler must outlive the
  /// parser, including after the parser is moved.
  static auto make(Settings settings, diagnostic_handler& dh)
    -> Option<JsonParser>;

  /// Parses a frame as one or more objects, warning about malformed input,
  /// multiple objects, or trailing bytes. Decoded objects remain available
  /// through `take_ready()` and `flush()`.
  auto parse(simdjson::padded_string_view frame) -> void;
  /// Reserves the padding required by simdjson without changing the contents.
  auto parse(std::string& frame) -> void;

  /// Parses exactly one complete object. On failure, no partial events are
  /// returned. Does not change the pending frames or ready batches.
  auto parse_document(std::string_view source) -> failure_or<Events>;

  /// Parses exactly one complete JSON value into a single-row array. Lists
  /// retain their columnar elements, so callers can validate and unwrap them
  /// without rebuilding rows. On failure, no partial values are returned.
  /// Does not change the pending frames or ready batches.
  auto parse_document_value(std::string_view source) -> failure_or<Array<Data>>;

  auto flush() -> void;
  auto take_ready() -> std::vector<Events>;
  /// Number of events not yet cut into a batch.
  auto length() const -> size_t;

private:
  JsonParser(Settings settings, Box<transforming_diagnostic_handler> dh,
             EventBuilder builder);

  Settings settings_;
  /// Pointer-stable because the builder borrows its diagnostic handler.
  Box<transforming_diagnostic_handler> dh_;
  EventBuilder builder_;
  simdjson::ondemand::parser parser_;
  size_t frames_ = 0;
  std::vector<Events> ready_;
};

} // namespace tenzir::nova
