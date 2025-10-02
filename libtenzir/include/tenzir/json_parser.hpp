//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/padded_buffer.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/multi_series_builder.hpp"
#include "tenzir/si_literals.hpp"

#include <simdjson.h>

namespace tenzir::json {

using namespace tenzir::si_literals;

/// This is the initial simdjson buffer size when *NOT* using NDJSON.
constexpr auto initial_simdjson_batch_size = 10_M;
/// This is the maximum size we increase the simdjson buffer when *NOT* using
/// NDJSON.
constexpr auto max_simdjson_batch_size = 2_G;
static_assert(initial_simdjson_batch_size <= max_simdjson_batch_size);
static_assert(max_simdjson_batch_size <= 4_G,
              "simdjson specifies 4G as an upper bound for the batch_size");

/// Parses simdjson objects into the given `series_builder` handles.
class doc_parser {
public:
  /// The result of a parsing operation
  enum class result {
    /// The parsing succeeded
    success,
    /// The parsing failed, but wrote elements into the builder
    failure_with_write,
    /// The parsing failed, but did not affect the builder
    failure_no_change,
  };

  doc_parser(std::string_view parsed_document, diagnostic_handler& diag)
    : parsed_document_{parsed_document}, diag_{diag} {
  }

  doc_parser(std::string_view parsed_document, diagnostic_handler& diag,
             std::size_t parsed_lines)
    : parsed_document_{parsed_document},
      diag_{diag},
      parsed_lines_{parsed_lines} {
  }

  [[nodiscard]] auto parse_object(auto&& v, auto builder, size_t depth = 0u)
    -> bool {
    auto obj = v.get_object();
    if (obj.error()) {
      report_parse_err(v, "object");
      return false;
    }
    for (auto pair : obj) {
      if (pair.error()) {
        report_parse_err(v, "key value pair");
        return false;
      }
      auto maybe_key = pair.unescaped_key();
      if (maybe_key.error()) {
        report_parse_err(v, "key in an object");
        return false;
      }
      auto key = maybe_key.value_unsafe();
      auto val = pair.value();
      if (val.error()) {
        report_parse_err(val, fmt::format("object value at key `{}`", key));
        return false;
      }
      auto value_parse_result = result::success;
      value_parse_result = parse_value(
        val.value_unsafe(), builder.unflattened_field(key), depth + 1);
      if (value_parse_result != result::success) {
        return false;
      }
    }
    return true;
  }

  [[nodiscard]] auto parse_value(auto&& val, auto&& builder, size_t depth)
    -> result {
    TENZIR_ASSERT(depth <= defaults::max_recursion,
                  "nesting too deep in JSON parser");
    auto type = val.type();
    if (type.error()) {
      report_parse_err(val, "a value");
      return result::failure_no_change;
    }
    switch (type.value_unsafe()) {
      case simdjson::ondemand::json_type::null:
        builder.null();
        return result::success;
      case simdjson::ondemand::json_type::number:
        return parse_number(val, builder);
      case simdjson::ondemand::json_type::boolean: {
        auto result = val.get_bool();
        if (result.error()) {
          report_parse_err(val, "a boolean value");
          return result::failure_no_change;
        }
        builder.data(result.value_unsafe());
        return result::success;
      }
      case simdjson::ondemand::json_type::string:
        return parse_string(val, builder);
      case simdjson::ondemand::json_type::array: {
        const auto success = parse_array(val, builder.list(), depth + 1);
        return success ? result::success : result::failure_with_write;
      }
      case simdjson::ondemand::json_type::object: {
        const auto success = parse_object(val, builder.record(), depth + 1);
        return success ? result::success : result::failure_with_write;
      }
      case simdjson::ondemand::json_type::unknown: {
        report_parse_err(val, "a value");
        return result::failure_no_change;
      }
    }
    TENZIR_UNREACHABLE();
  }

private:
  [[nodiscard]] auto parse_number(auto&& val, auto&& builder) -> result {
    auto kind = simdjson::ondemand::number_type{};
    auto result = val.get_number_type();
    if (result.error()) {
      report_parse_err(val, "a number");
      return result::failure_no_change;
    }
    kind = result.value_unsafe();
    switch (kind) {
      case simdjson::ondemand::number_type::floating_point_number: {
        auto result = val.get_double();
        if (result.error()) {
          report_parse_err(val, "a number");
          return result::failure_no_change;
        }
        builder.data(result.value_unsafe());
        return result::success;
      }
      case simdjson::ondemand::number_type::signed_integer: {
        auto result = val.get_int64();
        if (result.error()) {
          report_parse_err(val, "a number");
          return result::failure_no_change;
        }
        builder.data(result.value_unsafe());
        return result::success;
      }
      case simdjson::ondemand::number_type::unsigned_integer: {
        auto result = val.get_uint64();
        if (result.error()) {
          report_parse_err(val, "a number");
          return result::failure_no_change;
        }
        builder.data(result.value_unsafe());
        return result::success;
      }
      case simdjson::ondemand::number_type::big_integer: {
        // TODO is this a good idea?
        // from the users PoV this isnt an error/warning. its just a limitation
        // of the library/tenzir we could
        // * store null (current behaviour)
        // * store a double (i.e. as an approx value)
        // * store the value as a string
        report_parse_err(val, "a big integer",
                         "value does not fit into 64 bits");
        /// We need this potential unpacking here, as `parse_json` may give us
        /// an entire `document` which has a slightly different iterface
        auto raw = val.raw_json_token();
        if constexpr (std::same_as<decltype(raw), std::string_view>) {
          builder.data(std::string{raw});
        } else {
          if (raw.error()) {
            builder.null();
          } else {
            builder.data(std::string{raw.value_unsafe()});
          }
        }
        return result::success;
      }
    }
    TENZIR_UNREACHABLE();
  }

  [[nodiscard]] auto parse_string(auto&& val, auto&& builder) -> result {
    auto maybe_str = val.get_string();
    if (maybe_str.error()) {
      report_parse_err(val, "a string");
      return result::failure_no_change;
    }
    builder.data_unparsed(std::string{maybe_str.value_unsafe()});
    return result::success;
  }

  [[nodiscard]] auto parse_array(auto&& val, auto builder, size_t depth)
    -> bool {
    auto arr = val.get_array();
    if (arr.error()) {
      report_parse_err(val, "an array");
      return false;
    }
    auto written_once = false;
    for (auto element : arr) {
      if (element.error()) {
        report_parse_err(element, "an array element");
        return false;
      }
      auto res = parse_value(element.value_unsafe(), builder, depth + 1);
      written_once |= res != result::failure_no_change;
      if (res != result::success) {
        return false;
      }
    }
    return true;
  }

  void report_parse_err(auto& v, std::string expected, std::string note = {}) {
    if (parsed_lines_) {
      report_parse_err_with_parsed_lines(v, std::move(expected),
                                         std::move(note));
      return;
    }
    diagnostic::warning("failed to parse {} in the JSON document",
                        std::move(expected))
      .note(note)
      .emit(diag_);
  }

  void report_parse_err_with_parsed_lines(auto& v, std::string description,
                                          std::string note) {
    if (v.current_location().error()) {
      diagnostic::warning("failed to parse {} in the JSON document",
                          std::move(description))
        .note("line {}", *parsed_lines_)
        .note(note)
        .emit(diag_);
      return;
    }
    auto column = v.current_location().value_unsafe() - parsed_document_.data();
    auto b = diagnostic::warning("failed to parse {} in the JSON document",
                                 std::move(description))
               .note("line {} column {}", *parsed_lines_, column);
    if (not note.empty()) {
      b = std::move(b).note("{}", note);
    }
    std::move(b).emit(diag_);
  }

  std::string_view parsed_document_;
  diagnostic_handler& diag_;
  std::optional<std::size_t> parsed_lines_;
};
class parser_base {
public:
  parser_base(std::string name_, diagnostic_handler& dh_,
              multi_series_builder::options options)
    : dh{std::make_unique<transforming_diagnostic_handler>(
        dh_,
        [name = std::move(name_)](diagnostic d) {
          d.message = fmt::format("{} parser: {}", name, d.message);
          return d;
        })},
      builder{std::move(options), *dh, modules::get_schema,
              detail::data_builder::non_number_parser} {
  }
  // this has to be pointer stable because `builder` holds a reference to it
  // internally
  std::unique_ptr<transforming_diagnostic_handler> dh;
  multi_series_builder builder;
  simdjson::ondemand::parser json_parser;
  bool abort_requested = false;
};

class ndjson_parser final : public parser_base {
public:
  using parser_base::parser_base;

  auto parse(simdjson::padded_string_view json_line) -> void;
  auto validate_completion() const -> void;

private:
  std::size_t lines_processed_ = 0u;
};

class default_parser final : public parser_base {
public:
  default_parser(std::string name_, diagnostic_handler& dh,
                 multi_series_builder::options options, bool arrays_of_objects)
    : parser_base{std::move(name_), dh, std::move(options)},
      arrays_of_objects_{arrays_of_objects} {
  }

  auto parse(const chunk& json_chunk) -> void;
  auto validate_completion() -> void;

private:
  auto handle_truncated_bytes() -> void;
  bool arrays_of_objects_;
  // The simdjson suggests to initialize the padding part to either 0s or
  // spaces.
  detail::padded_buffer<simdjson::SIMDJSON_PADDING, '\0'> buffer_;
  size_t current_batch_size = initial_simdjson_batch_size;
  simdjson::ondemand::document_stream stream_;
};

} // namespace tenzir::json
