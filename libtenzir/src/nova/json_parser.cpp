//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/json_parser.hpp"

#include "tenzir/detail/flat_set.hpp"
#include "tenzir/detail/function.hpp"
#include "tenzir/json_parser.hpp"
#include "tenzir/nova/array_base.hpp"
#include "tenzir/nova/type_system.hpp"

#include <algorithm>
#include <simdjson.h>
#include <utility>

namespace tenzir::nova {

namespace {

using tenzir::json::with_surrounding_bytes;

constexpr auto initial_batch_size = size_t{10 * 1024 * 1024};

/// Returns the current position in the document, for the context of warnings.
using Locator
  = detail::function_view<auto()->simdjson::simdjson_result<const char*>>;

auto parse_object_impl(simdjson::ondemand::object object,
                       EventBuilder::Record row, diagnostic_handler& dh,
                       std::string_view source = {},
                       Option<Locator> where = None{},
                       JsonDecodingOptions options = {}, size_t depth = 0)
  -> bool;

/// Warns about a malformed part of a JSON document. If the document's `source`
/// is known, the warning shows the bytes around `location`.
auto warn_malformed(diagnostic_handler& dh, std::string_view source,
                    simdjson::simdjson_result<const char*> location,
                    std::string_view message) -> void {
  auto b = diagnostic::warning("{}", message);
  if (not source.empty()) {
    b = with_surrounding_bytes(std::move(b), source, location);
  }
  std::move(b).emit(dh);
}

/// Recursively writes a single simdjson value into an event builder field or
/// list. Emits warnings on malformed values and falls back to `null`. Returns
/// whether the value was well-formed. `source`, if given, is the text of the
/// document for the context of warnings.
auto parse_value_impl(auto&& val, auto&& out, diagnostic_handler& dh,
                      std::string_view source = {},
                      JsonDecodingOptions options = {}, size_t depth = 0)
  -> bool {
  if (depth > options.max_depth) {
    warn_malformed(dh, source, val.current_location(),
                   "JSON nesting is too deep");
    out.null();
    return false;
  }
  auto type = val.type();
  if (type.error()) {
    warn_malformed(dh, source, val.current_location(),
                   "failed to parse a JSON value");
    out.null();
    return false;
  }
  switch (type.value_unsafe()) {
    case simdjson::ondemand::json_type::null: {
      auto result = val.is_null();
      if (result.error() or not result.value_unsafe()) {
        warn_malformed(dh, source, val.current_location(),
                       "failed to parse a JSON null");
        out.null();
        return false;
      }
      out.null();
      return true;
    }
    case simdjson::ondemand::json_type::boolean: {
      auto result = val.get_bool();
      if (result.error()) {
        warn_malformed(dh, source, val.current_location(),
                       "failed to parse a JSON boolean");
        out.null();
        return false;
      }
      out.data(result.value_unsafe());
      return true;
    }
    case simdjson::ondemand::json_type::number: {
      auto kind = val.get_number_type();
      if (kind.error()) {
        warn_malformed(dh, source, val.current_location(),
                       "failed to parse a JSON number");
        out.null();
        return false;
      }
      switch (kind.value_unsafe()) {
        case simdjson::ondemand::number_type::floating_point_number: {
          auto number = val.get_double();
          if (number.error()) {
            warn_malformed(dh, source, val.current_location(),
                           "failed to parse a JSON number");
            out.null();
            return false;
          }
          out.data(number.value_unsafe());
          return true;
        }
        case simdjson::ondemand::number_type::signed_integer: {
          auto number = val.get_int64();
          if (number.error()) {
            warn_malformed(dh, source, val.current_location(),
                           "failed to parse a JSON number");
            out.null();
            return false;
          }
          out.data(number.value_unsafe());
          return true;
        }
        case simdjson::ondemand::number_type::unsigned_integer: {
          auto number = val.get_uint64();
          if (number.error()) {
            warn_malformed(dh, source, val.current_location(),
                           "failed to parse a JSON number");
            out.null();
            return false;
          }
          if (options.reject_unsigned_integers) {
            warn_malformed(dh, source, val.current_location(),
                           "JSON integer does not fit into int64");
            out.null();
            return false;
          }
          out.data(number.value_unsafe());
          return true;
        }
        case simdjson::ondemand::number_type::big_integer: {
          if (options.reject_oversized_integers) {
            warn_malformed(dh, source, val.current_location(),
                           "JSON integer does not fit into 64 bits");
            out.null();
            return false;
          }
          // Does not fit into 64 bits; store the raw token as a string.
          out.data(std::string_view{val.raw_json_token()});
          return true;
        }
      }
      TENZIR_UNREACHABLE();
    }
    case simdjson::ondemand::json_type::string: {
      auto str = val.get_string();
      if (str.error()) {
        warn_malformed(dh, source, val.current_location(),
                       "failed to parse a JSON string");
        out.null();
        return false;
      }
      out.data_unparsed(str.value_unsafe());
      return true;
    }
    case simdjson::ondemand::json_type::array: {
      auto arr = val.get_array();
      if (arr.error()) {
        warn_malformed(dh, source, val.current_location(),
                       "failed to parse a JSON array");
        out.null();
        return false;
      }
      auto elements = out.list();
      auto ok = true;
      for (auto element : arr.value_unsafe()) {
        if (element.error()) {
          warn_malformed(dh, source, element.current_location(),
                         "failed to parse a JSON array element");
          elements.null();
          ok = false;
          continue;
        }
        ok &= parse_value_impl(element.value_unsafe(), elements, dh, source,
                               options, depth + 1);
      }
      return ok;
    }
    case simdjson::ondemand::json_type::object: {
      auto obj = val.get_object();
      if (obj.error()) {
        warn_malformed(dh, source, val.current_location(),
                       "failed to parse a JSON object");
        out.null();
        return false;
      }
      return parse_object_impl(obj.value_unsafe(), out.record(), dh, source,
                               Locator{[&] {
                                 return val.current_location();
                               }},
                               options, depth);
    }
    case simdjson::ondemand::json_type::unknown: {
      warn_malformed(dh, source, val.current_location(),
                     "failed to parse a JSON value");
      out.null();
      return false;
    }
  }
  TENZIR_UNREACHABLE();
}

/// Writes the fields of a JSON object into `row`. Returns whether the object
/// was well-formed.
auto parse_object_impl(simdjson::ondemand::object object,
                       EventBuilder::Record row, diagnostic_handler& dh,
                       std::string_view source, Option<Locator> where,
                       JsonDecodingOptions options, size_t depth) -> bool {
  auto location = [&]() -> simdjson::simdjson_result<const char*> {
    if (where) {
      return (*where)();
    }
    return simdjson::UNINITIALIZED;
  };
  auto ok = true;
  auto keys = detail::flat_set<std::string_view>{};
  for (auto pair : object) {
    if (pair.error()) {
      warn_malformed(dh, source, location(),
                     "failed to parse a JSON key-value pair");
      ok = false;
      continue;
    }
    auto key = pair.unescaped_key();
    if (key.error()) {
      warn_malformed(dh, source, location(), "failed to parse a JSON key");
      ok = false;
      continue;
    }
    auto value = pair.value();
    if (value.error()) {
      warn_malformed(dh, source, value.current_location(),
                     "failed to parse a JSON object value");
      ok = false;
      continue;
    }
    if ((options.first_duplicate_wins or options.reject_duplicate_keys)
        and not keys.insert(key.value_unsafe()).second) {
      if (options.reject_duplicate_keys) {
        warn_malformed(dh, source, location(), "duplicate JSON object key");
        ok = false;
      }
      // Ignoring a repeated key must not hide malformed JSON in its value.
      auto discarded_settings = EventBuilder::Settings{};
      discarded_settings.raw = true;
      auto discarded
        = EventBuilder::make_prevalidated(std::move(discarded_settings), dh);
      ok &= parse_value_impl(value.value_unsafe(), discarded.value(), dh,
                             source, options, depth + 1);
      continue;
    }
    auto field = options.exact_keys ? row.exact_field(key.value_unsafe())
                                    : row.field(key.value_unsafe());
    ok &= parse_value_impl(value.value_unsafe(), std::move(field), dh, source,
                           options, depth + 1);
  }
  return ok;
}

/// Points diagnostics without a location to the operator.
auto make_operator_dh(location operator_location, diagnostic_handler& dh)
  -> Box<transforming_diagnostic_handler> {
  return Box<transforming_diagnostic_handler>{
    std::in_place, dh, [operator_location](diagnostic d) {
      if (operator_location and not d.has_location()) {
        d.annotations.emplace_back(true, std::string{}, operator_location);
      }
      return d;
    }};
}

} // namespace

auto parse_json_object(simdjson::ondemand::object object,
                       EventBuilder::Record row, diagnostic_handler& dh,
                       std::string_view source) -> bool {
  return parse_object_impl(object, std::move(row), dh, source);
}

auto parse_json_value(simdjson::ondemand::document& document,
                      EventBuilder::Field field, diagnostic_handler& dh,
                      std::string_view source, JsonDecodingOptions options)
  -> bool {
  return parse_value_impl(document, std::move(field), dh, source, options);
}

auto JsonParser::make(Settings settings, diagnostic_handler& dh)
  -> Option<JsonParser> {
  auto parser_dh = make_operator_dh(settings.origin, dh);
  auto builder = EventBuilder::make(settings.builder, *parser_dh);
  if (not builder) {
    return None{};
  }
  settings.batch_size = std::max(settings.batch_size, size_t{1});
  return JsonParser{std::move(settings), std::move(parser_dh),
                    std::move(builder).unwrap()};
}

JsonParser::JsonParser(Settings settings,
                       Box<transforming_diagnostic_handler> dh,
                       EventBuilder builder)
  : settings_{std::move(settings)},
    dh_{std::move(dh)},
    builder_{std::move(builder)} {
}

auto JsonParser::parse(simdjson::padded_string_view line) -> void {
  ++frames_;
  auto& dh = *dh_;
  auto source = std::string_view{line.data(), line.size()};
  auto stream = simdjson::ondemand::document_stream{};
  if (auto err = parser_
                   .iterate_many(line.data(), line.size(),
                                 std::max(line.size(), initial_batch_size))
                   .get(stream)) {
    diagnostic::warning("{}", error_message(err)).emit(dh);
    return;
  }
  auto objects = size_t{0};
  auto failed = false;
  for (auto doc_it = stream.begin(); doc_it != stream.end(); ++doc_it) {
    if (auto err = doc_it.error()) {
      with_surrounding_bytes(diagnostic::warning("{}", error_message(err))
                               .note("line {}", frames_)
                               .note("skipped invalid JSON at index {}",
                                     doc_it.current_index()),
                             source, source.data() + doc_it.current_index())
        .emit(dh);
      failed = true;
      break;
    }
    auto doc = *doc_it;
    auto object = doc.get_object();
    if (auto err = object.error()) {
      auto loc = doc.current_location();
      auto message = err == simdjson::INCORRECT_TYPE
                       ? std::string{"expected a JSON object"}
                       : std::string{error_message(err)};
      auto column = loc.error()
                      ? size_t{0}
                      : static_cast<size_t>(loc.value_unsafe() - source.data());
      with_surrounding_bytes(diagnostic::warning("{}", message)
                               .note("line {} column {}", frames_, column)
                               .note("skipped invalid JSON"),
                             source, loc)
        .emit(dh);
      failed = true;
      break;
    }
    parse_object_impl(object.value_unsafe(), builder_.event(), dh, {}, None{},
                      settings_.decoding);
    ++objects;
    if (length() >= settings_.batch_size) {
      flush();
    }
  }
  if (objects == 0 and not failed) {
    with_surrounding_bytes(diagnostic::warning("line did not contain a "
                                               "single valid JSON object")
                             .note("line {}", frames_)
                             .note("skipped invalid JSON"),
                           source, source.data())
      .emit(dh);
  } else if (objects > 1) {
    with_surrounding_bytes(
      diagnostic::warning("more than one JSON object in line")
        .note("line {}", frames_)
        .note("encountered a total of {} objects", objects),
      source, source.data())
      .emit(dh);
  }
  if (auto truncated = stream.truncated_bytes();
      truncated > 0 and objects > 0) {
    with_surrounding_bytes(diagnostic::warning("skipped remaining invalid "
                                               "JSON bytes")
                             .note("line {}", frames_)
                             .note("{} bytes remained", truncated)
                             .note("skipped invalid JSON"),
                           source, source.data() + source.size() - truncated)
      .emit(dh);
  }
}

auto JsonParser::parse(std::string& frame) -> void {
  frame.reserve(frame.size() + simdjson::SIMDJSON_PADDING);
  parse(simdjson::padded_string_view{frame});
}

auto JsonParser::parse_document(std::string_view source) -> failure_or<Events> {
  auto buffer = std::string{source};
  auto document = parser_.iterate(buffer);
  // Iterating may reallocate the buffer to add padding.
  source = buffer;
  if (document.error()) {
    warn_malformed(*dh_, source, source.data(),
                   simdjson::error_message(document.error()));
    return failure::promise();
  }
  auto& doc = document.value_unsafe();
  auto object = doc.get_object();
  if (object.error()) {
    warn_malformed(*dh_, source, doc.current_location(),
                   object.error() == simdjson::INCORRECT_TYPE
                     ? "expected a JSON object"
                     : simdjson::error_message(object.error()));
    return failure::promise();
  }
  auto builder = EventBuilder::make_prevalidated(settings_.builder, *dh_);
  auto ok = parse_object_impl(object.value_unsafe(), builder.event(), *dh_,
                              source, Locator{[&] {
                                return doc.current_location();
                              }},
                              settings_.decoding);
  if (ok and not doc.at_end()) {
    warn_malformed(*dh_, source, doc.current_location(),
                   "found trailing content after the JSON object");
    ok = false;
  }
  if (not ok) {
    return failure::promise();
  }
  return builder.finish();
}

auto JsonParser::parse_document_value(std::string_view source)
  -> failure_or<Array<Data>> {
  auto buffer = std::string{source};
  auto document = parser_.iterate(buffer);
  // Iterating may reallocate the buffer to add padding.
  source = buffer;
  if (document.error() != simdjson::SUCCESS) {
    warn_malformed(*dh_, source, source.data(),
                   simdjson::error_message(document.error()));
    return failure::promise();
  }
  auto& doc = document.value_unsafe();
  auto builder = EventBuilder::make_prevalidated(settings_.builder, *dh_);
  auto ok
    = parse_value_impl(doc, builder.value(), *dh_, source, settings_.decoding);
  if (ok and not doc.at_end()) {
    warn_malformed(*dh_, source, doc.current_location(),
                   "found trailing content after the JSON value");
    ok = false;
  }
  if (not ok) {
    return failure::promise();
  }
  return builder.finish_data();
}

auto JsonParser::length() const -> size_t {
  return static_cast<size_t>(builder_.length());
}

auto JsonParser::flush() -> void {
  if (builder_.length() > 0) {
    ready_.push_back(builder_.finish());
  }
}

auto JsonParser::take_ready() -> std::vector<Events> {
  return std::exchange(ready_, {});
}

} // namespace tenzir::nova
