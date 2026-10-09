//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

/// What the generated bindings are written against: JSON in and out, paths and
/// statuses. Included by `generated/api.cpp` alone and nested, so that generic
/// names like `field`, `write` and `encode` reach no header.

#pragma once

#include <tenzir/box.hpp>
#include <tenzir/controller/transport.hpp>
#include <tenzir/option.hpp>
#include <tenzir/result.hpp>
#include <tenzir/time.hpp>
#include <tenzir/unit.hpp>
#include <tenzir/variant.hpp>

#include <fmt/chrono.h>
#include <fmt/format.h>

#include <chrono>
#include <cstdint>
#include <iterator>
#include <simdjson.h>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace tenzir::api {

inline auto is_null(simdjson::dom::element value) -> bool {
  return value.is_null();
}

/// The last occurrence of a member, not the first that simdjson's own lookup
/// returns: a duplicated key resolves to its last value in `JSON.parse`, and
/// both sides have to read the same body the same way.
inline auto find_last(simdjson::dom::object const& object, std::string_view key)
  -> Option<simdjson::dom::element> {
  auto found = Option<simdjson::dom::element>{None{}};
  for (auto entry : object) {
    if (entry.key == key) {
      found = entry.value;
    }
  }
  return found;
}

/// Fetches a member that has to be there.
inline auto field(simdjson::dom::object const& object, std::string_view key,
                  std::string_view schema)
  -> Result<simdjson::dom::element, ParseError> {
  auto value = find_last(object, key);
  if (not value) {
    return Err{
      ParseError{std::string{schema}, fmt::format("missing `{}`", key)}};
  }
  return *value;
}

/// Fetches a member that may be absent or null, which mean the same thing here
/// even though the wire distinguishes them.
inline auto
optional_field(simdjson::dom::object const& object, std::string_view key)
  -> Option<simdjson::dom::element> {
  auto value = find_last(object, key);
  if (not value or value->is_null()) {
    return None{};
  }
  return value;
}

/// A parsed body.
///
/// DOM rather than on-demand, which is a semantic choice: parsing is eager,
/// so a body broken anywhere is refused here, the way `JSON.parse` refuses it
/// in TypeScript, rather than accepted because no decoder looked at the broken
/// part. Elements point into the parser, so it lives behind a stable address.
class Document {
public:
  static auto parse(std::string_view body, std::string_view schema)
    -> Result<Document, ParseError> {
    // Asked before parsing, because it is the one cause worth naming: nothing
    // arrived at all, as opposed to something that arrived broken.
    if (body.empty()) {
      return Err{ParseError{std::string{schema}, "body is empty"}};
    }
    // Moved before parsing, so no element points into it yet. The input is
    // copied and padded inside `parse`, and is not needed afterwards.
    auto state = Box{State{}};
    if (state->parser.parse(body.data(), body.size()).get(state->root)
        != simdjson::SUCCESS) {
      return Err{ParseError{std::string{schema}, "body is not JSON"}};
    }
    return Document{std::move(state)};
  }

  /// Valid for as long as this object is.
  auto root() const -> simdjson::dom::element {
    return state_->root;
  }

private:
  struct State {
    // Not an aggregate: `State{}` would copy-initialize the parser past its
    // `explicit` default constructor, which is ill-formed.
    State() = default;

    simdjson::dom::parser parser;
    simdjson::dom::element root;
  };

  explicit Document(Box<State> state) : state_{std::move(state)} {
  }

  // Boxed for the address, not for the allocation: moving the parser would
  // leave every element pointing at where its document used to be.
  Box<State> state_;
};

inline auto object_of(simdjson::dom::element value, std::string_view schema)
  -> Result<simdjson::dom::object, ParseError> {
  auto object = simdjson::dom::object{};
  if (value.get_object().get(object) != simdjson::SUCCESS) {
    return Err{ParseError{std::string{schema}, "expected an object"}};
  }
  return object;
}

inline auto string_of(simdjson::dom::element value, std::string_view schema)
  -> Result<std::string, ParseError> {
  auto view = std::string_view{};
  if (value.get_string().get(view) != simdjson::SUCCESS) {
    return Err{ParseError{std::string{schema}, "expected a string"}};
  }
  return std::string{view};
}

inline auto bool_of(simdjson::dom::element value, std::string_view schema)
  -> Result<bool, ParseError> {
  auto result = false;
  if (value.get_bool().get(result) != simdjson::SUCCESS) {
    return Err{ParseError{std::string{schema}, "expected a boolean"}};
  }
  return result;
}

inline auto double_of(simdjson::dom::element value, std::string_view schema)
  -> Result<double, ParseError> {
  auto result = 0.0;
  if (value.get_double().get(result) != simdjson::SUCCESS) {
    return Err{ParseError{std::string{schema}, "expected a number"}};
  }
  return result;
}

/// `S.Int` on the wire is a JSON number, and JSON has one number type. What
/// arrives has to be whole and stay in JavaScript's safe-integer range, and
/// neither is true of every number a sender may write, so both are checked
/// here rather than truncated silently.
inline auto int64_of(simdjson::dom::element value, std::string_view schema)
  -> Result<std::int64_t, ParseError> {
  auto result = std::int64_t{0};
  if (value.get_int64().get(result) == simdjson::SUCCESS) {
    constexpr auto largest_safe_integer = std::int64_t{9'007'199'254'740'991};
    if (result >= -largest_safe_integer and result <= largest_safe_integer) {
      return result;
    }
  }
  if (value.is_number()) {
    return Err{ParseError{std::string{schema},
                          "expected an integer, not a fractional or "
                          "unsafe number"}};
  }
  return Err{ParseError{std::string{schema}, "expected an integer"}};
}

/// `S.DateTimeUtc` is a string on the wire and a `DateTime.Utc` after
/// decoding. The OpenAPI document says only "string", which is the whole
/// reason this generator reads the AST instead.
///
/// RFC 3339 and nothing else: a full date, a full time, an offset. That is
/// the only shape the platform writes. Effect additionally accepts whatever
/// the JavaScript engine's `Date` parser takes. That is engine-defined, down
/// to reading a zoneless time in the server's local zone, and no contract to
/// chase. The line that holds is one-directional: accept nothing the
/// TypeScript side refuses.
inline auto time_of(simdjson::dom::element value, std::string_view schema)
  -> Result<time, ParseError> {
  TRY(auto text, string_of(value, schema));
  auto view = std::string_view{text};
  auto invalid = [&] {
    return Err{
      ParseError{std::string{schema}, "expected an RFC 3339 date-time"}};
  };
  auto number = [&view](std::size_t digits, unsigned& out) {
    if (view.size() < digits) {
      return false;
    }
    out = 0;
    for (auto i = std::size_t{0}; i < digits; ++i) {
      auto character = view[i];
      if (character < '0' or character > '9') {
        return false;
      }
      out = out * 10 + static_cast<unsigned>(character - '0');
    }
    view.remove_prefix(digits);
    return true;
  };
  auto expect = [&view](char character) {
    if (not view.starts_with(character)) {
      return false;
    }
    view.remove_prefix(1);
    return true;
  };
  auto year = 0u;
  auto month = 0u;
  auto day = 0u;
  auto hour = 0u;
  auto minute = 0u;
  auto second = 0u;
  if (not(number(4, year) and expect('-') and number(2, month) and expect('-')
          and number(2, day) and expect('T') and number(2, hour) and expect(':')
          and number(2, minute) and expect(':') and number(2, second))) {
    return invalid();
  }
  if (hour > 23 or minute > 59 or second > 59) {
    return invalid();
  }
  auto fraction = duration{0};
  if (view.starts_with('.')) {
    view.remove_prefix(1);
    auto digits = 0;
    auto nanoseconds = std::int64_t{0};
    while (not view.empty() and view.front() >= '0' and view.front() <= '9') {
      // Nanoseconds are as fine as `time` counts; digits past them are heard
      // and contribute nothing.
      if (digits < 9) {
        nanoseconds = nanoseconds * 10 + (view.front() - '0');
        ++digits;
      }
      view.remove_prefix(1);
    }
    if (digits == 0) {
      return invalid();
    }
    for (; digits < 9; ++digits) {
      nanoseconds *= 10;
    }
    fraction = duration{nanoseconds};
  }
  auto offset = std::chrono::minutes{0};
  if (not expect('Z')) {
    auto negative = view.starts_with('-');
    if (not negative and not view.starts_with('+')) {
      return invalid();
    }
    view.remove_prefix(1);
    auto offset_hours = 0u;
    auto offset_minutes = 0u;
    if (not(number(2, offset_hours) and expect(':')
            and number(2, offset_minutes))
        or offset_hours > 23 or offset_minutes > 59) {
      return invalid();
    }
    offset
      = std::chrono::hours{offset_hours} + std::chrono::minutes{offset_minutes};
    if (negative) {
      offset = -offset;
    }
  }
  if (not view.empty()) {
    return invalid();
  }
  auto date
    = std::chrono::year_month_day{std::chrono::year{static_cast<int>(year)},
                                  std::chrono::month{month},
                                  std::chrono::day{day}};
  if (not date.ok()) {
    return invalid();
  }
  return time{std::chrono::sys_days{date}} + std::chrono::hours{hour}
         + std::chrono::minutes{minute} + std::chrono::seconds{second}
         + fraction - offset;
}

inline auto unit_of(simdjson::dom::element, std::string_view)
  -> Result<Unit, ParseError> {
  return Unit{};
}

inline auto write_string(std::string_view value, std::string& out) -> void {
  out += '"';
  for (auto character : value) {
    switch (character) {
      case '"':
        out += R"(\")";
        break;
      case '\\':
        out += R"(\\)";
        break;
      case '\n':
        out += R"(\n)";
        break;
      case '\r':
        out += R"(\r)";
        break;
      case '\t':
        out += R"(\t)";
        break;
      default:
        if (static_cast<unsigned char>(character) < 0x20) {
          fmt::format_to(std::back_inserter(out), R"(\u{:04x})",
                         static_cast<unsigned>(character));
        } else {
          out += character;
        }
    }
  }
  out += '"';
}

inline auto write_json(std::string const& value, std::string& out) -> void {
  write_string(value, out);
}

inline auto write_json(bool value, std::string& out) -> void {
  out += value ? "true" : "false";
}

inline auto write_json(double value, std::string& out) -> void {
  fmt::format_to(std::back_inserter(out), "{}", value);
}

/// Writers for the shapes a success type can take on its own, so that
/// `encode` needs nothing generated for `std::vector<Pipeline>` beyond
/// the writer for a `Pipeline`.
template <class T>
auto write_json(std::vector<T> const& values, std::string& out) -> void {
  out += '[';
  auto separator = "";
  for (auto const& value : values) {
    out += separator;
    separator = ",";
    write_json(value, out);
  }
  out += ']';
}

template <class T>
auto write_json(Option<T> const& value, std::string& out) -> void {
  if (value) {
    write_json(*value, out);
  } else {
    out += "null";
  }
}

inline auto write_time(time value, std::string& out) -> void {
  fmt::format_to(std::back_inserter(out), R"("{:%FT%T}Z")",
                 std::chrono::floor<std::chrono::microseconds>(value));
}

/// A refusal, as a frame.
///
/// `dispatch` turns a request away before a handler sees it when it does not
/// decode, which is the one failure the serving side has that no group
/// declares. The framework does the same in TypeScript. The shape is the
/// framework's too: Effect declares `HttpApiDecodeError` on every endpoint's
/// 400, so a client generated from the API decodes this refusal instead of
/// failing on the refusal itself.
///
/// Hand-written against Effect v3's serialization, which is hardwired there
/// and frozen with v3. Effect v4 makes the decode error an ordinary declared
/// error (Effect-TS/effect#4751); once the platform migrates and the spec
/// declares a shape of our own, this becomes generated code like every other
/// failure writer. `FRAMEWORK_FAILURES` in the generator's `model.ts` is the
/// other half of that change.
inline auto as_request_failure(std::uint16_t status) {
  return [status](ParseError error) {
    auto body = std::string{};
    body += R"({"_tag":"HttpApiDecodeError","issues":[],"message":)";
    write_string(fmt::format("{}: {}", error.schema, error.reason), body);
    body += '}';
    return HttpResponse{status, std::move(body)};
  };
}

/// A request we could not read. The sender is at fault, and `dispatch`
/// refuses it before a handler sees it.
inline constexpr auto bad_request = std::uint16_t{400};

/// The `_tag` of an error body, where it has one.
///
/// Parsed on its own: the bodies this reads are small and rare, declared
/// failures only, and the alternative is a decoder that has to be told what
/// it is looking at before it looks.
inline auto tag_of(std::string_view body) -> Option<std::string> {
  auto parsed = Document::parse(body, "error");
  if (parsed.is_err()) {
    return None{};
  }
  auto document = std::move(parsed).unwrap();
  auto object = simdjson::dom::object{};
  if (document.root().get_object().get(object) != simdjson::SUCCESS) {
    return None{};
  }
  auto value = optional_field(object, "_tag");
  if (not value) {
    return None{};
  }
  auto view = std::string_view{};
  if (value->get_string().get(view) != simdjson::SUCCESS) {
    return None{};
  }
  return std::string{view};
}

/// Matches a `/a/:x/b` template against a concrete path, appending each
/// parameter it binds. Segment counts must agree; no segment may be empty.
inline auto match_path(std::string_view pattern, std::string_view path,
                       std::vector<std::string_view>& parameters) -> bool {
  parameters.clear();
  auto next = [](std::string_view& rest) {
    auto slash = rest.find('/');
    auto segment = rest.substr(0, slash);
    rest.remove_prefix(slash == std::string_view::npos ? rest.size()
                                                       : slash + 1);
    return segment;
  };
  while (not pattern.empty() or not path.empty()) {
    if (pattern.empty() or path.empty()) {
      return false;
    }
    auto expected = next(pattern);
    auto actual = next(path);
    if (expected.starts_with(':')) {
      if (actual.empty()) {
        return false;
      }
      parameters.push_back(actual);
    } else if (expected != actual) {
      return false;
    }
  }
  return true;
}

/// Turns a handler's answer into a frame, under the status the endpoint says it
/// succeeds with. A failure carries its own status, which is why the error type
/// holds one.
template <std::uint16_t Status, class T>
auto encode(T value) -> HttpResponse {
  auto body = std::string{};
  write_json(value, body);
  return HttpResponse{Status, std::move(body)};
}

/// A body-less success is a body-less response: an endpoint that declares
/// nothing succeeds with 204, and a 204 that carries bytes is malformed.
template <std::uint16_t Status>
auto encode() -> HttpResponse {
  return HttpResponse{Status, std::string{}};
}

/// One declared failure, as a frame. Its own status, so nothing maps it.
template <class F>
auto refused(F failed) -> HttpResponse {
  auto body = std::string{};
  write_json(failed, body);
  return HttpResponse{failed.status, std::move(body)};
}

/// Several, where an endpoint declares more than one. The overload rather than
/// an `if constexpr` because a single declared failure is that type, not a
/// variant holding it.
template <class... Fs>
auto refused(variant<Fs...> failed) -> HttpResponse {
  return match(std::move(failed), [](auto inner) {
    return refused(std::move(inner));
  });
}

/// A handler that may fail.
template <std::uint16_t Status, class T, class E>
auto encode(Result<T, E> answer) -> HttpResponse {
  if (answer.is_err()) {
    return refused(std::move(answer).unwrap_err());
  }
  // A handler whose success carries nothing answers with `Result<void, E>`,
  // and unwrapping that yields nothing to encode.
  if constexpr (std::is_void_v<T>) {
    std::move(answer).unwrap();
    return encode<Status>();
  } else {
    return encode<Status>(std::move(answer).unwrap());
  }
}

} // namespace tenzir::api
