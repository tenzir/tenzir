//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/read_detection.hpp"

#include "tenzir/detail/string.hpp"

#include <fmt/format.h>

#include <simdjson.h>
#include <utility>
#include <vector>

namespace tenzir::read_detection {

namespace {

using detection_state = read_detection_result::result_state;

struct json_scan_result {
  enum class kind {
    incomplete,
    invalid,
    complete,
  };

  kind state = kind::incomplete;
  char top_level = '\0';
  size_t end = 0;
  char first_array_element = '\0';
};

auto scan_json_value(std::string_view input) -> json_scan_result {
  auto bytes = detail::trim_front(input);
  if (bytes.empty()) {
    return {};
  }
  auto stack = std::vector<char>{};
  auto in_string = false;
  auto escaped = false;
  auto top = bytes.front();
  auto first_array_element = char{'\0'};
  if (top != '{' and top != '[') {
    return {.state = json_scan_result::kind::invalid};
  }
  for (auto i = size_t{0}; i < bytes.size(); ++i) {
    auto ch = bytes[i];
    if (in_string) {
      if (escaped) {
        escaped = false;
      } else if (ch == '\\') {
        escaped = true;
      } else if (ch == '"') {
        in_string = false;
      }
      continue;
    }
    if (ch == '"') {
      in_string = true;
      continue;
    }
    if (top == '[' and stack.size() == 1 and first_array_element == '\0'
        and not detail::ascii_whitespace.contains(ch) and ch != '[') {
      if (ch != ',' and ch != ']') {
        first_array_element = ch;
      }
    }
    if (ch == '{' or ch == '[') {
      stack.push_back(ch == '{' ? '}' : ']');
      continue;
    }
    if (ch == '}' or ch == ']') {
      if (stack.empty() or stack.back() != ch) {
        return {.state = json_scan_result::kind::invalid};
      }
      stack.pop_back();
      if (stack.empty()) {
        return {
          .state = json_scan_result::kind::complete,
          .top_level = top,
          .end = input.size() - bytes.size() + i + 1,
          .first_array_element = first_array_element,
        };
      }
    }
  }
  return {};
}

auto is_valid_json(std::string_view input, simdjson::dom::element_type expected)
  -> bool {
  auto parser = simdjson::dom::parser{};
  auto bytes = std::string{input};
  auto doc = parser.parse(bytes);
  return not doc.error() and doc.value_unsafe().type() == expected;
}

auto complete_json_object_line(std::string_view line) -> bool {
  auto scan = scan_json_value(line);
  return scan.state == json_scan_result::kind::complete
         and scan.top_level == '{'
         and detail::trim_front(line.substr(scan.end)).empty()
         and is_valid_json(line, simdjson::dom::element_type::OBJECT);
}

} // namespace

auto reject(std::string reason) -> read_detection_result {
  return {.state = detection_state::reject, .reason = std::move(reason)};
}

auto need_more(std::string reason) -> read_detection_result {
  return {.state = detection_state::need_more, .reason = std::move(reason)};
}

auto match(std::string reason) -> read_detection_result {
  return {.state = detection_state::match, .reason = std::move(reason)};
}

auto candidate(std::string format_name, std::string operator_name,
               std::string pipeline, uint64_t specificity,
               std::function<read_detection_result(read_detection_input)> detect)
  -> read_detection_candidate {
  return {
    .format_name = std::move(format_name),
    .operator_name = std::move(operator_name),
    .pipeline = std::move(pipeline),
    .specificity = specificity,
    .detect = std::move(detect),
  };
}

auto sample_lines(read_detection_input input, size_t max_lines) -> line_sample {
  auto result = line_sample{};
  auto rest = input.bytes;
  while (result.complete.size() < max_lines) {
    auto newline = rest.find('\n');
    if (newline == std::string_view::npos) {
      break;
    }
    auto line = rest.substr(0, newline);
    if (not line.empty() and line.back() == '\r') {
      line.remove_suffix(1);
    }
    result.complete.push_back(line);
    rest.remove_prefix(newline + 1);
  }
  if (result.complete.size() == max_lines) {
    return result;
  }
  // The final unterminated line is complete evidence at EOF and a partial
  // line while more input may arrive.
  if (input.eof) {
    if (not rest.empty()) {
      result.complete.push_back(rest);
    }
  } else {
    result.partial = rest;
  }
  return result;
}

auto json_object(read_detection_input input) -> read_detection_result {
  auto scan = scan_json_value(input.bytes);
  if (scan.state == json_scan_result::kind::invalid) {
    return reject();
  }
  if (scan.state == json_scan_result::kind::incomplete) {
    return input.eof ? reject("incomplete JSON object") : need_more();
  }
  if (scan.top_level != '{') {
    return reject();
  }
  auto rest = detail::trim_front(input.bytes.substr(scan.end));
  if (not rest.empty()) {
    return reject("trailing bytes after object");
  }
  if (not is_valid_json(input.bytes, simdjson::dom::element_type::OBJECT)) {
    return reject("invalid JSON object");
  }
  return match("top-level JSON object");
}

auto json_array(read_detection_input input) -> read_detection_result {
  auto scan = scan_json_value(input.bytes);
  if (scan.state == json_scan_result::kind::invalid) {
    return reject();
  }
  if (scan.state == json_scan_result::kind::incomplete) {
    return input.eof ? reject("incomplete JSON array") : need_more();
  }
  if (scan.top_level == '[' and scan.first_array_element == '{'
      and is_valid_json(input.bytes, simdjson::dom::element_type::ARRAY)) {
    return match("top-level array of objects");
  }
  return reject();
}

auto ndjson(read_detection_input input) -> read_detection_result {
  auto sample = sample_lines(input, 3);
  std::erase_if(sample.complete, [](std::string_view& line) {
    line = detail::trim_front(line);
    return line.empty();
  });
  // NDJSON readers consume input line by line, so every complete line must
  // be a valid JSON object on its own.
  for (auto line : sample.complete) {
    if (not complete_json_object_line(line)) {
      return reject("line is not a JSON object");
    }
  }
  // An unterminated trailing line must still be the prefix of a JSON object.
  auto partial = detail::trim_front(sample.partial);
  if (not partial.empty()) {
    auto scan = scan_json_value(partial);
    if (scan.state == json_scan_result::kind::invalid
        or scan.top_level == '[') {
      return reject("trailing line is not a JSON object prefix");
    }
    if (scan.state == json_scan_result::kind::complete
        and not detail::trim_front(partial.substr(scan.end)).empty()) {
      return reject("trailing bytes after object");
    }
  }
  if (sample.complete.empty()) {
    return input.eof ? reject() : need_more();
  }
  if (sample.complete.size() == 1 and partial.empty()) {
    // A single object could equally be a whole-input JSON document; defer to
    // the JSON object detector until more evidence arrives.
    return input.eof ? reject() : need_more();
  }
  return match("JSON object lines");
}

auto json_field(read_detection_input input, std::string_view field)
  -> read_detection_result {
  auto object = json_object(input);
  auto lines = ndjson(input);
  auto valid_json_shape = object.state == detection_state::match
                          or lines.state == detection_state::match;
  if (valid_json_shape and input.bytes.contains(field)) {
    return match(fmt::format("contains `{}`", field));
  }
  if (object.state == detection_state::need_more
      or lines.state == detection_state::need_more) {
    return need_more();
  }
  return reject();
}

auto gelf(read_detection_input input) -> read_detection_result {
  auto object = json_object(input);
  auto lines = ndjson(input);
  auto valid_json_shape = object.state == detection_state::match
                          or lines.state == detection_state::match;
  if (valid_json_shape and input.bytes.contains("\"version\"")
      and input.bytes.contains("\"host\"")
      and input.bytes.contains("\"short_message\"")) {
    return match("GELF fields");
  }
  if (object.state == detection_state::need_more
      or lines.state == detection_state::need_more) {
    return need_more();
  }
  return reject();
}

auto magic_prefix(read_detection_input input, std::string_view magic,
                  std::string reason) -> read_detection_result {
  if (input.bytes.size() < magic.size()) {
    if (input.eof or not magic.starts_with(input.bytes)) {
      return reject();
    }
    return need_more();
  }
  return input.bytes.starts_with(magic) ? match(std::move(reason)) : reject();
}

} // namespace tenzir::read_detection
