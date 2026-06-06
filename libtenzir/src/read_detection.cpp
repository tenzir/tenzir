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

#include <algorithm>
#include <array>
#include <ranges>
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

auto match(uint64_t confidence, std::string reason) -> read_detection_result {
  return {
    .state = detection_state::match,
    .confidence = confidence,
    .reason = std::move(reason),
  };
}

auto candidate(std::string format_name, std::string operator_name,
               std::string pipeline, int64_t priority,
               std::function<read_detection_result(read_detection_input)> detect,
               std::vector<std::string> after) -> read_detection_candidate {
  return {
    .format_name = std::move(format_name),
    .operator_name = std::move(operator_name),
    .pipeline = std::move(pipeline),
    .after = std::move(after),
    .priority = priority,
    .detect = std::move(detect),
  };
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
  return match(70, "top-level JSON object");
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
    return match(75, "top-level array of objects");
  }
  return reject();
}

auto ndjson(read_detection_input input) -> read_detection_result {
  auto lines = detail::split_lines(input.bytes, 3);
  std::erase_if(lines, [](std::string_view& line) {
    line = detail::trim_front(line);
    return line.empty();
  });
  if (lines.empty()) {
    return input.eof ? reject() : need_more();
  }
  if (lines.size() >= 2 and complete_json_object_line(lines[0])
      and complete_json_object_line(lines[1])) {
    return match(82, "multiple JSON object lines");
  }
  auto bytes = detail::trim_front(input.bytes);
  auto scan = scan_json_value(bytes);
  if (scan.state == json_scan_result::kind::complete
      and scan.top_level == '{') {
    auto first_object = bytes.substr(0, scan.end);
    if (not is_valid_json(first_object, simdjson::dom::element_type::OBJECT)) {
      return reject("invalid JSON object");
    }
    auto rest = detail::trim_front(bytes.substr(scan.end));
    if (not rest.empty()) {
      return match(82, "JSON object followed by more input");
    }
  }
  if (input.eof) {
    return reject();
  }
  return need_more();
}

auto json_field(read_detection_input input, std::string_view field,
                uint64_t confidence) -> read_detection_result {
  auto object = json_object(input);
  auto lines = ndjson(input);
  auto valid_json_shape = object.state == detection_state::match
                          or lines.state == detection_state::match;
  if (valid_json_shape and input.bytes.contains(field)) {
    return match(confidence, fmt::format("contains `{}`", field));
  }
  if (object.state == detection_state::need_more
      or lines.state == detection_state::need_more) {
    return need_more();
  }
  if (object.state == detection_state::reject
      and lines.state == detection_state::reject) {
    return reject();
  }
  return reject();
}

auto gelf(read_detection_input input) -> read_detection_result {
  auto object = json_object(input);
  auto lines = ndjson(input);
  if (object.state == detection_state::reject
      and lines.state == detection_state::reject) {
    return reject();
  }
  auto valid_json_shape = object.state == detection_state::match
                          or lines.state == detection_state::match;
  if (valid_json_shape and input.bytes.contains("\"version\"")
      and input.bytes.contains("\"host\"")
      and input.bytes.contains("\"short_message\"")) {
    return match(90, "GELF fields");
  }
  if (object.state == detection_state::need_more
      or lines.state == detection_state::need_more) {
    return need_more();
  }
  return reject();
}

auto zeek_tsv(read_detection_input input) -> read_detection_result {
  auto bytes = detail::trim_front(input.bytes);
  if (bytes.starts_with("#separator") or bytes.starts_with("#fields")
      or bytes.starts_with("#types")) {
    return match(95, "Zeek TSV header");
  }
  return bytes.size() < 10 and not input.eof ? need_more() : reject();
}

auto syslog(read_detection_input input) -> read_detection_result {
  auto bytes = detail::trim_front(input.bytes);
  if (bytes.empty()) {
    return input.eof ? reject() : need_more();
  }
  if (bytes.front() == '<') {
    auto end = bytes.find('>');
    if (end != std::string_view::npos and end > 1 and end <= 4) {
      auto pri = bytes.substr(1, end - 1);
      if (std::ranges::all_of(pri, [](char c) {
            return c >= '0' and c <= '9';
          })) {
        return match(50, "syslog priority prefix");
      }
    }
  }
  return bytes.size() < 32 and not input.eof ? need_more() : reject();
}

auto xsv(read_detection_input input, char sep, uint64_t confidence)
  -> read_detection_result {
  if (not detail::is_valid_utf8(input.bytes)) {
    if (not input.eof and detail::is_valid_utf8_prefix(input.bytes)) {
      return need_more("partial UTF-8 sequence");
    }
    return reject();
  }
  auto quoting = detail::quoting_escaping_policy{
    .quotes = "\"'",
    .backslashes_escape = true,
    .doubled_quotes_escape = true,
  };
  auto count_separators = [&](std::string_view line) {
    auto count = size_t{0};
    auto offset = size_t{0};
    while (offset < line.size()) {
      auto next = quoting.find_not_in_quotes(line, sep, offset);
      if (next == std::string_view::npos) {
        break;
      }
      ++count;
      offset = next + 1;
    }
    return count;
  };
  auto lines = detail::split_lines(input.bytes, 3);
  std::erase_if(lines, [](std::string_view& line) {
    line = detail::trim_front(line);
    return line.empty();
  });
  auto counts = std::vector<size_t>{};
  for (auto line : lines) {
    if (line.starts_with("#")) {
      continue;
    }
    counts.push_back(count_separators(line));
  }
  if (counts.size() >= 2 and counts[0] > 0
      and std::ranges::all_of(counts, [&](size_t count) {
            return count == counts[0];
          })) {
    return match(confidence, "stable delimiter counts");
  }
  if (input.eof and counts.size() == 1 and counts[0] > 0) {
    return match(confidence - 5, "single delimited row");
  }
  return input.eof ? reject() : need_more();
}

auto kv(read_detection_input input) -> read_detection_result {
  auto lines = detail::split_lines(input.bytes, 2);
  std::erase_if(lines, [](std::string_view& line) {
    line = detail::trim_front(line);
    return line.empty();
  });
  if (lines.empty()) {
    return input.eof ? reject() : need_more();
  }
  auto has_kv = [](std::string_view line) {
    auto eq = line.find('=');
    return eq != std::string_view::npos and eq > 0 and eq + 1 < line.size();
  };
  if (std::ranges::all_of(lines, has_kv)) {
    return match(65, "key-value assignments");
  }
  return input.eof ? reject() : need_more();
}

auto yaml(read_detection_input input) -> read_detection_result {
  if (not detail::is_valid_utf8(input.bytes)) {
    if (not input.eof and detail::is_valid_utf8_prefix(input.bytes)) {
      return need_more("partial UTF-8 sequence");
    }
    return reject();
  }
  auto bytes = detail::trim_front(input.bytes);
  if (bytes.empty()) {
    return input.eof ? reject() : need_more();
  }
  if (bytes.front() == '{' or bytes.front() == '[') {
    return reject("JSON-compatible YAML is left to JSON detectors");
  }
  auto lines = detail::split_lines(bytes, 3);
  std::erase_if(lines, [](std::string_view& line) {
    line = detail::trim_front(line);
    return line.empty();
  });
  if (std::ranges::any_of(lines, [](std::string_view line) {
        auto colon = line.find(':');
        return colon != std::string_view::npos and colon > 0;
      })) {
    return match(45, "YAML mapping");
  }
  return input.eof ? reject() : need_more();
}

auto pcap(read_detection_input input) -> read_detection_result {
  constexpr auto magics = std::array{
    std::string_view{"\xd4\xc3\xb2\xa1", 4},
    std::string_view{"\xa1\xb2\xc3\xd4", 4},
    std::string_view{"\x4d\x3c\xb2\xa1", 4},
    std::string_view{"\xa1\xb2\x3c\x4d", 4},
    std::string_view{"\x0a\x0d\x0d\x0a", 4},
  };
  if (input.bytes.size() < 4) {
    return input.eof ? reject() : need_more();
  }
  return std::ranges::any_of(magics,
                             [&](std::string_view magic) {
                               return input.bytes.starts_with(magic);
                             })
           ? match(100, "pcap magic")
           : reject();
}

auto magic_prefix(read_detection_input input, std::string_view magic,
                  uint64_t confidence, std::string reason)
  -> read_detection_result {
  if (input.bytes.size() < magic.size()) {
    return input.eof ? reject() : need_more();
  }
  return input.bytes.starts_with(magic) ? match(confidence, std::move(reason))
                                        : reject();
}

} // namespace tenzir::read_detection
