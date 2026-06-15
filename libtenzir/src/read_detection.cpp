//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/read_detection.hpp"

#include "tenzir/detail/assert.hpp"

#include <utility>

namespace tenzir {

read_detection_candidate::read_detection_candidate(
  std::string pipeline, read_detection::specificity specificity,
  std::function<read_detection_result(read_detection_input)> detect)
  : pipeline{std::move(pipeline)},
    specificity{specificity},
    detect{std::move(detect)} {
  TENZIR_ASSERT(not this->pipeline.empty());
  TENZIR_ASSERT(this->detect);
}

} // namespace tenzir

namespace tenzir::read_detection {

auto reject() -> read_detection_result {
  return {.state = read_detection_result::result_state::reject};
}

auto need_more() -> read_detection_result {
  return {.state = read_detection_result::result_state::need_more};
}

auto match() -> read_detection_result {
  return {.state = read_detection_result::result_state::match};
}

auto candidate(std::string pipeline, specificity specificity,
               std::function<read_detection_result(read_detection_input)> detect)
  -> read_detection_candidate {
  return read_detection_candidate{
    std::move(pipeline),
    specificity,
    std::move(detect),
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

auto magic_prefix(read_detection_input input, std::string_view magic)
  -> read_detection_result {
  if (input.bytes.size() < magic.size()) {
    if (input.eof or not magic.starts_with(input.bytes)) {
      return reject();
    }
    return need_more();
  }
  return input.bytes.starts_with(magic) ? match() : reject();
}

} // namespace tenzir::read_detection
