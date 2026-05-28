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

namespace tenzir::read_detection {

auto reject(std::string reason = {}) -> read_detection_result;

auto need_more(std::string reason = {}) -> read_detection_result;

auto match(uint64_t confidence, std::string reason = {})
  -> read_detection_result;

auto candidate(std::string format_name, std::string operator_name,
               std::string pipeline, int64_t priority,
               std::function<read_detection_result(read_detection_input)> detect,
               std::vector<std::string> after = {}) -> read_detection_candidate;

auto json_object(read_detection_input input) -> read_detection_result;

auto json_array(read_detection_input input) -> read_detection_result;

auto ndjson(read_detection_input input) -> read_detection_result;

auto json_field(read_detection_input input, std::string_view field,
                uint64_t confidence) -> read_detection_result;

auto gelf(read_detection_input input) -> read_detection_result;

auto zeek_tsv(read_detection_input input) -> read_detection_result;

auto syslog(read_detection_input input) -> read_detection_result;

auto xsv(read_detection_input input, char sep, uint64_t confidence)
  -> read_detection_result;

auto kv(read_detection_input input) -> read_detection_result;

auto yaml(read_detection_input input) -> read_detection_result;

auto pcap(read_detection_input input) -> read_detection_result;

auto magic_prefix(read_detection_input input, std::string_view magic,
                  uint64_t confidence, std::string reason = {})
  -> read_detection_result;

} // namespace tenzir::read_detection
