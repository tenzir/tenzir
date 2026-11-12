//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "cloudwatch/request_plan.hpp"

#include <cstdint>
#include <string>
#include <string_view>

/// The JSON payloads of the requests that `from_amazon_cloudwatch` sends,
/// which only the unit tests need.
namespace tenzir::plugins::cloudwatch {

/// The payload of the `FilterLogEvents` request after `emitted` events, with
/// the pagination `token`.
auto filter_log_events_payload(FromCloudWatchArgs const& args,
                               CloudWatchRequest const& request,
                               uint64_t emitted, std::string_view token)
  -> std::string;

/// The payload of the `GetLogEvents` request after `emitted` events, with
/// the pagination `token`.
auto get_log_events_payload(FromCloudWatchArgs const& args,
                            CloudWatchRequest const& request, uint64_t emitted,
                            std::string_view token) -> std::string;

/// The payload of the `StartLiveTail` request.
auto live_tail_payload(FromCloudWatchArgs const& args,
                       CloudWatchRequest const& request) -> std::string;

} // namespace tenzir::plugins::cloudwatch
