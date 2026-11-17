//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/ir.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pushdown/planner.hpp>
#include <tenzir/time.hpp>

#include <cstdint>
#include <span>
#include <string>
#include <vector>

/// The stages after `from_amazon_cloudwatch` that CloudWatch Logs computes,
/// and the request parameters that spell them.
namespace tenzir::plugins::cloudwatch {

enum class FromMode;
struct FromCloudWatchArgs;

/// Returns the mode that `args` select.
auto from_mode(FromCloudWatchArgs const& args) -> FromMode;

/// Returns the milliseconds since the epoch of `value`, as the requests take
/// them.
auto epoch_ms(time value) -> int64_t;

/// What a read spells of the stages that `from_amazon_cloudwatch` took over.
struct CloudWatchRequest {
  /// The filter pattern, which replaces none of the user's.
  Option<std::string> pattern;
  /// The `startTime` and `endTime` of the request in milliseconds since the
  /// epoch: the user's `start` and `end`, narrowed by the filters on
  /// `timestamp`. The start is inclusive, and the end is inclusive for
  /// `FilterLogEvents` in a search and exclusive for `GetLogEvents` in a
  /// replay.
  Option<int64_t> start_time;
  Option<int64_t> end_time;
  /// The log streams from the filters on `log_stream`.
  Option<std::vector<std::string>> streams;
  /// Whether no event can pass, so that the read completes without a
  /// request.
  bool empty = false;
  /// The number of events after which the read completes, from the user's
  /// `count` and the limits.
  Option<uint64_t> limit;
};

/// Returns the planner of the stages that a read with `args` takes over.
auto cloudwatch_planner(FromCloudWatchArgs args)
  -> pushdown::Planner<CloudWatchRequest>;

} // namespace tenzir::plugins::cloudwatch
