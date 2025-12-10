//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/detail/env.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>

#include <aws/core/Aws.h>
#include <aws/core/utils/Outcome.h>
#include <aws/logs/CloudWatchLogsClient.h>
#include <aws/logs/model/FilterLogEventsRequest.h>
#include <aws/logs/model/FilterLogEventsResult.h>

#include <string_view>

using namespace std::chrono_literals;

namespace tenzir::plugins::cloudwatch {

namespace {

/// Default poll interval for tailing logs.
static constexpr auto default_poll_interval = 1s;

struct connector_args {
  located<std::string> log_group;
  std::optional<located<std::string>> filter_pattern;

  template <class Inspector>
  friend auto inspect(Inspector& f, connector_args& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.cloudwatch.connector_args")
      .fields(f.field("log_group", x.log_group),
              f.field("filter_pattern", x.filter_pattern));
  }
};

class from_cloudwatch_operator final
  : public crtp_operator<from_cloudwatch_operator> {
public:
  from_cloudwatch_operator() = default;

  explicit from_cloudwatch_operator(connector_args args)
    : args_{std::move(args)} {
  }

  auto
  operator()(operator_control_plane& ctrl) const -> generator<table_slice> {
    // Configure AWS client
    auto config = Aws::Client::ClientConfiguration{};
    if (auto endpoint_url = detail::getenv("AWS_ENDPOINT_URL")) {
      config.endpointOverride = *endpoint_url;
    }
    if (auto endpoint_url = detail::getenv("AWS_ENDPOINT_URL_LOGS")) {
      config.endpointOverride = *endpoint_url;
    }
    config.allowSystemProxy = true;

    auto client = Aws::CloudWatchLogs::CloudWatchLogsClient{config};

    // Define output schema
    const auto output_type = type{
      "tenzir.cloudwatch",
      record_type{
        {"timestamp", time_type{}},
        {"ingestion_time", time_type{}},
        {"log_group", string_type{}},
        {"log_stream", string_type{}},
        {"message", string_type{}},
        {"event_id", string_type{}},
      },
    };
    auto builder = series_builder{output_type};

    co_yield {};

    // Start from now - poll_interval to catch recent events
    auto start_time
      = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch() - 60s)
          .count();

    auto last_yield_time = std::chrono::steady_clock::now();

    while (true) {
      auto request = Aws::CloudWatchLogs::Model::FilterLogEventsRequest{};
      request.SetLogGroupName(Aws::String{args_.log_group.inner.c_str()});
      request.SetStartTime(start_time);

      // Set optional filter pattern
      if (args_.filter_pattern) {
        request.SetFilterPattern(
          Aws::String{args_.filter_pattern->inner.c_str()});
      }

      // Keep fetching while there are more results
      Aws::String next_token;
      do {
        if (! next_token.empty()) {
          request.SetNextToken(next_token);
        }

        auto outcome = client.FilterLogEvents(request);

        if (! outcome.IsSuccess()) {
          diagnostic::error("failed to filter CloudWatch log events")
            .primary(args_.log_group.source)
            .note("{}", outcome.GetError().GetMessage())
            .emit(ctrl.diagnostics());
          co_return;
        }

        const auto& result = outcome.GetResult();
        const auto& events = result.GetEvents();

        for (const auto& event : events) {
          auto row = builder.record();

          // Convert milliseconds to time type
          auto ts = time{std::chrono::duration_cast<duration>(
            std::chrono::milliseconds{event.GetTimestamp()})};
          auto ing_ts = time{std::chrono::duration_cast<duration>(
            std::chrono::milliseconds{event.GetIngestionTime()})};

          row.field("timestamp").data(ts);
          row.field("ingestion_time").data(ing_ts);
          row.field("log_group").data(args_.log_group.inner);
          row.field("log_stream").data(std::string{event.GetLogStreamName()});
          row.field("message").data(std::string{event.GetMessage()});
          row.field("event_id").data(std::string{event.GetEventId()});

          // Track the latest timestamp for next poll
          if (event.GetTimestamp() >= start_time) {
            start_time = event.GetTimestamp() + 1;
          }
        }

        next_token = result.GetNextToken();
      } while (! next_token.empty());

      // Yield if we have data
      const auto now = std::chrono::steady_clock::now();
      if (builder.length() > 0 && now - last_yield_time >= 1s) {
        co_yield builder.finish_assert_one_slice();
        last_yield_time = now;
      } else {
        co_yield {};
      }

      // Wait before next poll
      std::this_thread::sleep_for(default_poll_interval);
    }
  }

  auto detached() const -> bool override {
    return true;
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto
  optimize(const expression&, event_order) const -> optimize_result override {
    return do_not_optimize(*this);
  }

  auto name() const -> std::string override {
    return "from_cloudwatch";
  }

  friend auto inspect(auto& f, from_cloudwatch_operator& x) -> bool {
    return f.object(x)
      .pretty_name("from_cloudwatch_operator")
      .fields(f.field("args", x.args_));
  }

private:
  connector_args args_;
};

} // namespace
} // namespace tenzir::plugins::cloudwatch
