//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/detail/env.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <aws/core/Aws.h>
#include <aws/core/http/HttpTypes.h>
#include <aws/core/utils/Outcome.h>
#include <aws/logs/CloudWatchLogsClient.h>
#include <aws/logs/model/FilterLogEventsRequest.h>
#include <aws/logs/model/FilterLogEventsResult.h>

#include <string_view>
#include <thread>

using namespace std::chrono_literals;

namespace tenzir::plugins::cloudwatch {

namespace {

/// Default poll interval for tailing logs.
static constexpr auto default_poll_interval = 1s;

struct connector_args {
  located<std::string> log_group;
  std::optional<located<std::string>> filter_pattern;
  std::optional<time> from;

  template <class Inspector>
  friend auto inspect(Inspector& f, connector_args& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.cloudwatch.connector_args")
      .fields(f.field("log_group", x.log_group),
              f.field("filter_pattern", x.filter_pattern),
              f.field("from", x.from));
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
    // Use HTTP/1.1 to avoid potential HTTP/2 issues with event streaming
    config.version = Aws::Http::Version::HTTP_VERSION_NONE;

    auto client = Aws::CloudWatchLogs::CloudWatchLogsClient{config};

    // Define output schema (matches AWS CloudWatch log event format)
    const auto output_type = type{
      "tenzir.cloudwatch",
      record_type{
        {"logStreamName", string_type{}},
        {"timestamp", int64_type{}},
        {"message", string_type{}},
        {"ingestionTime", int64_type{}},
        {"eventId", string_type{}},
      },
    };

    // Use FilterLogEvents with polling
    for (auto&& slice : run_filter_log_events(ctrl, client, output_type)) {
      co_yield std::move(slice);
    }
  }

private:
  auto run_filter_log_events(operator_control_plane& ctrl,
                             Aws::CloudWatchLogs::CloudWatchLogsClient& client,
                             const type& output_type) const
    -> generator<table_slice> {
    auto builder = series_builder{output_type};

    co_yield {};

    // Start from the provided time, or now - 60s by default
    auto start_time = int64_t{};
    if (args_.from) {
      start_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                     args_.from->time_since_epoch())
                     .count();
    } else {
      start_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::system_clock::now().time_since_epoch() - 60s)
                     .count();
    }

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

          row.field("logStreamName")
            .data(std::string(event.GetLogStreamName().c_str()));
          row.field("timestamp").data(event.GetTimestamp());
          row.field("message").data(std::string(event.GetMessage().c_str()));
          row.field("ingestionTime").data(event.GetIngestionTime());
          row.field("eventId").data(std::string(event.GetEventId().c_str()));

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

public:
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

class plugin final : public virtual operator_plugin2<from_cloudwatch_operator> {
public:
  auto make(operator_factory_plugin::invocation inv,
            session ctx) const -> failure_or<operator_ptr> override {
    auto args = connector_args{};
    TRY(argument_parser2::operator_(this->name())
          .positional("log_group", args.log_group)
          .named("filter", args.filter_pattern)
          .named("from", args.from)
          .parse(inv, ctx));

    if (args.log_group.inner.empty()) {
      diagnostic::error("log_group must not be empty")
        .primary(args.log_group.source)
        .hint("provide a CloudWatch log group name or ARN")
        .emit(ctx);
      return failure::promise();
    }

    return std::make_unique<from_cloudwatch_operator>(std::move(args));
  }
};

} // namespace

} // namespace tenzir::plugins::cloudwatch

TENZIR_REGISTER_PLUGIN(tenzir::plugins::cloudwatch::plugin)
