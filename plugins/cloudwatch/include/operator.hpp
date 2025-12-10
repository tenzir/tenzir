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
#include <aws/core/http/HttpTypes.h>
#include <aws/core/utils/Outcome.h>
#include <aws/logs/CloudWatchLogsClient.h>
#include <aws/logs/model/FilterLogEventsRequest.h>
#include <aws/logs/model/FilterLogEventsResult.h>
#include <aws/logs/model/StartLiveTailHandler.h>
#include <aws/logs/model/StartLiveTailInitialResponse.h>
#include <aws/logs/model/StartLiveTailRequest.h>

#include <atomic>
#include <mutex>
#include <queue>
#include <string_view>
#include <thread>

using namespace std::chrono_literals;

namespace tenzir::plugins::cloudwatch {

namespace {

/// Default poll interval for tailing logs (non-live mode).
static constexpr auto default_poll_interval = 1s;

/// Structure to hold a log event from Live Tail.
struct live_tail_event {
  int64_t timestamp;
  int64_t ingestion_time;
  std::string log_group;
  std::string log_stream;
  std::string message;
};

struct connector_args {
  located<std::string> log_group;
  std::optional<located<std::string>> filter_pattern;
  bool live = false;

  template <class Inspector>
  friend auto inspect(Inspector& f, connector_args& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.cloudwatch.connector_args")
      .fields(f.field("log_group", x.log_group),
              f.field("filter_pattern", x.filter_pattern),
              f.field("live", x.live));
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

    // Define output schema
    const auto output_type = type{
      "tenzir.cloudwatch",
      record_type{
        {"timestamp", time_type{}},
        {"ingestion_time", time_type{}},
        {"log_group", string_type{}},
        {"log_stream", string_type{}},
        {"message", string_type{}},
      },
    };

    if (args_.live) {
      // Use StartLiveTail for real-time streaming
      for (auto&& slice : run_live_tail(ctrl, client, output_type)) {
        co_yield std::move(slice);
      }
    } else {
      // Use FilterLogEvents with polling
      for (auto&& slice : run_filter_log_events(ctrl, client, output_type)) {
        co_yield std::move(slice);
      }
    }
  }

private:
  auto run_live_tail(operator_control_plane& ctrl,
                     Aws::CloudWatchLogs::CloudWatchLogsClient& client,
                     const type& output_type) const -> generator<table_slice> {
    auto builder = series_builder{output_type};

    // Thread-safe event queue for receiving events from the handler
    auto event_queue = std::queue<live_tail_event>{};
    auto queue_mutex = std::mutex{};
    auto running = std::atomic<bool>{true};
    auto error_message = std::string{};
    auto has_error = std::atomic<bool>{false};

    // Build the request
    auto request = Aws::CloudWatchLogs::Model::StartLiveTailRequest{};

    // Set log group identifiers - needs to be ARN or log group name
    Aws::Vector<Aws::String> log_groups;
    log_groups.push_back(Aws::String{args_.log_group.inner.c_str()});
    request.SetLogGroupIdentifiers(std::move(log_groups));

    // Set optional filter pattern
    if (args_.filter_pattern) {
      request.SetLogEventFilterPattern(
        Aws::String{args_.filter_pattern->inner.c_str()});
    }

    // Set up the event stream handler
    Aws::CloudWatchLogs::Model::StartLiveTailHandler handler;

    handler.SetInitialResponseCallback(
      [](const Aws::CloudWatchLogs::Model::StartLiveTailInitialResponse&) {
        TENZIR_DEBUG("CloudWatch Live Tail initial response received");
      });

    handler.SetLiveTailSessionStartCallback(
      [](const Aws::CloudWatchLogs::Model::LiveTailSessionStart&) {
        TENZIR_DEBUG("CloudWatch Live Tail session started");
      });

    handler.SetLiveTailSessionUpdateCallback(
      [&event_queue, &queue_mutex, &running](
        const Aws::CloudWatchLogs::Model::LiveTailSessionUpdate& session_update) {
        if (! running.load()) {
          return;
        }
        const auto& results = session_update.GetSessionResults();
        std::lock_guard<std::mutex> lock(queue_mutex);
        for (const auto& event : results) {
          live_tail_event evt;
          evt.timestamp = event.GetTimestamp();
          evt.ingestion_time = event.GetIngestionTime();
          evt.log_group = std::string(event.GetLogGroupIdentifier().c_str());
          evt.log_stream = std::string(event.GetLogStreamName().c_str());
          evt.message = std::string(event.GetMessage().c_str());
          event_queue.push(std::move(evt));
        }
      });

    handler.SetOnErrorCallback(
      [&has_error, &error_message, &running](
        const Aws::Client::AWSError<Aws::CloudWatchLogs::CloudWatchLogsErrors>&
          error) {
        error_message = std::string(error.GetMessage().c_str());
        has_error.store(true);
        running.store(false);
        TENZIR_ERROR("CloudWatch Live Tail error: {}", error_message);
      });

    // Set the handler on the request
    request.SetEventStreamHandler(handler);

    co_yield {};

    TENZIR_DEBUG("starting CloudWatch Live Tail session");

    // Start the live tail in a separate thread since it blocks
    auto stream_thread = std::thread([&client, &request, &running]() {
      // Call StartLiveTail - this blocks until the stream ends
      client.StartLiveTail(request);
      running.store(false);
    });

    auto last_yield_time = std::chrono::steady_clock::now();

    // Process events from the queue
    while (running.load() || ! event_queue.empty()) {
      if (has_error.load()) {
        diagnostic::error("CloudWatch Live Tail error")
          .primary(args_.log_group.source)
          .note("{}", error_message)
          .emit(ctrl.diagnostics());
        break;
      }

      // Drain events from the queue
      {
        std::lock_guard<std::mutex> lock(queue_mutex);
        while (! event_queue.empty()) {
          const auto& event = event_queue.front();
          auto row = builder.record();

          auto ts = time{std::chrono::duration_cast<duration>(
            std::chrono::milliseconds{event.timestamp})};
          auto ing_ts = time{std::chrono::duration_cast<duration>(
            std::chrono::milliseconds{event.ingestion_time})};

          row.field("timestamp").data(ts);
          row.field("ingestion_time").data(ing_ts);
          row.field("log_group").data(event.log_group);
          row.field("log_stream").data(event.log_stream);
          row.field("message").data(event.message);

          event_queue.pop();
        }
      }

      // Yield if we have data and timeout expired
      const auto now = std::chrono::steady_clock::now();
      if (builder.length() > 0 && now - last_yield_time >= 1s) {
        co_yield builder.finish_assert_one_slice();
        last_yield_time = now;
      } else {
        co_yield {};
      }

      // Small sleep to avoid busy-waiting
      std::this_thread::sleep_for(100ms);
    }

    // Clean up
    running.store(false);
    if (stream_thread.joinable()) {
      stream_thread.join();
    }

    // Yield any remaining data
    if (builder.length() > 0) {
      co_yield builder.finish_assert_one_slice();
    }
  }

  auto run_filter_log_events(operator_control_plane& ctrl,
                             Aws::CloudWatchLogs::CloudWatchLogsClient& client,
                             const type& output_type) const
    -> generator<table_slice> {
    auto builder = series_builder{output_type};

    co_yield {};

    // Start from now - 60s to catch recent events
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
          row.field("log_stream")
            .data(std::string(event.GetLogStreamName().c_str()));
          row.field("message").data(std::string(event.GetMessage().c_str()));

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

} // namespace
} // namespace tenzir::plugins::cloudwatch
