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
#include <aws/logs/model/StartLiveTailHandler.h>
#include <aws/logs/model/StartLiveTailRequest.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <string_view>
#include <thread>

using namespace std::chrono_literals;

namespace tenzir::plugins::cloudwatch {

namespace {

/// Default poll interval when using fallback mode.
static constexpr auto default_poll_interval = 1s;

/// A wrapper around CloudWatch Logs Live Tail.
class cloudwatch_live_tail {
public:
  struct log_event {
    std::string log_group;
    std::string log_stream;
    std::string message;
    int64_t timestamp;
    int64_t ingestion_time;
  };

  explicit cloudwatch_live_tail(
    std::vector<std::string> log_group_identifiers,
    std::optional<std::string> filter_pattern,
    std::optional<std::vector<std::string>> log_stream_names,
    std::optional<std::vector<std::string>> log_stream_prefixes)
    : log_group_identifiers_{std::move(log_group_identifiers)},
      filter_pattern_{std::move(filter_pattern)},
      log_stream_names_{std::move(log_stream_names)},
      log_stream_prefixes_{std::move(log_stream_prefixes)},
      running_{false},
      error_occurred_{false} {
    auto config = Aws::Client::ClientConfiguration{};
    if (auto endpoint_url = detail::getenv("AWS_ENDPOINT_URL")) {
      config.endpointOverride = *endpoint_url;
    }
    if (auto endpoint_url = detail::getenv("AWS_ENDPOINT_URL_LOGS")) {
      config.endpointOverride = *endpoint_url;
    }
    config.allowSystemProxy = true;
    client_ = Aws::CloudWatchLogs::CloudWatchLogsClient{config};
  }

  ~cloudwatch_live_tail() {
    stop();
  }

  auto start() -> std::optional<std::string> {
    if (running_) {
      return "Live tail already running";
    }
    running_ = true;
    error_occurred_ = false;
    error_message_.clear();

    // Start the streaming thread
    stream_thread_ = std::thread([this]() {
      run_live_tail();
    });

    return std::nullopt;
  }

  auto stop() -> void {
    running_ = false;
    if (stream_thread_.joinable()) {
      stream_thread_.join();
    }
  }

  auto pop_events() -> std::vector<log_event> {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    std::vector<log_event> events;
    while (! event_queue_.empty()) {
      events.push_back(std::move(event_queue_.front()));
      event_queue_.pop();
    }
    return events;
  }

  auto has_error() const -> bool {
    return error_occurred_;
  }

  auto get_error() const -> std::string {
    return error_message_;
  }

  auto is_running() const -> bool {
    return running_;
  }

private:
  auto run_live_tail() -> void {
    auto request = Aws::CloudWatchLogs::Model::StartLiveTailRequest{};

    // Set log group identifiers
    Aws::Vector<Aws::String> log_groups;
    for (const auto& lg : log_group_identifiers_) {
      log_groups.push_back(Aws::String{lg.c_str()});
    }
    request.SetLogGroupIdentifiers(std::move(log_groups));

    // Set optional filter pattern
    if (filter_pattern_) {
      request.SetLogEventFilterPattern(Aws::String{filter_pattern_->c_str()});
    }

    // Set optional log stream names
    if (log_stream_names_ && ! log_stream_names_->empty()) {
      Aws::Vector<Aws::String> streams;
      for (const auto& s : *log_stream_names_) {
        streams.push_back(Aws::String{s.c_str()});
      }
      request.SetLogStreamNames(std::move(streams));
    }

    // Set optional log stream prefixes
    if (log_stream_prefixes_ && ! log_stream_prefixes_->empty()) {
      Aws::Vector<Aws::String> prefixes;
      for (const auto& p : *log_stream_prefixes_) {
        prefixes.push_back(Aws::String{p.c_str()});
      }
      request.SetLogStreamNamePrefixes(std::move(prefixes));
    }

    // Create the handler for streaming events
    Aws::CloudWatchLogs::Model::StartLiveTailHandler handler;

    handler.SetLiveTailSessionStartCallback(
      [](
        const Aws::CloudWatchLogs::Model::LiveTailSessionStart& session_start) {
        TENZIR_DEBUG("Live tail session started");
      });

    handler.SetLiveTailSessionUpdateCallback(
      [this](const Aws::CloudWatchLogs::Model::LiveTailSessionUpdate& update) {
        const auto& results = update.GetSessionResults();
        std::lock_guard<std::mutex> lock(queue_mutex_);
        for (const auto& result : results) {
          log_event event;
          event.log_group = result.GetLogGroupIdentifier();
          event.log_stream = result.GetLogStreamName();
          event.message = result.GetMessage();
          event.timestamp = result.GetTimestamp();
          event.ingestion_time = result.GetIngestionTime();
          event_queue_.push(std::move(event));
        }
      });

    handler.SetOnErrorCallback(
      [this](
        const Aws::Client::AWSError<Aws::CloudWatchLogs::CloudWatchLogsErrors>&
          error) {
        error_occurred_ = true;
        error_message_ = error.GetMessage();
        TENZIR_ERROR("CloudWatch Live Tail error: {}", error_message_);
      });

    // Start the streaming request
    client_.StartLiveTailAsync(request, handler);

    // Keep the thread alive while running
    while (running_ && ! error_occurred_) {
      std::this_thread::sleep_for(100ms);
    }
  }

  std::vector<std::string> log_group_identifiers_;
  std::optional<std::string> filter_pattern_;
  std::optional<std::vector<std::string>> log_stream_names_;
  std::optional<std::vector<std::string>> log_stream_prefixes_;

  Aws::CloudWatchLogs::CloudWatchLogsClient client_;
  std::thread stream_thread_;
  std::atomic<bool> running_;
  std::atomic<bool> error_occurred_;
  std::string error_message_;

  std::queue<log_event> event_queue_;
  std::mutex queue_mutex_;
};

struct connector_args {
  located<std::string> log_group;
  std::optional<located<std::string>> filter_pattern;
  std::optional<located<std::vector<std::string>>> log_stream_names;
  std::optional<located<std::vector<std::string>>> log_stream_prefixes;

  template <class Inspector>
  friend auto inspect(Inspector& f, connector_args& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.cloudwatch.connector_args")
      .fields(f.field("log_group", x.log_group),
              f.field("filter_pattern", x.filter_pattern),
              f.field("log_stream_names", x.log_stream_names),
              f.field("log_stream_prefixes", x.log_stream_prefixes));
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
    auto builder = series_builder{output_type};

    // Prepare log group identifiers (needs to be ARN format for API)
    std::vector<std::string> log_groups;
    log_groups.push_back(args_.log_group.inner);

    // Create the live tail instance
    auto live_tail = cloudwatch_live_tail{
      std::move(log_groups),
      args_.filter_pattern ? std::make_optional(args_.filter_pattern->inner)
                           : std::nullopt,
      args_.log_stream_names ? std::make_optional(args_.log_stream_names->inner)
                             : std::nullopt,
      args_.log_stream_prefixes
        ? std::make_optional(args_.log_stream_prefixes->inner)
        : std::nullopt};

    // Start the live tail
    if (auto err = live_tail.start()) {
      diagnostic::error("failed to start CloudWatch Live Tail")
        .primary(args_.log_group.source)
        .note("{}", *err)
        .emit(ctrl.diagnostics());
      co_return;
    }

    co_yield {};

    auto last_yield_time = std::chrono::steady_clock::now();

    while (live_tail.is_running()) {
      if (live_tail.has_error()) {
        diagnostic::error("CloudWatch Live Tail error")
          .primary(args_.log_group.source)
          .note("{}", live_tail.get_error())
          .emit(ctrl.diagnostics());
        co_return;
      }

      // Pop events from the queue
      auto events = live_tail.pop_events();

      for (const auto& event : events) {
        auto row = builder.record();
        // Convert milliseconds to time type
        auto ts = time{std::chrono::duration_cast<duration>(
          std::chrono::milliseconds{event.timestamp})};
        auto ing_ts = time{std::chrono::duration_cast<duration>(
          std::chrono::milliseconds{event.ingestion_time})};

        row.field("timestamp").data(ts);
        row.field("ingestion_time").data(ing_ts);
        row.field("log_group").data(event.log_group);
        row.field("log_stream").data(event.log_stream);
        row.field("message").data(event.message);
      }

      // Yield if we have data and timeout expired
      const auto now = std::chrono::steady_clock::now();
      if (builder.length() > 0 && now - last_yield_time >= 1s) {
        co_yield builder.finish_assert_one_slice();
        last_yield_time = now;
      } else {
        co_yield {};
      }
    }

    // Yield any remaining data
    if (builder.length() > 0) {
      co_yield builder.finish_assert_one_slice();
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
