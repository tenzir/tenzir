//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "io/prometheus/write/v2/types.pb.h"
#include "prometheus/remote.pb.h"

#include <tenzir/async/notify.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/http.hpp>
#include <tenzir/http_pool.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/secret_resolution.hpp>
#include <tenzir/tls_options.hpp>
#include <tenzir/to_string.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/try.hpp>
#include <tenzir/version.hpp>

#include <arrow/util/compression.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cmath>
#include <exception>
#include <iterator>
#include <limits>
#include <map>
#include <ranges>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace tenzir::plugins::prometheus {

namespace {

using namespace std::string_view_literals;

constexpr auto default_protobuf_message = "prometheus.WriteRequest";
constexpr auto v2_protobuf_message = "io.prometheus.write.v2.Request";
constexpr auto v1_content_type = "application/x-protobuf";
constexpr auto v2_content_type
  = "application/x-protobuf;proto=io.prometheus.write.v2.Request";
constexpr auto max_uncompressed_bytes_default = uint64_t{4 * 1024 * 1024};

using Headers = std::vector<std::pair<std::string, std::string>>;

struct ToPrometheusArgs {
  located<secret> url;
  located<std::string> protobuf_message{
    std::string{default_protobuf_message},
    location::unknown,
  };
  ast::expression name = ast::root_field{
    ast::identifier{"metric", location::unknown},
    true,
  };
  ast::expression value = ast::root_field{
    ast::identifier{"value", location::unknown},
    true,
  };
  ast::expression timestamp = ast::root_field{
    ast::identifier{"timestamp", location::unknown},
    true,
  };
  ast::expression labels = ast::root_field{
    ast::identifier{"labels", location::unknown},
    true,
  };
  ast::expression type = ast::root_field{
    ast::identifier{"type", location::unknown},
    true,
  };
  ast::expression help = ast::root_field{
    ast::identifier{"help", location::unknown},
    true,
  };
  ast::expression unit = ast::root_field{
    ast::identifier{"unit", location::unknown},
    true,
  };
  ast::expression family = ast::root_field{
    ast::identifier{"family", location::unknown},
    true,
  };
  ast::expression start_timestamp = ast::root_field{
    ast::identifier{"start_timestamp", location::unknown},
    true,
  };
  Option<located<data>> headers;
  Option<located<data>> tls;
  Option<located<duration>> timeout;
  Option<located<duration>> connection_timeout;
  Option<located<uint64_t>> max_retry_count;
  Option<located<duration>> retry_delay;
  located<duration> flush_interval{std::chrono::seconds{1}, location::unknown};
  located<uint64_t> max_samples_per_request{2000, location::unknown};
  located<uint64_t> max_uncompressed_bytes{
    max_uncompressed_bytes_default,
    location::unknown,
  };
  bool sanitize_names = true;
  location operator_location;
};

enum class Protocol {
  v1,
  v2,
};

enum class MetricType {
  unknown,
  counter,
  gauge,
  histogram,
  gaugehistogram,
  summary,
  info,
  stateset,
};

struct Sample {
  double value = 0.0;
  int64_t timestamp_ms = 0;
  int64_t start_timestamp_ms = 0;
};

struct Metadata {
  MetricType type = MetricType::unknown;
  std::string family;
  std::string help;
  std::string unit;
};

struct Series {
  std::vector<std::pair<std::string, std::string>> labels;
  std::vector<Sample> samples;
  Metadata metadata;
};

auto lower_ascii(std::string_view input) -> std::string {
  auto result = std::string{input};
  std::ranges::transform(result, result.begin(), [](unsigned char c) {
    return static_cast<char>(detail::ascii_tolower(c));
  });
  return result;
}

auto is_reserved_header(std::string_view name) -> bool {
  constexpr auto reserved = std::array{
    "content-encoding"sv,
    "content-length"sv,
    "content-type"sv,
    "user-agent"sv,
    "x-prometheus-remote-write-version"sv,
  };
  return std::ranges::any_of(reserved, [name](std::string_view header) {
    return detail::ascii_icase_equal(name, header);
  });
}

auto sanitize_metric_name(std::string name) -> std::string {
  if (name.empty()) {
    return name;
  }
  const auto first_ok = [](unsigned char c) {
    return std::isalpha(c) or c == '_' or c == ':';
  };
  const auto rest_ok = [](unsigned char c) {
    return std::isalnum(c) or c == '_' or c == ':';
  };
  if (not first_ok(static_cast<unsigned char>(name.front()))) {
    name.front() = '_';
  }
  for (auto& c : name | std::views::drop(1)) {
    if (not rest_ok(static_cast<unsigned char>(c))) {
      c = '_';
    }
  }
  return name;
}

auto sanitize_label_name(std::string name) -> std::string {
  if (name.empty()) {
    return name;
  }
  const auto first_ok = [](unsigned char c) {
    return std::isalpha(c) or c == '_';
  };
  const auto rest_ok = [](unsigned char c) {
    return std::isalnum(c) or c == '_';
  };
  if (not first_ok(static_cast<unsigned char>(name.front()))) {
    name.front() = '_';
  }
  for (auto& c : name | std::views::drop(1)) {
    if (not rest_ok(static_cast<unsigned char>(c))) {
      c = '_';
    }
  }
  return name;
}

auto valid_metric_name(std::string_view name) -> bool {
  if (name.empty()) {
    return false;
  }
  const auto first = static_cast<unsigned char>(name.front());
  if (not(std::isalpha(first) or first == '_' or first == ':')) {
    return false;
  }
  return std::ranges::all_of(name | std::views::drop(1), [](unsigned char c) {
    return std::isalnum(c) or c == '_' or c == ':';
  });
}

auto valid_label_name(std::string_view name) -> bool {
  if (name.empty()) {
    return false;
  }
  const auto first = static_cast<unsigned char>(name.front());
  if (not(std::isalpha(first) or first == '_')) {
    return false;
  }
  return std::ranges::all_of(name | std::views::drop(1), [](unsigned char c) {
    return std::isalnum(c) or c == '_';
  });
}

auto to_metric_type(std::string_view type) -> MetricType {
  auto normalized = lower_ascii(type);
  if (normalized == "counter") {
    return MetricType::counter;
  }
  if (normalized == "gauge") {
    return MetricType::gauge;
  }
  if (normalized == "histogram") {
    return MetricType::histogram;
  }
  if (normalized == "gaugehistogram" or normalized == "gauge_histogram") {
    return MetricType::gaugehistogram;
  }
  if (normalized == "summary") {
    return MetricType::summary;
  }
  if (normalized == "info") {
    return MetricType::info;
  }
  if (normalized == "stateset" or normalized == "state_set") {
    return MetricType::stateset;
  }
  return MetricType::unknown;
}

auto v1_metric_type(MetricType type)
  -> ::prometheus::MetricMetadata::MetricType {
  switch (type) {
    case MetricType::counter:
      return ::prometheus::MetricMetadata::COUNTER;
    case MetricType::gauge:
      return ::prometheus::MetricMetadata::GAUGE;
    case MetricType::histogram:
      return ::prometheus::MetricMetadata::HISTOGRAM;
    case MetricType::gaugehistogram:
      return ::prometheus::MetricMetadata::GAUGEHISTOGRAM;
    case MetricType::summary:
      return ::prometheus::MetricMetadata::SUMMARY;
    case MetricType::info:
      return ::prometheus::MetricMetadata::INFO;
    case MetricType::stateset:
      return ::prometheus::MetricMetadata::STATESET;
    case MetricType::unknown:
      return ::prometheus::MetricMetadata::UNKNOWN;
  }
  TENZIR_UNREACHABLE();
}

auto v2_metric_type(MetricType type)
  -> ::io::prometheus::write::v2::Metadata::MetricType {
  using Type = ::io::prometheus::write::v2::Metadata;
  switch (type) {
    case MetricType::counter:
      return Type::METRIC_TYPE_COUNTER;
    case MetricType::gauge:
      return Type::METRIC_TYPE_GAUGE;
    case MetricType::histogram:
      return Type::METRIC_TYPE_HISTOGRAM;
    case MetricType::gaugehistogram:
      return Type::METRIC_TYPE_GAUGEHISTOGRAM;
    case MetricType::summary:
      return Type::METRIC_TYPE_SUMMARY;
    case MetricType::info:
      return Type::METRIC_TYPE_INFO;
    case MetricType::stateset:
      return Type::METRIC_TYPE_STATESET;
    case MetricType::unknown:
      return Type::METRIC_TYPE_UNSPECIFIED;
  }
  TENZIR_UNREACHABLE();
}

auto to_string_value(data_view3 value, location loc, diagnostic_handler& dh)
  -> std::optional<std::string> {
  if (auto* str = try_as<std::string_view>(&value)) {
    return std::string{*str};
  }
  return tenzir::to_string(value, loc, dh);
}

auto to_string_value(data_view3 value, ast::expression const& expr,
                     diagnostic_handler& dh) -> std::optional<std::string> {
  return to_string_value(value, expr.get_location(), dh);
}

auto to_double(data_view3 value) -> std::optional<double> {
  return value.match(
    [](double x) -> std::optional<double> {
      return x;
    },
    [](int64_t x) -> std::optional<double> {
      return static_cast<double>(x);
    },
    [](uint64_t x) -> std::optional<double> {
      return static_cast<double>(x);
    },
    [](auto const&) -> std::optional<double> {
      return {};
    });
}

auto to_timestamp_ms(data_view3 value) -> std::optional<int64_t> {
  return value.match(
    [](time x) -> std::optional<int64_t> {
      return std::chrono::duration_cast<std::chrono::milliseconds>(
               x.time_since_epoch())
        .count();
    },
    [](int64_t x) -> std::optional<int64_t> {
      return x;
    },
    [](uint64_t x) -> std::optional<int64_t> {
      if (x > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return {};
      }
      return static_cast<int64_t>(x);
    },
    [](auto const&) -> std::optional<int64_t> {
      return {};
    });
}

auto now_ms() -> int64_t {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
           std::chrono::system_clock::now().time_since_epoch())
    .count();
}

auto add_symbol(std::vector<std::string>& symbols,
                std::unordered_map<std::string, uint32_t>& symbol_refs,
                std::string value) -> uint32_t {
  if (auto it = symbol_refs.find(value); it != symbol_refs.end()) {
    return it->second;
  }
  auto ref = detail::narrow<uint32_t>(symbols.size());
  symbols.push_back(value);
  symbol_refs.emplace(symbols.back(), ref);
  return ref;
}

auto serialize_v1(std::vector<Series> series) -> std::string {
  auto request = ::prometheus::WriteRequest{};
  auto metadata_seen = std::set<std::string>{};
  for (auto& entry : series) {
    std::ranges::sort(entry.samples, {}, &Sample::timestamp_ms);
    auto* ts = request.add_timeseries();
    for (auto const& [name, value] : entry.labels) {
      auto* label = ts->add_labels();
      label->set_name(name);
      label->set_value(value);
    }
    for (auto const& sample : entry.samples) {
      auto* out = ts->add_samples();
      out->set_value(sample.value);
      out->set_timestamp(sample.timestamp_ms);
    }
    auto family = entry.metadata.family.empty() ? entry.labels.front().second
                                                : entry.metadata.family;
    if (metadata_seen.insert(family).second
        and (entry.metadata.type != MetricType::unknown
             or not entry.metadata.help.empty()
             or not entry.metadata.unit.empty())) {
      auto* metadata = request.add_metadata();
      metadata->set_type(v1_metric_type(entry.metadata.type));
      metadata->set_metric_family_name(family);
      metadata->set_help(entry.metadata.help);
      metadata->set_unit(entry.metadata.unit);
    }
  }
  return request.SerializeAsString();
}

auto serialize_v2(std::vector<Series> series) -> std::string {
  auto request = ::io::prometheus::write::v2::Request{};
  auto symbols = std::vector<std::string>{""};
  auto symbol_refs = std::unordered_map<std::string, uint32_t>{{"", 0}};
  for (auto& entry : series) {
    std::ranges::sort(entry.samples, {}, &Sample::timestamp_ms);
    auto* ts = request.add_timeseries();
    for (auto const& [name, value] : entry.labels) {
      ts->add_labels_refs(add_symbol(symbols, symbol_refs, name));
      ts->add_labels_refs(add_symbol(symbols, symbol_refs, value));
    }
    for (auto const& sample : entry.samples) {
      auto* out = ts->add_samples();
      out->set_value(sample.value);
      out->set_timestamp(sample.timestamp_ms);
      if (sample.start_timestamp_ms != 0) {
        out->set_start_timestamp(sample.start_timestamp_ms);
      }
    }
    auto* metadata = ts->mutable_metadata();
    metadata->set_type(v2_metric_type(entry.metadata.type));
    metadata->set_help_ref(
      add_symbol(symbols, symbol_refs, entry.metadata.help));
    metadata->set_unit_ref(
      add_symbol(symbols, symbol_refs, entry.metadata.unit));
  }
  for (auto& symbol : symbols) {
    request.add_symbols(std::move(symbol));
  }
  return request.SerializeAsString();
}

auto snappy_compress(std::string body, diagnostic_handler& dh, location loc)
  -> std::optional<std::string> {
  auto codec_result = arrow::util::Codec::Create(
    arrow::Compression::type::SNAPPY,
    arrow::util::Codec::UseDefaultCompressionLevel());
  if (not codec_result.ok()) {
    diagnostic::error("failed to create Snappy codec: {}",
                      codec_result.status().ToString())
      .primary(loc)
      .emit(dh);
    return {};
  }
  auto codec = codec_result.MoveValueUnsafe();
  auto input_size = detail::narrow<int64_t>(body.size());
  auto* input = reinterpret_cast<uint8_t const*>(body.data());
  auto output_size = codec->MaxCompressedLen(input_size, input);
  auto output = std::string{};
  output.resize(output_size);
  auto compressed_size
    = codec->Compress(input_size, input, detail::narrow<int64_t>(output.size()),
                      reinterpret_cast<uint8_t*>(output.data()));
  if (not compressed_size.ok()) {
    diagnostic::error("Snappy compression failed: {}",
                      compressed_size.status().ToString())
      .primary(loc)
      .emit(dh);
    return {};
  }
  output.resize(*compressed_size);
  return output;
}

auto resolve_secrets(OpCtx& ctx, ToPrometheusArgs& args,
                     std::string& resolved_url, Headers& resolved_headers)
  -> Task<failure_or<void>> {
  auto requests = std::vector<secret_request>{};
  requests.emplace_back(
    make_secret_request("url", args.url, resolved_url, ctx.dh()));
  auto header_requests = http::make_header_secret_requests(
    args.headers, resolved_headers, ctx.dh());
  requests.insert(requests.end(),
                  std::make_move_iterator(header_requests.begin()),
                  std::make_move_iterator(header_requests.end()));
  CO_TRY(co_await ctx.resolve_secrets(std::move(requests)));
  if (resolved_url.empty()) {
    diagnostic::error("`url` must not be empty").primary(args.url).emit(ctx);
    co_return failure::promise();
  }
  co_return {};
}

class ToPrometheus final : public Operator<table_slice, void> {
public:
  explicit ToPrometheus(ToPrometheusArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    bytes_write_counter_
      = ctx.make_counter(MetricsLabel{"operator", "to_prometheus"},
                         MetricsDirection::write, MetricsVisibility::external_);
    if (args_.protobuf_message.inner == v2_protobuf_message) {
      protocol_ = Protocol::v2;
    }
    if (auto result = co_await resolve_secrets(ctx, args_, url_, headers_);
        result.is_error()) {
      done_ = true;
      co_return;
    }
    auto config = http::make_http_pool_config(
      args_.tls, url_, args_.url.source, ctx, get_timeout(),
      std::addressof(ctx.actor_system().config()));
    if (config.is_success()) {
      config->connection_timeout = get_connection_timeout();
      config->max_retry_count = get_max_retry_count();
      config->retry_delay = get_retry_delay();
      auto loc = args_.operator_location;
      config->on_retry = [dh = &ctx.dh(), loc](std::string_view message) {
        diagnostic::warning("{}", message).primary(loc).emit(*dh);
      };
    }
    if (config.is_error()) {
      done_ = true;
      co_return;
    }
    try {
      pool_ = HttpPool::make(ctx.io_executor(), url_, std::move(*config));
    } catch (std::exception const& e) {
      diagnostic::error("failed to initialize HTTP client: {}", e.what())
        .primary(args_.url)
        .emit(ctx);
      done_ = true;
    }
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (done_ or input.rows() == 0) {
      co_return;
    }
    auto name = eval(args_.name, input, ctx.dh());
    auto value = eval(args_.value, input, ctx.dh());
    auto timestamp = eval(args_.timestamp, input, ctx.dh());
    auto labels = eval(args_.labels, input, ctx.dh());
    auto type = eval(args_.type, input, ctx.dh());
    auto help = eval(args_.help, input, ctx.dh());
    auto unit = eval(args_.unit, input, ctx.dh());
    auto family = eval(args_.family, input, ctx.dh());
    auto start_timestamp = eval(args_.start_timestamp, input, ctx.dh());
    for (auto row = int64_t{0}; row < detail::narrow<int64_t>(input.rows());
         ++row) {
      auto sample = make_sample(row, name, value, timestamp, labels, type, help,
                                unit, family, start_timestamp, ctx);
      if (not sample) {
        continue;
      }
      add_sample(std::move(*sample));
      if (pending_sample_count_ >= args_.max_samples_per_request.inner) {
        co_await send_request(ctx);
      } else if (next_flush_.is_none()) {
        next_flush_
          = std::chrono::steady_clock::now() + args_.flush_interval.inner;
        flush_ready_->notify_one();
      }
    }
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (not next_flush_) {
      co_await flush_ready_->wait();
    }
    if (next_flush_) {
      co_await sleep_until(*next_flush_);
    }
    co_return {};
  }

  auto process_task(Any result, OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(result);
    if (next_flush_ and std::chrono::steady_clock::now() >= *next_flush_) {
      co_await send_request(ctx);
      next_flush_ = None{};
    }
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    co_await send_request(ctx);
    done_ = true;
    co_return FinalizeBehavior::done;
  }

  auto prepare_snapshot(OpCtx& ctx) -> Task<void> override {
    co_await send_request(ctx);
    next_flush_ = None{};
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

private:
  struct PendingSample {
    std::vector<std::pair<std::string, std::string>> labels;
    Sample sample;
    Metadata metadata;
  };

  auto make_sample(int64_t row, multi_series const& names,
                   multi_series const& values, multi_series const& timestamps,
                   multi_series const& labels, multi_series const& types,
                   multi_series const& helps, multi_series const& units,
                   multi_series const& families,
                   multi_series const& start_timestamps, OpCtx& ctx)
    -> std::optional<PendingSample> {
    if (names.is_null(row)) {
      diagnostic::warning("metric name is `null`, skipping event")
        .primary(args_.name)
        .emit(ctx);
      return {};
    }
    auto name = to_string_value(names.view3_at(row), args_.name, ctx.dh());
    if (not name or name->empty()) {
      diagnostic::warning("metric name must be a non-empty string")
        .primary(args_.name)
        .emit(ctx);
      return {};
    }
    if (args_.sanitize_names) {
      *name = sanitize_metric_name(std::move(*name));
    }
    if (not valid_metric_name(*name)) {
      diagnostic::warning("invalid Prometheus metric name `{}`, skipping event",
                          *name)
        .primary(args_.name)
        .emit(ctx);
      return {};
    }
    if (values.is_null(row)) {
      diagnostic::warning("metric value is `null`, skipping event")
        .primary(args_.value)
        .emit(ctx);
      return {};
    }
    auto value = to_double(values.view3_at(row));
    if (not value or not std::isfinite(*value)) {
      diagnostic::warning(
        "metric value must be a finite number, skipping event")
        .primary(args_.value)
        .emit(ctx);
      return {};
    }
    auto result = PendingSample{};
    result.labels.emplace_back("__name__", *name);
    if (not labels.is_null(row)) {
      auto label_value = labels.view3_at(row);
      auto* record = try_as<view3<tenzir::record>>(&label_value);
      if (not record) {
        diagnostic::warning(
          "`labels` must evaluate to a record, skipping event")
          .primary(args_.labels)
          .emit(ctx);
        return {};
      }
      auto seen = std::set<std::string>{"__name__"};
      for (auto [label_name, label_value] : *record) {
        auto final_name = std::string{label_name};
        if (final_name == "__name__") {
          diagnostic::warning("`labels.__name__` is reserved, skipping event")
            .primary(args_.labels)
            .emit(ctx);
          return {};
        }
        if (args_.sanitize_names) {
          final_name = sanitize_label_name(std::move(final_name));
        }
        if (not valid_label_name(final_name)) {
          diagnostic::warning(
            "invalid Prometheus label name `{}`, skipping event", final_name)
            .primary(args_.labels)
            .emit(ctx);
          return {};
        }
        if (not seen.insert(final_name).second) {
          diagnostic::warning("duplicate label `{}`, skipping event",
                              final_name)
            .primary(args_.labels)
            .emit(ctx);
          return {};
        }
        auto value_string
          = to_string_value(label_value, args_.labels, ctx.dh()).value_or("");
        result.labels.emplace_back(std::move(final_name),
                                   std::move(value_string));
      }
    }
    std::ranges::sort(result.labels, {}, [](auto const& label) -> auto const& {
      return label.first;
    });
    result.sample.value = *value;
    result.sample.timestamp_ms
      = timestamps.is_null(row)
          ? now_ms()
          : to_timestamp_ms(timestamps.view3_at(row)).value_or(now_ms());
    if (not start_timestamps.is_null(row)) {
      result.sample.start_timestamp_ms
        = to_timestamp_ms(start_timestamps.view3_at(row)).value_or(0);
    }
    result.metadata.family
      = families.is_null(row)
          ? *name
          : to_string_value(families.view3_at(row), args_.family, ctx.dh())
              .value_or(*name);
    if (not types.is_null(row)) {
      auto type_text
        = to_string_value(types.view3_at(row), args_.type, ctx.dh());
      result.metadata.type
        = type_text ? to_metric_type(*type_text) : MetricType::unknown;
    }
    if (not helps.is_null(row)) {
      result.metadata.help
        = to_string_value(helps.view3_at(row), args_.help, ctx.dh())
            .value_or("");
    }
    if (not units.is_null(row)) {
      result.metadata.unit
        = to_string_value(units.view3_at(row), args_.unit, ctx.dh())
            .value_or("");
    }
    return result;
  }

  auto add_sample(PendingSample sample) -> void {
    auto key = labels_key(sample.labels);
    auto& series = pending_[key];
    if (series.labels.empty()) {
      series.labels = std::move(sample.labels);
      series.metadata = std::move(sample.metadata);
    }
    series.samples.push_back(sample.sample);
    ++pending_sample_count_;
  }

  auto
  labels_key(std::vector<std::pair<std::string, std::string>> const& labels)
    -> std::string {
    auto result = std::string{};
    for (auto const& [name, value] : labels) {
      result += name;
      result += '\0';
      result += value;
      result += '\0';
    }
    return result;
  }

  auto take_pending() -> std::vector<Series> {
    auto result = std::vector<Series>{};
    result.reserve(pending_.size());
    for (auto& [_, series] : pending_) {
      result.push_back(std::move(series));
    }
    pending_.clear();
    pending_sample_count_ = 0;
    return result;
  }

  auto serialize(std::vector<Series> series) const -> std::string {
    return protocol_ == Protocol::v1 ? serialize_v1(std::move(series))
                                     : serialize_v2(std::move(series));
  }

  auto can_split(std::vector<Series> const& series) -> bool {
    return series.size() > 1
           or (series.size() == 1 and series.front().samples.size() > 1);
  }

  auto split_series(std::vector<Series>& series) -> std::vector<Series> {
    TENZIR_ASSERT(can_split(series));
    if (series.size() > 1) {
      auto result = std::vector<Series>{};
      auto half = series.size() / 2;
      result.reserve(series.size() - half);
      std::move(series.begin() + detail::narrow<int64_t>(half), series.end(),
                std::back_inserter(result));
      series.erase(series.begin() + detail::narrow<int64_t>(half),
                   series.end());
      return result;
    }
    auto& entry = series.front();
    auto result = std::vector<Series>{};
    auto half = entry.samples.size() / 2;
    auto split = Series{};
    split.labels = entry.labels;
    split.metadata = entry.metadata;
    split.samples.reserve(entry.samples.size() - half);
    std::move(entry.samples.begin() + detail::narrow<int64_t>(half),
              entry.samples.end(), std::back_inserter(split.samples));
    entry.samples.erase(entry.samples.begin() + detail::narrow<int64_t>(half),
                        entry.samples.end());
    result.push_back(std::move(split));
    return result;
  }

  auto send_request(OpCtx& ctx) -> Task<void> {
    if (pending_.empty() or not pool_) {
      co_return;
    }
    co_await send_series(take_pending(), ctx);
  }

  auto send_series(std::vector<Series> series, OpCtx& ctx) -> Task<void> {
    if (series.empty()) {
      co_return;
    }
    auto uncompressed = serialize(series);
    if (uncompressed.size() > args_.max_uncompressed_bytes.inner) {
      if (not can_split(series)) {
        diagnostic::error(
          "Prometheus remote write request with a single sample exceeds "
          "`max_uncompressed_bytes`")
          .primary(args_.max_uncompressed_bytes)
          .emit(ctx);
        co_return;
      }
      auto second_half = split_series(series);
      co_await send_series(std::move(series), ctx);
      co_await send_series(std::move(second_half), ctx);
      co_return;
    }
    auto compressed = snappy_compress(std::move(uncompressed), ctx.dh(),
                                      args_.operator_location);
    if (not compressed) {
      co_return;
    }
    auto headers = std::map<std::string, std::string>{};
    for (auto const& [name, value] : headers_) {
      headers[name] = value;
    }
    headers["Content-Type"]
      = protocol_ == Protocol::v1 ? v1_content_type : v2_content_type;
    headers["Content-Encoding"] = "snappy";
    headers["Content-Length"] = fmt::to_string(compressed->size());
    headers["User-Agent"] = fmt::format("Tenzir/{}", version::version);
    headers["X-Prometheus-Remote-Write-Version"]
      = protocol_ == Protocol::v1 ? "0.1.0" : "2.0.0";
    auto compressed_size = compressed->size();
    auto result
      = co_await (*pool_)->post(std::move(*compressed), std::move(headers));
    if (result.is_err()) {
      diagnostic::error("HTTP request failed: {}",
                        std::move(result).unwrap_err())
        .primary(args_.operator_location)
        .emit(ctx);
      co_return;
    }
    auto response = std::move(result).unwrap();
    if (not response.is_status_success()) {
      diagnostic::error("HTTP request returned status {}", response.status_code)
        .note("response body: {}", response.body)
        .primary(args_.operator_location)
        .emit(ctx);
      co_return;
    }
    bytes_write_counter_.add(compressed_size);
  }

  auto get_timeout() const -> std::chrono::milliseconds {
    if (args_.timeout) {
      return std::chrono::duration_cast<std::chrono::milliseconds>(
        args_.timeout->inner);
    }
    return std::chrono::seconds{30};
  }

  auto get_connection_timeout() const -> std::chrono::milliseconds {
    if (args_.connection_timeout) {
      return std::chrono::duration_cast<std::chrono::milliseconds>(
        args_.connection_timeout->inner);
    }
    return http::default_connection_timeout;
  }

  auto get_max_retry_count() const -> uint32_t {
    if (args_.max_retry_count) {
      return detail::narrow<uint32_t>(args_.max_retry_count->inner);
    }
    return http::default_max_retry_count;
  }

  auto get_retry_delay() const -> std::chrono::milliseconds {
    if (args_.retry_delay) {
      return std::chrono::duration_cast<std::chrono::milliseconds>(
        args_.retry_delay->inner);
    }
    return http::default_retry_delay;
  }

  ToPrometheusArgs args_;
  Protocol protocol_ = Protocol::v1;
  std::string url_;
  Headers headers_;
  Option<Box<HttpPool>> pool_;
  std::map<std::string, Series> pending_;
  uint64_t pending_sample_count_ = 0;
  bool done_ = false;
  MetricsCounter bytes_write_counter_;
  mutable Option<std::chrono::steady_clock::time_point> next_flush_;
  mutable Box<Notify> flush_ready_{std::in_place};
};

class ToPrometheusPlugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "to_prometheus";
  }

  auto describe() const -> Description override {
    auto d = Describer<ToPrometheusArgs, ToPrometheus>{};
    d.positional("url", &ToPrometheusArgs::url);
    auto protobuf_message = d.named_optional(
      "protobuf_message", &ToPrometheusArgs::protobuf_message);
    d.named_optional("name", &ToPrometheusArgs::name, "string");
    d.named_optional("value", &ToPrometheusArgs::value, "number");
    d.named_optional("timestamp", &ToPrometheusArgs::timestamp, "time");
    d.named_optional("labels", &ToPrometheusArgs::labels, "record");
    d.named_optional("type", &ToPrometheusArgs::type, "string");
    d.named_optional("help", &ToPrometheusArgs::help, "string");
    d.named_optional("unit", &ToPrometheusArgs::unit, "string");
    d.named_optional("family", &ToPrometheusArgs::family, "string");
    d.named_optional("start_timestamp", &ToPrometheusArgs::start_timestamp,
                     "time");
    auto headers = d.named("headers", &ToPrometheusArgs::headers, "record");
    auto tls_validator = tls_options{
      {.is_server = false}}.add_to_describer(d, &ToPrometheusArgs::tls);
    auto timeout = d.named("timeout", &ToPrometheusArgs::timeout);
    auto connection_timeout
      = d.named("connection_timeout", &ToPrometheusArgs::connection_timeout);
    auto max_retry_count
      = d.named("max_retry_count", &ToPrometheusArgs::max_retry_count);
    auto retry_delay = d.named("retry_delay", &ToPrometheusArgs::retry_delay);
    auto flush_interval
      = d.named_optional("flush_interval", &ToPrometheusArgs::flush_interval);
    auto max_samples_per_request = d.named_optional(
      "max_samples_per_request", &ToPrometheusArgs::max_samples_per_request);
    auto max_uncompressed_bytes = d.named_optional(
      "max_uncompressed_bytes", &ToPrometheusArgs::max_uncompressed_bytes);
    d.named("sanitize_names", &ToPrometheusArgs::sanitize_names);
    d.operator_location(&ToPrometheusArgs::operator_location);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      tls_validator(ctx);
      auto message = ctx.get(protobuf_message)
                       .value_or(located<std::string>{default_protobuf_message,
                                                      location::unknown});
      if (message.inner != default_protobuf_message
          and message.inner != v2_protobuf_message) {
        diagnostic::error("unsupported protobuf message `{}`", message.inner)
          .primary(message.source)
          .note("supported values are `{}` and `{}`", default_protobuf_message,
                v2_protobuf_message)
          .emit(ctx);
      }
      if (auto value = ctx.get(timeout);
          value and value->inner < duration::zero()) {
        diagnostic::error("`timeout` must be a non-negative duration")
          .primary(value->source)
          .emit(ctx);
      }
      if (auto value = ctx.get(connection_timeout);
          value and value->inner < duration::zero()) {
        diagnostic::error(
          "`connection_timeout` must be a non-negative duration")
          .primary(value->source)
          .emit(ctx);
      }
      if (auto value = ctx.get(max_retry_count);
          value and value->inner > std::numeric_limits<uint32_t>::max()) {
        diagnostic::error("`max_retry_count` must be <= {}",
                          std::numeric_limits<uint32_t>::max())
          .primary(value->source)
          .emit(ctx);
      }
      if (auto value = ctx.get(retry_delay);
          value and value->inner < duration::zero()) {
        diagnostic::error("`retry_delay` must be a non-negative duration")
          .primary(value->source)
          .emit(ctx);
      }
      if (auto value = ctx.get(flush_interval);
          value and value->inner <= duration::zero()) {
        diagnostic::error("`flush_interval` must be positive")
          .primary(value->source)
          .emit(ctx);
      }
      if (auto value = ctx.get(max_samples_per_request);
          value and value->inner == 0) {
        diagnostic::error("`max_samples_per_request` must be positive")
          .primary(value->source)
          .emit(ctx);
      }
      if (auto value = ctx.get(max_uncompressed_bytes);
          value and value->inner == 0) {
        diagnostic::error("`max_uncompressed_bytes` must be positive")
          .primary(value->source)
          .emit(ctx);
      }
      if (auto value = ctx.get(headers)) {
        auto const* rec = try_as<record>(&value->inner);
        if (not rec) {
          diagnostic::error("`headers` must be a record")
            .primary(value->source)
            .emit(ctx);
        } else {
          for (auto const& [name, header_value] : *rec) {
            if (is_reserved_header(name)) {
              diagnostic::error("reserved remote-write header `{}`", name)
                .primary(value->source)
                .note("this header is set by `to_prometheus`")
                .emit(ctx);
            }
            if (not is<std::string>(header_value)
                and not is<secret>(header_value)) {
              diagnostic::error("header values must be `string` or `secret`")
                .primary(value->source)
                .emit(ctx);
            }
          }
        }
      }
      return {};
    });
    return d.invariant_order();
  }
};

} // namespace

} // namespace tenzir::plugins::prometheus

TENZIR_REGISTER_PLUGIN(tenzir::plugins::prometheus::ToPrometheusPlugin)
