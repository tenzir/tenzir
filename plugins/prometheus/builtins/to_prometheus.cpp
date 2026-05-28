//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "io/prometheus/write/v2/types.pb.h"
#include "prometheus/remote.pb.h"

#include <tenzir/arc.hpp>
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
#include <bit>
#include <charconv>
#include <chrono>
#include <cmath>
#include <exception>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
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
constexpr auto v2_samples_written_header
  = "X-Prometheus-Remote-Write-Samples-Written";
constexpr auto max_uncompressed_bytes_default = uint64_t{4 * 1024 * 1024};
constexpr auto prometheus_stale_marker_bits = uint64_t{0x7ff0000000000002};

using Headers = std::vector<http::Header>;

template <class Config>
auto make_pool_config(Option<located<data>> const& tls, std::string& url,
                      location url_loc, diagnostic_handler& dh,
                      std::chrono::milliseconds request_timeout,
                      Config const& cfg) -> failure_or<HttpPoolConfig>
  requires requires {
    http::make_http_pool_config(tls, url, url_loc, dh, request_timeout, cfg);
  }
{
  return http::make_http_pool_config(tls, url, url_loc, dh, request_timeout,
                                     cfg);
}

template <class Config>
auto make_pool_config(Option<located<data>> const& tls, std::string& url,
                      location url_loc, diagnostic_handler& dh,
                      std::chrono::milliseconds request_timeout,
                      Config const& cfg) -> failure_or<HttpPoolConfig>
  requires(not requires {
    http::make_http_pool_config(tls, url, url_loc, dh, request_timeout, cfg);
  })
{
  return http::make_http_pool_config(tls, url, url_loc, dh, request_timeout,
                                     std::addressof(cfg));
}

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

/// Returns whether `name` is one of the Remote Write headers that the
/// specification reserves for senders. Users may add authentication headers,
/// but these protocol headers must stay under operator control so payload
/// schema, compression, and write-version negotiation remain consistent with
/// the body.
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

/// Rewrites an arbitrary metric name to the legacy Prometheus identifier shape.
/// Remote Write v1 requires `[a-zA-Z_:][a-zA-Z0-9_:]*`; with
/// `sanitize_names=true`, this helper keeps valid ASCII bytes and replaces any
/// byte that would violate the regex with `_`. Empty names remain empty so the
/// caller can reject them with a precise diagnostic.
auto sanitize_metric_name(std::string name) -> std::string {
  if (name.empty()) {
    return name;
  }
  auto const first_ok = [](unsigned char c) {
    return detail::ascii_isalpha(c) or c == '_' or c == ':';
  };
  auto const rest_ok = [](unsigned char c) {
    return detail::ascii_isalnum(c) or c == '_' or c == ':';
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

/// Rewrites an arbitrary label name to the legacy Prometheus identifier shape.
/// Label names use the metric-name rules without `:`. This intentionally works
/// byte-by-byte instead of normalizing UTF-8: Remote Write v2 can keep UTF-8
/// names by disabling sanitization, while v1 needs ASCII-safe labels.
auto sanitize_label_name(std::string name) -> std::string {
  if (name.empty()) {
    return name;
  }
  auto const first_ok = [](unsigned char c) {
    return detail::ascii_isalpha(c) or c == '_';
  };
  auto const rest_ok = [](unsigned char c) {
    return detail::ascii_isalnum(c) or c == '_';
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

/// Checks the Remote Write v1 metric-name regex without rewriting the input.
auto valid_legacy_metric_name(std::string_view name) -> bool {
  if (name.empty()) {
    return false;
  }
  auto const first = static_cast<unsigned char>(name.front());
  if (not(detail::ascii_isalpha(first) or first == '_' or first == ':')) {
    return false;
  }
  return std::ranges::all_of(name | std::views::drop(1), [](unsigned char c) {
    return detail::ascii_isalnum(c) or c == '_' or c == ':';
  });
}

/// Checks the Remote Write v1 label-name regex without rewriting the input.
auto valid_legacy_label_name(std::string_view name) -> bool {
  if (name.empty()) {
    return false;
  }
  auto const first = static_cast<unsigned char>(name.front());
  if (not(detail::ascii_isalpha(first) or first == '_')) {
    return false;
  }
  return std::ranges::all_of(name | std::views::drop(1), [](unsigned char c) {
    return detail::ascii_isalnum(c) or c == '_';
  });
}

/// Parses the OpenMetrics/Prometheus metadata type names accepted by the
/// operator. `unknown` is distinct from parse failure: v1 can serialize
/// UNKNOWN, while v2 skips invalid type text to avoid unspecified metadata.
auto parse_metric_type(std::string_view type) -> Option<MetricType> {
  auto normalized = detail::ascii_tolower(type);
  if (normalized == "unknown") {
    return MetricType::unknown;
  }
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
  return {};
}

/// Returns whether a row carries any metadata worth serializing. `family` alone
/// is not metadata on the wire; it only chooses the v1 metadata family name.
auto has_metadata(Metadata const& metadata) -> bool {
  return metadata.type != MetricType::unknown or not metadata.help.empty()
         or not metadata.unit.empty();
}

/// Fills missing metadata fields without overwriting earlier non-empty values.
/// Rows for the same series or metric family can arrive with partial metadata;
/// merging makes the emitted metadata independent from row order when later
/// rows supply fields that earlier rows omitted.
auto merge_metadata(Metadata& target, Metadata source) -> void {
  if (target.family.empty()) {
    target.family = std::move(source.family);
  }
  if (target.type == MetricType::unknown) {
    target.type = source.type;
  }
  if (target.help.empty()) {
    target.help = std::move(source.help);
  }
  if (target.unit.empty()) {
    target.unit = std::move(source.unit);
  }
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
  -> Option<std::string> {
  if (auto* str = try_as<std::string_view>(&value)) {
    return std::string{*str};
  }
  return tenzir::to_string(value, loc, dh);
}

auto to_string_value(data_view3 value, ast::expression const& expr,
                     diagnostic_handler& dh) -> Option<std::string> {
  return to_string_value(value, expr.get_location(), dh);
}

/// Converts numeric Tenzir values to Prometheus' float64 sample value. Other
/// types are rejected by returning `None` so callers can skip the row with a
/// warning instead of inventing a value.
auto to_double(data_view3 value) -> Option<double> {
  return value.match(
    [](double x) -> Option<double> {
      return x;
    },
    [](int64_t x) -> Option<double> {
      return static_cast<double>(x);
    },
    [](uint64_t x) -> Option<double> {
      return static_cast<double>(x);
    },
    [](auto const&) -> Option<double> {
      return {};
    });
}

/// Converts accepted timestamp inputs to Prometheus milliseconds since epoch.
/// Unsigned integers above `INT64_MAX` are rejected because the protobuf field
/// is signed; using a checked cast here would panic on user data.
auto to_timestamp_ms(data_view3 value) -> Option<int64_t> {
  return value.match(
    [](time x) -> Option<int64_t> {
      return std::chrono::duration_cast<std::chrono::milliseconds>(
               x.time_since_epoch())
        .count();
    },
    [](int64_t x) -> Option<int64_t> {
      return x;
    },
    [](uint64_t x) -> Option<int64_t> {
      if (x > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return {};
      }
      return static_cast<int64_t>(x);
    },
    [](auto const&) -> Option<int64_t> {
      return {};
    });
}

/// Returns true for Prometheus' stale-marker NaN. Remote Write reserves this
/// exact IEEE-754 bit pattern to mark a time series as no longer appended to;
/// other NaNs remain invalid sample values.
auto is_prometheus_stale_marker(double value) -> bool {
  return std::bit_cast<uint64_t>(value) == prometheus_stale_marker_bits;
}

auto now_ms() -> int64_t {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
           std::chrono::system_clock::now().time_since_epoch())
    .count();
}

auto parse_u64(std::string_view value) -> Option<uint64_t> {
  auto result = uint64_t{};
  auto const* begin = value.data();
  auto const* end = value.data() + value.size();
  auto [ptr, err] = std::from_chars(begin, end, result);
  if (err != std::errc{} or ptr != end) {
    return {};
  }
  return result;
}

/// Validates the mandatory Remote Write v2 acknowledgement header. A 2xx
/// without the expected written-sample count may mean a v1 receiver decoded the
/// v2 body as an empty v1 request, so accepting it would silently drop data.
auto check_v2_written_samples(http::Response const& response,
                              uint64_t expected_samples, OpCtx& ctx,
                              location loc) -> bool {
  auto header = http::find(response.headers, v2_samples_written_header);
  auto written_samples = header ? parse_u64(*header) : Option<uint64_t>{0};
  if (not written_samples) {
    diagnostic::error("HTTP response returned invalid `{}` header",
                      v2_samples_written_header)
      .note("header value: {}", *header)
      .primary(loc)
      .emit(ctx);
    return false;
  }
  if (*written_samples != expected_samples) {
    diagnostic::error("HTTP response wrote {} of {} Prometheus samples",
                      *written_samples, expected_samples)
      .note("Remote Write v2 receivers report successful writes via the `{}` "
            "header",
            v2_samples_written_header)
      .primary(loc)
      .emit(ctx);
    return false;
  }
  return true;
}

/// Adds `value` to the Remote Write v2 symbol table and returns its reference.
/// The v2 wire format interns label and metadata strings by index; the first
/// symbol must stay the empty string for optional unset references.
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

/// Serializes series into the legacy `prometheus.WriteRequest` schema. Metadata
/// is request-wide in v1, so this first merges partial metadata per metric
/// family and emits at most one metadata record for each family.
auto serialize_v1(std::vector<Series> series) -> std::string {
  auto request = ::prometheus::WriteRequest{};
  auto metadata_by_family = std::map<std::string, Metadata>{};
  for (auto& entry : series) {
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
    auto metric_name
      = std::ranges::find(entry.labels, "__name__", [](auto const& label) {
          return label.first;
        });
    TENZIR_ASSERT(metric_name != entry.labels.end());
    auto family = entry.metadata.family.empty() ? metric_name->second
                                                : entry.metadata.family;
    if (has_metadata(entry.metadata)) {
      auto metadata = std::move(entry.metadata);
      metadata.family = family;
      merge_metadata(metadata_by_family[family], std::move(metadata));
    }
  }
  for (auto& [family, metadata] : metadata_by_family) {
    auto* out = request.add_metadata();
    out->set_type(v1_metric_type(metadata.type));
    out->set_metric_family_name(family);
    out->set_help(metadata.help);
    out->set_unit(metadata.unit);
  }
  return request.SerializeAsString();
}

/// Serializes series into the Remote Write v2 schema. Labels and metadata use
/// symbol references, and metadata is only emitted when a concrete metric type
/// exists because strict receivers may reject `METRIC_TYPE_UNSPECIFIED`.
auto serialize_v2(std::vector<Series> series) -> std::string {
  auto request = ::io::prometheus::write::v2::Request{};
  auto symbols = std::vector<std::string>{""};
  auto symbol_refs = std::unordered_map<std::string, uint32_t>{{"", 0}};
  for (auto& entry : series) {
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
    if (entry.metadata.type != MetricType::unknown) {
      auto* metadata = ts->mutable_metadata();
      metadata->set_type(v2_metric_type(entry.metadata.type));
      if (not entry.metadata.help.empty()) {
        metadata->set_help_ref(
          add_symbol(symbols, symbol_refs, entry.metadata.help));
      }
      if (not entry.metadata.unit.empty()) {
        metadata->set_unit_ref(
          add_symbol(symbols, symbol_refs, entry.metadata.unit));
      }
    }
  }
  for (auto& symbol : symbols) {
    request.add_symbols(std::move(symbol));
  }
  return request.SerializeAsString();
}

/// Sorts samples in-place to satisfy Remote Write's per-series timestamp order.
/// This must happen before request-size splitting so recursive sends preserve
/// the global order for one series across multiple HTTP requests.
auto sort_samples(std::vector<Series>& series) -> void {
  for (auto& entry : series) {
    std::ranges::sort(entry.samples, {}, &Sample::timestamp_ms);
  }
}

/// Compresses the protobuf body with Snappy block compression, the only
/// compression format Remote Write v1/v2 receivers are required to accept.
auto snappy_compress(std::string body, diagnostic_handler& dh, location loc)
  -> Option<std::string> {
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

/// Resolves the endpoint URL and user headers. Header secrets are resolved into
/// the same vector that later gets copied for each request before protocol
/// headers are overwritten.
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
                         MetricsDirection::write, MetricsVisibility::external_,
                         MetricsUnit::bytes);
    events_write_counter_
      = ctx.make_counter(MetricsLabel{"operator", "to_prometheus"},
                         MetricsDirection::write, MetricsVisibility::external_,
                         MetricsUnit::events);
    if (args_.protobuf_message.inner == v2_protobuf_message) {
      protocol_ = Protocol::v2;
    }
    if (auto result = co_await resolve_secrets(ctx, args_, url_, headers_);
        result.is_error()) {
      done_ = true;
      co_return;
    }
    auto config = make_pool_config(args_.tls, url_, args_.url.source, ctx.dh(),
                                   get_timeout(), ctx.actor_system().config());
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
        if (not co_await send_request(ctx)) {
          done_ = true;
          co_return;
        }
        next_flush_ = None{};
      } else if (next_flush_.is_none()) {
        arm_flush_timer();
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
      if (not co_await send_request(ctx)) {
        done_ = true;
        co_return;
      }
      next_flush_ = None{};
    }
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    if (not co_await send_request(ctx)) {
      done_ = true;
      co_return FinalizeBehavior::done;
    }
    done_ = true;
    co_return FinalizeBehavior::done;
  }

  auto prepare_snapshot(OpCtx& ctx) -> Task<void> override {
    if (not co_await send_request(ctx)) {
      done_ = true;
    }
    next_flush_ = None{};
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

private:
  using Labels = std::vector<std::pair<std::string, std::string>>;

  struct PendingSample {
    Labels labels;
    Sample sample;
    Metadata metadata;
  };

  struct PendingSeries {
    std::vector<Sample> samples;
    Metadata metadata;
  };

  /// Converts one input row into a pending Prometheus sample. This is where the
  /// operator applies v1/v2 naming rules, row-level validation, timestamp
  /// fallback, and metadata extraction before the sample joins a pending series.
  auto make_sample(int64_t row, multi_series const& names,
                   multi_series const& values, multi_series const& timestamps,
                   multi_series const& labels, multi_series const& types,
                   multi_series const& helps, multi_series const& units,
                   multi_series const& families,
                   multi_series const& start_timestamps, OpCtx& ctx)
    -> Option<PendingSample> {
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
    if (protocol_ == Protocol::v1 and not valid_legacy_metric_name(*name)) {
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
    if (not value
        or (std::isnan(*value) and not is_prometheus_stale_marker(*value))) {
      diagnostic::warning(
        "metric value must be a number or Prometheus stale marker, skipping "
        "event")
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
        if (final_name.empty()) {
          diagnostic::warning("Prometheus label name must not be empty, "
                              "skipping event")
            .primary(args_.labels)
            .emit(ctx);
          return {};
        }
        if (protocol_ == Protocol::v1
            and not valid_legacy_label_name(final_name)) {
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
          = to_string_value(label_value, args_.labels, ctx.dh()).unwrap_or("");
        if (value_string.empty()) {
          diagnostic::warning("Prometheus label `{}` has an empty value, "
                              "skipping event",
                              final_name)
            .primary(args_.labels)
            .emit(ctx);
          return {};
        }
        result.labels.emplace_back(std::move(final_name),
                                   std::move(value_string));
      }
    }
    std::ranges::sort(result.labels, {}, [](auto const& label) -> auto const& {
      return label.first;
    });
    result.sample.value = *value;
    if (timestamps.is_null(row)) {
      result.sample.timestamp_ms = now_ms();
    } else if (auto timestamp = to_timestamp_ms(timestamps.view3_at(row))) {
      result.sample.timestamp_ms = *timestamp;
    } else {
      diagnostic::warning("invalid metric timestamp, skipping event")
        .primary(args_.timestamp)
        .emit(ctx);
      return {};
    }
    if (protocol_ == Protocol::v2 and not start_timestamps.is_null(row)) {
      if (auto start_timestamp
          = to_timestamp_ms(start_timestamps.view3_at(row))) {
        result.sample.start_timestamp_ms = *start_timestamp;
      } else {
        diagnostic::warning("invalid metric start timestamp, skipping event")
          .primary(args_.start_timestamp)
          .emit(ctx);
        return {};
      }
    }
    result.metadata.family
      = families.is_null(row)
          ? *name
          : to_string_value(families.view3_at(row), args_.family, ctx.dh())
              .unwrap_or(*name);
    if (not types.is_null(row)) {
      auto type_text
        = to_string_value(types.view3_at(row), args_.type, ctx.dh());
      if (type_text) {
        auto type = parse_metric_type(*type_text);
        if (protocol_ == Protocol::v2 and not type) {
          diagnostic::warning("invalid Prometheus metric type `{}`, skipping "
                              "event",
                              *type_text)
            .primary(args_.type)
            .emit(ctx);
          return {};
        }
        result.metadata.type = type.unwrap_or(MetricType::unknown);
      }
    }
    if (not helps.is_null(row)) {
      result.metadata.help
        = to_string_value(helps.view3_at(row), args_.help, ctx.dh())
            .unwrap_or("");
    }
    if (not units.is_null(row)) {
      result.metadata.unit
        = to_string_value(units.view3_at(row), args_.unit, ctx.dh())
            .unwrap_or("");
    }
    return result;
  }

  /// Adds a row to the current request buffer, grouping by the complete sorted
  /// label set so all samples for one time series share one protobuf series.
  auto add_sample(PendingSample sample) -> void {
    auto& series = pending_[sample.labels];
    if (series.samples.empty()) {
      series.metadata = std::move(sample.metadata);
    } else {
      merge_metadata(series.metadata, std::move(sample.metadata));
    }
    series.samples.push_back(sample.sample);
    ++pending_sample_count_;
  }

  /// Moves the pending map into a linear vector that can be sorted, split, and
  /// serialized without holding on to mutable operator buffering state.
  auto take_pending() -> std::vector<Series> {
    auto result = std::vector<Series>{};
    result.reserve(pending_.size());
    for (auto& [labels, series] : pending_) {
      result.push_back({
        .labels = labels,
        .samples = std::move(series.samples),
        .metadata = std::move(series.metadata),
      });
    }
    pending_.clear();
    pending_sample_count_ = 0;
    return result;
  }

  auto serialize(std::vector<Series> series) const -> std::string {
    return protocol_ == Protocol::v1 ? serialize_v1(std::move(series))
                                     : serialize_v2(std::move(series));
  }

  /// Splits an oversized serialized request roughly in half. For multi-series
  /// requests this splits by series; for a single huge series it splits the
  /// already timestamp-sorted sample vector to keep recursive sends ordered.
  auto split_series(std::vector<Series>& series) -> std::vector<Series> {
    TENZIR_ASSERT(
      series.size() > 1
      or (series.size() == 1 and series.front().samples.size() > 1));
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

  auto arm_flush_timer() -> void {
    next_flush_ = std::chrono::steady_clock::now() + args_.flush_interval.inner;
    flush_ready_->notify_one();
  }

  /// Flushes the current pending buffer. The boolean return value tells callers
  /// whether they may continue sending follow-up split requests; diagnostics at
  /// error severity already cancel the pipeline, but this avoids extra noise.
  auto send_request(OpCtx& ctx) -> Task<bool> {
    if (pending_.empty() or not pool_) {
      co_return true;
    }
    co_return co_await send_series(take_pending(), ctx);
  }

  /// Serializes, compresses, and sends a concrete series batch. Returns false
  /// after any emitted error so recursive split handling can stop immediately.
  auto send_series(std::vector<Series> series, OpCtx& ctx) -> Task<bool> {
    if (series.empty()) {
      co_return true;
    }
    auto sample_count = uint64_t{0};
    for (auto const& entry : series) {
      sample_count += entry.samples.size();
    }
    sort_samples(series);
    auto uncompressed = serialize(series);
    if (uncompressed.size() > args_.max_uncompressed_bytes.inner) {
      if (series.size() == 1 and series.front().samples.size() == 1) {
        diagnostic::error(
          "Prometheus remote write request with a single sample exceeds "
          "`max_uncompressed_bytes`")
          .primary(args_.max_uncompressed_bytes)
          .emit(ctx);
        co_return false;
      }
      auto second_half = split_series(series);
      if (not co_await send_series(std::move(series), ctx)) {
        co_return false;
      }
      co_return co_await send_series(std::move(second_half), ctx);
    }
    auto compressed = snappy_compress(std::move(uncompressed), ctx.dh(),
                                      args_.operator_location);
    if (not compressed) {
      co_return false;
    }
    auto headers = headers_;
    http::set(headers, "Content-Type",
              protocol_ == Protocol::v1 ? v1_content_type : v2_content_type);
    http::set(headers, "Content-Encoding", "snappy");
    http::set(headers, "Content-Length", fmt::to_string(compressed->size()));
    http::set(headers, "User-Agent",
              fmt::format("Tenzir/{}", version::version));
    http::set(headers, "X-Prometheus-Remote-Write-Version",
              protocol_ == Protocol::v1 ? "0.1.0" : "2.0.0");
    auto compressed_size = compressed->size();
    auto result
      = co_await (*pool_)->post(std::move(*compressed), std::move(headers));
    if (result.is_err()) {
      diagnostic::error("HTTP request failed: {}",
                        std::move(result).unwrap_err())
        .primary(args_.operator_location)
        .emit(ctx);
      co_return false;
    }
    auto response = std::move(result).unwrap();
    if (not response.is_status_success()) {
      diagnostic::error("HTTP request returned status {}", response.status_code)
        .note("response body: {}", response.body)
        .primary(args_.operator_location)
        .emit(ctx);
      co_return false;
    }
    if (protocol_ == Protocol::v2
        and not check_v2_written_samples(response, sample_count, ctx,
                                         args_.operator_location)) {
      co_return false;
    }
    bytes_write_counter_.add(compressed_size);
    events_write_counter_.add(sample_count);
    co_return true;
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
  std::map<Labels, PendingSeries> pending_;
  uint64_t pending_sample_count_ = 0;
  bool done_ = false;
  MetricsCounter bytes_write_counter_;
  MetricsCounter events_write_counter_;
  Option<std::chrono::steady_clock::time_point> next_flush_;
  mutable Arc<Notify> flush_ready_{std::in_place};
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
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::prometheus

TENZIR_REGISTER_PLUGIN(tenzir::plugins::prometheus::ToPrometheusPlugin)
