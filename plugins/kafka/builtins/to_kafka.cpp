//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "kafka/configuration.hpp"
#include "kafka/operator.hpp"
#include "kafka/to_kafka_legacy.hpp"
#include "tenzir/aws_iam.hpp"
#include "tenzir/co_match.hpp"
#include "tenzir/concept/printable/tenzir/json2.hpp"

#include <tenzir/as_bytes.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/entity_path.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/view3.hpp>

#include <folly/coro/Sleep.h>
#include <librdkafka/rdkafkacpp.h>

#include <chrono>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <string_view>

namespace tenzir::plugins::kafka {

namespace {

/// Number of successfully enqueued records between non-blocking producer polls.
constexpr auto producer_poll_interval = size_t{4096};

/// Returns a `json_printer2` if `expr` is a plain `print_json` or
/// `print_ndjson` call with constant boolean options, enabling the optimized
/// serialization path that bypasses expression evaluation entirely.
auto try_make_json_printer(ast::expression const& expr)
  -> std::optional<json_printer2> {
  auto const* const call = try_as<ast::function_call>(expr);
  if (not call) {
    return std::nullopt;
  }
  if (not call->fn.ref.resolved()) {
    return std::nullopt;
  }
  auto const& segments = call->fn.ref.segments();
  if (segments.size() != 1) {
    return std::nullopt;
  }
  auto compact = false;
  if (segments.front() == "print_ndjson") {
    compact = true;
  } else if (segments.front() != "print_json") {
    return std::nullopt;
  }
  // The first argument must be `this` (the positional argument).
  if (call->args.empty() or not is<ast::this_>(call->args.front())) {
    return std::nullopt;
  }
  // Parse any named options from the remaining arguments.
  auto strip = false;
  auto strip_null_fields = false;
  auto strip_nulls_in_lists = false;
  auto strip_empty_records = false;
  auto strip_empty_lists = false;
  for (auto const& arg : std::span{call->args}.subspan(1)) {
    auto const* const assignment = try_as<ast::assignment>(arg);
    if (not assignment) {
      return std::nullopt;
    }
    auto const* const fp = try_as<ast::field_path>(assignment->left);
    if (not fp or fp->path().size() != 1) {
      return std::nullopt;
    }
    // The value must be the boolean constant `true`.
    auto const* const val = try_as<ast::constant>(assignment->right);
    if (not val) {
      return std::nullopt;
    }
    auto const* const flag = try_as<bool>(val->value);
    if (not flag or not *flag) {
      return std::nullopt;
    }
    auto const name = fp->path().front().id.name;
    if (name == "strip") {
      strip = true;
    } else if (name == "strip_null_fields") {
      strip_null_fields = true;
    } else if (name == "strip_nulls_in_lists") {
      strip_nulls_in_lists = true;
    } else if (name == "strip_empty_records") {
      strip_empty_records = true;
    } else if (name == "strip_empty_lists") {
      strip_empty_lists = true;
    } else {
      return std::nullopt;
    }
  }
  return json_printer2{json_printer_options{
    .style = no_style(),
    .oneline = compact,
    .omit_null_fields = strip_null_fields or strip,
    .omit_nulls_in_lists = strip_nulls_in_lists or strip,
    .omit_empty_records = strip_empty_records or strip,
    .omit_empty_lists = strip_empty_lists or strip,
  }};
}

/// Short poll interval used while draining producer state after queue saturation.
constexpr auto queue_full_short_poll_ms = 10;

/// Long poll interval used after repeated queue-full retries.
constexpr auto queue_full_long_poll_ms = 100;

/// Stores process-wide `to_kafka` defaults from `kafka.yaml`.
auto sink_global_defaults() -> record& {
  static auto defaults = record{};
  return defaults;
}

/// Builds the default `message=` expression used by `to_kafka`.
auto default_message_expression() -> ast::expression {
  auto function
    = ast::entity{{ast::identifier{"print_ndjson", location::unknown}}};
  // Invariant: defaults bypass parser resolution in `OperatorPlugin`, so the
  // entity reference must be pre-resolved here.
  function.ref
    = entity_path{std::string{entity_pkg_std}, {"print_ndjson"}, entity_ns::fn};
  return ast::function_call{
    std::move(function),
    {ast::this_{location::unknown}},
    location::unknown,
    true,
  };
}

/// Parsed arguments for `to_kafka`.
struct ToKafkaArgs {
  std::string topic;
  ast::expression message = default_message_expression();
  std::optional<located<std::string>> key;
  std::optional<located<time>> timestamp;
  located<record> options;
  std::optional<located<std::string>> aws_region;
  std::optional<located<record>> aws_iam;
};

class AsyncKafkaProducer {
public:
  AsyncKafkaProducer(std::string topic, ast::expression message_expr,
                     configuration cfg, Box<RdKafka::Producer> producer,
                     std::optional<json_printer2> printer,
                     metrics_counter write_bytes_counter)
    : topic_{std::move(topic)},
      message_expr_{std::move(message_expr)},
      cfg_{std::move(cfg)},
      producer_{std::move(producer)},
      printer_{std::move(printer)},
      write_bytes_counter_{std::move(write_bytes_counter)} {
  }

  /// Serializes and produces all rows from `input` as Kafka messages.
  /// Returns false on fatal error.
  auto process(table_slice const& input, std::string_view key,
               int64_t timestamp_ms, OpCtx& ctx) -> Task<failure_or<void>> {
    if (printer_) {
      // Optimized path: bypass expression evaluation for plain print_json /
      // print_ndjson calls and serialize each row directly via json_printer2.
      for (auto row : values3(input)) {
        printer_->load_new(row);
        CO_TRY(co_await produce(printer_->bytes(), key, timestamp_ms, ctx));
      }
      co_return {};
    } else {
      // Expression evaluation path: evaluate the message= expression and
      // iterate the resulting string/blob series.
      const auto& messages = eval(message_expr_, input, ctx.dh());
      for (const auto& series : messages) {
        const auto impl = [&](const auto& array) -> Task<failure_or<void>> {
          for (auto row = int64_t{0}; row < array.length(); ++row) {
            if (array.IsNull(row)) {
              diagnostic::warning("expected `string` or `blob`, got `null`")
                .primary(message_expr_)
                .emit(ctx);
              continue;
            }
            CO_TRY(co_await produce(as_bytes(array.Value(row)), key,
                                    timestamp_ms, ctx));
          }
          co_return {};
        };
        if (auto strings = series.as<string_type>()) {
          CO_TRY(co_await impl(*strings->array));
          continue;
        }
        if (auto blob = series.as<blob_type>()) {
          CO_TRY(co_await impl(*blob->array));
          continue;
        }
        diagnostic::warning("expected `string` or `blob`, got `{}`",
                            series.type.kind())
          .primary(message_expr_)
          .emit(ctx);
      }
      co_return {};
    }
    if (produced_since_poll_ > 0) {
      producer_->poll(0);
      produced_since_poll_ = 0;
    }
    co_return {};
  }

  /// Flushes buffered messages and tears down producer resources.
  auto finalize() -> Task<void> {
    constexpr auto max_shutdown_tries = 10;
    constexpr auto min_backoff = std::chrono::milliseconds{100};
    constexpr auto max_backoff = std::chrono::milliseconds{10'000};
    for (auto i = 0; i < max_shutdown_tries; ++i) {
      auto result = producer_->flush(0);
      if (result == RdKafka::ERR_NO_ERROR) {
        co_return;
      }
      TENZIR_ASSERT(result == RdKafka::ERR__TIMED_OUT);
      const auto backoff = std::min(max_backoff, min_backoff * (1 << i));
      co_await folly::coro::sleep(backoff);
    }
    const auto pending = producer_->outq_len();
    if (pending > 0) {
      TENZIR_ERROR("to_kafka: {} messages were not delivered", pending);
    }
  }

private:
  /// Produces one Kafka record and drains delivery state on backpressure.
  auto produce(std::span<const std::byte> bytes, std::string_view key,
               int64_t timestamp_ms, OpCtx& ctx) -> Task<failure_or<void>> {
    while (true) {
      auto result = producer_->produce(
        topic_, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY,
        const_cast<char*>(reinterpret_cast<const char*>(bytes.data())),
        bytes.size(), key.empty() ? nullptr : key.data(), key.size(),
        timestamp_ms, nullptr);
      switch (result) {
        case RdKafka::ERR_NO_ERROR: {
          write_bytes_counter_.add(static_cast<uint64_t>(bytes.size()));
          ++produced_since_poll_;
          if (produced_since_poll_ >= producer_poll_interval) {
            producer_->poll(0);
            produced_since_poll_ = 0;
          }
          co_return {};
        }
        case RdKafka::ERR__QUEUE_FULL: {
          // Constant wait with uniform jitter to avoid thundering herd.
          constexpr auto base_ms = 10;
          constexpr auto jitter_ms = 10;
          static_assert(jitter_ms >= base_ms);
          thread_local auto rng = std::mt19937{std::random_device{}()};
          const auto delay_ms
            = base_ms
              + std::uniform_int_distribution<int>{-jitter_ms, jitter_ms}(rng);
          co_await folly::coro::sleep(std::chrono::milliseconds{delay_ms});
          producer_->poll(0);
          continue;
        }
        default:
          diagnostic::error("failed to produce kafka message: {}",
                            RdKafka::err2str(result))
            .emit(ctx);
          co_return failure::promise();
      }
    }
  }

  std::string topic_;
  ast::expression message_expr_;
  configuration cfg_;               // destroyed after producer_
  Box<RdKafka::Producer> producer_; // destroyed first (declared after cfg_)
  std::optional<json_printer2> printer_;
  metrics_counter write_bytes_counter_;
  size_t produced_since_poll_ = 0;
};

/// Streaming sink operator that serializes events and produces Kafka messages.
class ToKafka final : public Operator<table_slice, void> {
public:
  explicit ToKafka(ToKafkaArgs args) : args_{std::move(args)} {
  }
  ToKafka(ToKafka&&) = default;
  auto operator=(ToKafka&&) -> ToKafka& = default;
  ToKafka(const ToKafka&) = delete;
  auto operator=(const ToKafka&) -> ToKafka& = delete;
  ~ToKafka() override = default;

  auto start(OpCtx& ctx) -> Task<void> override {
    auto write_bytes_counter = ctx.make_counter(
      metrics_label{
        .key = "operator",
        .value = "to_kafka",
      },
      metrics_direction::write, metrics_visibility::external_);
    auto auth = co_await resolve_aws_iam_auth(
      args_.aws_iam, args_.aws_region, ctx,
      AwsIamRegionRequirement::required_with_iam);
    if (not auth) {
      done_ = true;
      co_return;
    }
    auto config = sink_global_defaults();
    auto cfg
      = configuration::make(config, auth->options, auth->credentials, ctx.dh());
    if (not cfg) {
      diagnostic::error("failed to create kafka configuration: {}", cfg.error())
        .emit(ctx);
      done_ = true;
      co_return;
    }
    auto user_options = args_.options;
    if (auth->options) {
      user_options.inner["sasl.mechanism"] = "OAUTHBEARER";
    }
    if (auto ok = co_await ctx.resolve_secrets(
          configure_or_request(user_options, *cfg, ctx.dh()));
        not ok) {
      done_ = true;
      co_return;
    }
    auto error = std::string{};
    auto* raw_producer = RdKafka::Producer::create(cfg->underlying(), error);
    if (raw_producer == nullptr) {
      diagnostic::error("failed to create kafka producer: {}", error).emit(ctx);
      done_ = true;
      co_return;
    }
    producer_.emplace(args_.topic, args_.message, std::move(*cfg),
                      Box<RdKafka::Producer>::from_unique_ptr(
                        std::unique_ptr<RdKafka::Producer>{raw_producer}),
                      try_make_json_printer(args_.message),
                      std::move(write_bytes_counter));
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (done_ or not producer_ or input.rows() == 0) {
      co_return;
    }
    const auto key = args_.key ? args_.key->inner : "";
    auto timestamp_ms = int64_t{0};
    if (args_.timestamp and args_.timestamp->inner != time{}) {
      timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       args_.timestamp->inner.time_since_epoch())
                       .count();
    }
    if (not co_await producer_->process(input, key, timestamp_ms, ctx)) {
      done_ = true;
    }
    co_return;
  }

  auto finalize(OpCtx&) -> Task<FinalizeBehavior> override {
    co_await producer_->finalize();
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

private:
  ToKafkaArgs args_;
  std::optional<AsyncKafkaProducer> producer_;
  bool done_ = false;
};

/// Plugin entrypoint that parses `to_kafka` arguments and creates operators.
class ToKafkaPlugin final
  : public virtual operator_plugin2<legacy::to_kafka_operator>,
    public virtual OperatorPlugin {
public:
  auto initialize(const record& unused_plugin_config,
                  const record& global_config) -> caf::error override {
    if (not unused_plugin_config.empty()) {
      return diagnostic::error("`{}.yaml` is unused; Use `kafka.yaml` instead",
                               this->name())
        .to_error();
    }
    auto defaults = record{};
    [&] {
      auto ptr = global_config.find("plugins");
      if (ptr == global_config.end()) {
        return;
      }
      const auto* plugin_config = try_as<record>(&ptr->second);
      if (not plugin_config) {
        return;
      }
      auto kafka_config_ptr = plugin_config->find("kafka");
      if (kafka_config_ptr == plugin_config->end()) {
        return;
      }
      const auto* kafka_config = try_as<record>(&kafka_config_ptr->second);
      if (not kafka_config or kafka_config->empty()) {
        return;
      }
      defaults = flatten(*kafka_config);
    }();
    if (not defaults.contains("bootstrap.servers")) {
      defaults["bootstrap.servers"] = "localhost";
    }
    if (not defaults.contains("client.id")) {
      defaults["client.id"] = "tenzir";
    }
    sink_global_defaults() = std::move(defaults);
    return caf::none;
  }

  auto name() const -> std::string override {
    return "to_kafka";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    return legacy::make_to_kafka(std::move(inv), ctx, sink_global_defaults());
  }

  auto describe() const -> Description override {
    auto initial = ToKafkaArgs{};
    initial.options = located{record{}, location::unknown};
    auto d = Describer<ToKafkaArgs, ToKafka>{std::move(initial)};
    d.positional("topic", &ToKafkaArgs::topic);
    d.named_optional("message", &ToKafkaArgs::message, "blob|string");
    d.named("key", &ToKafkaArgs::key);
    d.named("timestamp", &ToKafkaArgs::timestamp);
    auto options_arg = d.named_optional("options", &ToKafkaArgs::options);
    auto aws_region_arg = d.named("aws_region", &ToKafkaArgs::aws_region);
    auto aws_iam_arg = d.named("aws_iam", &ToKafkaArgs::aws_iam);
    d.validate([=](ValidateCtx& ctx) -> Empty {
      if (auto options = ctx.get(options_arg); options) {
        if (not validate_options(*options, ctx)) {
          return {};
        }
      }
      if (auto iam = ctx.get(aws_iam_arg); iam) {
        if (auto options = ctx.get(options_arg); options) {
          if (not check_sasl_mechanism(*options, ctx)) {
            return {};
          }
        }
        auto aws = aws_iam_options::from_record(*iam, ctx);
        if (not aws) {
          return {};
        }
        if (not ctx.get(aws_region_arg) and not aws->region) {
          diagnostic::error(
            "`aws_region` is required for Kafka MSK authentication")
            .primary(iam->source)
            .emit(ctx);
          return {};
        }
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::kafka

TENZIR_REGISTER_PLUGIN(tenzir::plugins::kafka::ToKafkaPlugin)
