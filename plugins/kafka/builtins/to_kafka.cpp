//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "kafka/librdkafka_utils.hpp"
#include "kafka/operator_args.hpp"
#include "kafka/to_kafka_legacy.hpp"
#include "tenzir/aws_iam.hpp"
#include "tenzir/concept/printable/tenzir/json2.hpp"

#include <tenzir/as_bytes.hpp>
#include <tenzir/atomic.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/entity_path.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/view3.hpp>

#include <fmt/format.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Collect.h>
#include <folly/coro/Sleep.h>
#include <librdkafka/rdkafkacpp.h>

#include <chrono>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <string_view>
#include <vector>

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
  uint64_t jobs = 1;
};

/// Bounded queue of table slices feeding the parallel producer workers.
using InputQueue = folly::coro::BoundedQueue<table_slice>;

class AsyncKafkaProducer {
public:
  AsyncKafkaProducer(std::string topic, ast::expression message_expr,
                     std::optional<ResolvedAwsIamAuth> auth,
                     producer_configuration cfg,
                     Box<RdKafka::Producer> producer,
                     MetricsCounter write_bytes_counter)
    : topic_{std::move(topic)},
      message_expr_{std::move(message_expr)},
      auth_{std::move(auth)},
      cfg_{std::move(cfg)},
      producer_{std::move(producer)},
      printer_{try_make_json_printer(message_expr_)},
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
    }
    if (produced_since_poll_ > 0) {
      producer_->poll(0);
      produced_since_poll_ = 0;
    }
    co_return {};
  }

  /// Flushes buffered messages and tears down producer resources.
  auto finalize(diagnostic_handler* dh) -> Task<void> {
    constexpr auto max_retries = 1000;
    for (auto retry = 0; retry < max_retries; ++retry) {
      auto result = producer_->flush(0);
      const auto pending = producer_->outq_len();
      if (result == RdKafka::ERR_NO_ERROR and pending == 0) {
        co_return;
      }
      if (result != RdKafka::ERR_NO_ERROR
          and result != RdKafka::ERR__TIMED_OUT) {
        if (dh) {
          auto out
            = diagnostic::error(
                "failed to flush produced Kafka messages for `{}`", topic_)
                .note("reason={}", RdKafka::err2str(result))
                .note("outbound.messages.pending={}", pending);
          out = add_connection_and_auth_notes(std::move(out));
          out = add_connectivity_hint(std::move(out));
          std::move(out).emit(*dh);
        }
        co_return;
      }
      co_await folly::coro::sleep(std::chrono::milliseconds{10});
    }
    if (dh) {
      const auto pending = producer_->outq_len();
      auto out = diagnostic::error(
                   "failed to flush produced Kafka messages for `{}`", topic_)
                   .note("reason=producer flush timed out after {} retries",
                         max_retries)
                   .note("outbound.messages.pending={}", pending);
      out = add_connection_and_auth_notes(std::move(out));
      out = add_connectivity_hint(std::move(out));
      std::move(out).emit(*dh);
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
        default: {
          auto out
            = diagnostic::error("failed to produce kafka message for `{}`: {}",
                                topic_, RdKafka::err2str(result));
          out = add_connection_and_auth_notes(std::move(out));
          out = add_connectivity_hint(std::move(out));
          std::move(out).emit(ctx);
          co_return failure::promise();
        }
      }
    }
  }

  auto add_connection_and_auth_notes(diagnostic_builder out) const
    -> diagnostic_builder {
    auto* conf = cfg_.conf ? cfg_.conf.get() : nullptr;
    out = add_kafka_connection_diagnostic_notes(std::move(out), conf);
    if (cfg_.oauth_callback) {
      out = std::move(out).note("oauth.callback_servicing=producer_poll");
    }
    if (auth_ and auth_->options and auth_->credentials) {
      out = add_kafka_aws_iam_diagnostic_notes(std::move(out),
                                               auth_->credentials);
    }
    return out;
  }

  auto add_connectivity_hint(diagnostic_builder out) const
    -> diagnostic_builder {
    if (auth_ and auth_->options and auth_->credentials) {
      return std::move(out).hint(
        "verify bootstrap broker reachability, TLS trust/cert setup, and "
        "IAM/SASL settings (region, mechanism, and assume-role inputs)");
    }
    return std::move(out).hint(
      "verify bootstrap broker reachability and that "
      "`security.protocol`/`sasl.mechanism` match broker requirements");
  }

  std::string topic_;
  ast::expression message_expr_;
  std::optional<ResolvedAwsIamAuth> auth_;
  producer_configuration cfg_;      // destroyed after producer_
  Box<RdKafka::Producer> producer_; // destroyed first (declared after cfg_)
  std::optional<json_printer2> printer_;
  MetricsCounter write_bytes_counter_;
  size_t produced_since_poll_ = 0;
};

/// Streaming sink operator that serializes events and produces Kafka messages.
class ToKafka final : public Operator<table_slice, void> {
public:
  explicit ToKafka(ToKafkaArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    write_bytes_counter_ = ctx.make_counter(
      MetricsLabel{
        "operator",
        "to_kafka",
      },
      MetricsDirection::write, MetricsVisibility::external_);
    auto auth = co_await resolve_aws_iam_auth(
      args_.aws_iam, args_.aws_region, ctx,
      AwsIamRegionRequirement::required_with_iam);
    if (not auth) {
      done_.store(true, std::memory_order_release);
      co_return;
    }
    auth_ = *auth;
    auto config = sink_global_defaults();
    auto cfg = make_producer_configuration(config, auth->options,
                                           auth->credentials, ctx.dh());
    if (not cfg) {
      diagnostic::error("failed to create kafka configuration: {}", cfg.error())
        .emit(ctx);
      done_.store(true, std::memory_order_release);
      co_return;
    }
    auto user_options = args_.options;
    if (auth->options) {
      user_options.inner["sasl.mechanism"] = "OAUTHBEARER";
    }
    if (auto ok = co_await ctx.resolve_secrets(
          configure_producer_or_request_secrets(*cfg, user_options, ctx.dh()));
        not ok) {
      done_.store(true, std::memory_order_release);
      co_return;
    }
    if (args_.jobs <= 1) {
      // Single-worker path: drive the producer directly from process() with no
      // queue overhead — same sequential behavior as before workers= existed.
      auto error = std::string{};
      TENZIR_ASSERT(cfg->conf);
      auto* raw_producer = RdKafka::Producer::create(cfg->conf.get(), error);
      if (raw_producer == nullptr) {
        diagnostic::error("failed to create kafka producer: {}", error)
          .emit(ctx);
        done_.store(true, std::memory_order_release);
        co_return;
      }
      producer_.emplace(args_.topic, args_.message, auth_, std::move(*cfg),
                        Box<RdKafka::Producer>::from_non_null(
                          std::unique_ptr<RdKafka::Producer>{raw_producer}),
                        write_bytes_counter_);
      co_return;
    }
    // Multi-worker path: feed a bounded queue from process() and drain it with
    // N independent producer workers.
    ctx_ = &ctx;
    input_queue_ = std::make_shared<InputQueue>(args_.jobs * 2);
    for (auto i = uint64_t{0}; i < args_.jobs; ++i) {
      auto worker_cfg = *cfg;
      auto error = std::string{};
      TENZIR_ASSERT(worker_cfg.conf);
      auto* raw_producer
        = RdKafka::Producer::create(worker_cfg.conf.get(), error);
      if (raw_producer == nullptr) {
        diagnostic::error("failed to create kafka producer: {}", error)
          .emit(ctx);
        done_.store(true, std::memory_order_release);
        co_return;
      }
      worker_handles_.push_back(ctx.spawn_task(worker_loop(AsyncKafkaProducer{
        args_.topic, args_.message, auth_, std::move(worker_cfg),
        Box<RdKafka::Producer>::from_non_null(
          std::unique_ptr<RdKafka::Producer>{raw_producer}),
        write_bytes_counter_})));
    }
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (done_.load(std::memory_order_acquire) or input.rows() == 0) {
      co_return;
    }
    if (producer_) {
      // Single-worker: original direct path.
      if (not co_await producer_->process(input,
                                          args_.key ? args_.key->inner : "",
                                          compute_timestamp_ms(), ctx)) {
        done_.store(true, std::memory_order_release);
      }
      co_return;
    }
    if (input_queue_) {
      co_await input_queue_->enqueue(std::move(input));
    }
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    if (producer_) {
      co_await producer_->finalize(&ctx.dh());
      co_return FinalizeBehavior::done;
    }
    if (worker_handles_.empty()) {
      co_return FinalizeBehavior::done;
    }
    // Signal workers to stop, then enqueue one empty slice per worker to
    // unblock any that are waiting on dequeue, then join all of them.
    done_.store(true, std::memory_order_release);
    TENZIR_ASSERT(input_queue_);
    for (auto i = size_t{0}; i < worker_handles_.size(); ++i) {
      co_await input_queue_->enqueue(table_slice{});
    }
    auto joins = std::vector<Task<void>>{};
    joins.reserve(worker_handles_.size());
    for (auto& handle : worker_handles_) {
      joins.push_back(handle.join());
    }
    co_await folly::coro::collectAllRange(std::move(joins));
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return done_.load(std::memory_order_acquire) ? OperatorState::done : OperatorState::unspecified;
  }

private:
  auto compute_timestamp_ms() const -> int64_t {
    if (args_.timestamp and args_.timestamp->inner != time{}) {
      return std::chrono::duration_cast<std::chrono::milliseconds>(
               args_.timestamp->inner.time_since_epoch())
        .count();
    }
    return int64_t{0};
  }

  auto worker_loop(AsyncKafkaProducer producer) -> Task<void> {
    const auto key = args_.key ? args_.key->inner : std::string{};
    const auto timestamp_ms = compute_timestamp_ms();
    while (true) {
      auto next = co_await input_queue_->dequeue();
      if (next.rows() == 0) {
        break;
      }
      if (not co_await producer.process(next, key, timestamp_ms, *ctx_)) {
        done_.store(true, std::memory_order_release);
        break;
      }
    }
    TENZIR_ASSERT(ctx_);
    co_await producer.finalize(&ctx_->dh());
  }

  ToKafkaArgs args_;
  std::optional<ResolvedAwsIamAuth> auth_;
  std::optional<AsyncKafkaProducer> producer_;
  MetricsCounter write_bytes_counter_;
  std::shared_ptr<InputQueue> input_queue_;
  std::vector<AsyncHandle<void>> worker_handles_;
  OpCtx* ctx_ = nullptr;
  Atomic<bool> done_{false};
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

  auto make(operator_factory_invocation inv, session ctx) const
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
    d.named_optional("_jobs", &ToKafkaArgs::jobs);
    auto options_arg = d.named_optional("options", &ToKafkaArgs::options);
    auto aws_region_arg = d.named("aws_region", &ToKafkaArgs::aws_region);
    auto aws_iam_arg = d.named("aws_iam", &ToKafkaArgs::aws_iam);
    d.validate([=](DescribeCtx& ctx) -> Empty {
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
