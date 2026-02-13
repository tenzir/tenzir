//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "kafka/configuration.hpp"
#include "kafka/operator.hpp"
#include "tenzir/aws_iam.hpp"
#include "tenzir/concept/printable/tenzir/json2.hpp"
#include "tenzir/generator.hpp"
#include "tenzir/operator_control_plane.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/resolve.hpp"

#include <tenzir/tql2/plugin.hpp>

#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>
#include <variant>

namespace tenzir::plugins::kafka {
namespace {

struct to_kafka_args {
  location op;
  std::string topic;
  ast::expression message = ast::function_call{
    ast::entity{{ast::identifier{"print_ndjson", location::unknown}}},
    {ast::this_{location::unknown}},
    location::unknown,
    true // method call
  };
  std::optional<located<std::string>> key;
  std::optional<located<time>> timestamp;
  located<record> options;
  std::optional<located<std::string>> aws_region;
  std::optional<tenzir::aws_iam_options> aws;
  uint64_t jobs = 0;

  friend auto inspect(auto& f, to_kafka_args& x) -> bool {
    return f.object(x).fields(f.field("op", x.op), f.field("topic", x.topic),
                              f.field("message", x.message),
                              f.field("key", x.key),
                              f.field("timestamp", x.timestamp),
                              f.field("options", x.options),
                              f.field("aws_region", x.aws_region),
                              f.field("aws", x.aws), f.field("jobs", x.jobs));
  }
};

// Owns the shared work queue between the main coroutine and worker threads.
// An empty table_slice signals shutdown and is left in the queue so that all
// workers observe it.
struct produce_synchronizer {
  auto put(table_slice slice) -> generator<std::monostate> {
    auto lock = std::unique_lock{mutex_};
    inputs_.push_back(std::move(slice));
    cv_.notify_one();
    constexpr static auto max_queue_size = 20;
    while (inputs_.size() > max_queue_size) {
      lock.unlock();
      co_yield {};
      lock.lock();
    }
  }

  auto shutdown() -> void {
    {
      auto lock = std::unique_lock{mutex_};
      inputs_.emplace_back();
    }
    cv_.notify_all();
  }

  // Blocks until a slice is available. Returns an empty slice on shutdown.
  auto take() -> table_slice {
    auto lock = std::unique_lock{mutex_};
    cv_.wait(lock, [&] {
      return not inputs_.empty();
    });
    auto const& front = inputs_.front();
    if (front.rows() == 0) {
      return {};
    }
    auto result = std::move(inputs_.front());
    inputs_.pop_front();
    return result;
  }

private:
  std::deque<table_slice> inputs_;
  std::mutex mutex_;
  std::condition_variable cv_;
};

// Owns a Kafka producer and sends table slices to Kafka.
class produce_worker {
public:
  // Takes a fully-configured configuration (secrets already resolved).
  static auto make(configuration config, to_kafka_args const& args,
                   diagnostic_handler& dh) -> std::optional<produce_worker> {
    auto p = producer::make(std::move(config));
    if (not p) {
      diagnostic::error(std::move(p).error()).primary(args.op).emit(dh);
      return std::nullopt;
    }
    return produce_worker{std::move(*p), args, dh};
  }

  static auto try_make_json_printer(ast::expression const& expr)
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

  auto send(table_slice const& slice) -> void {
    if (printer_) {
      send_optimized(slice);
    } else {
      send_with_expression(slice);
    }
  }

  auto send_with_expression(table_slice const& slice) -> void {
    auto const& ms = eval(args_.message, slice, dh_);
    for (auto const& s : ms) {
      match(
        *s.array,
        [&](concepts::one_of<arrow::BinaryArray, arrow::StringArray> auto const&
              array) {
          for (auto i = int64_t{}; i < array.length(); ++i) {
            if (array.IsNull(i)) {
              diagnostic::warning("expected `string` or `blob`, got `null`")
                .primary(args_.message)
                .emit(dh_);
              continue;
            }
            if (auto e = producer_.produce(
                  args_.topic, as_bytes(array.Value(i)), key_, timestamp_);
                e.valid()) {
              diagnostic::error(std::move(e)).primary(args_.op).emit(dh_);
            }
          }
        },
        [&](auto const&) {
          diagnostic::warning("expected `string` or `blob`, got `{}`",
                              s.type.kind())
            .primary(args_.message)
            .emit(dh_);
        });
    }
    producer_.poll(0ms);
  }

  auto send_optimized(table_slice const& slice) -> void {
    TENZIR_ASSERT(printer_);

    for (auto row : values3(slice)) {
      printer_->load_new(row);
      if (auto e
          = producer_.produce(args_.topic, printer_->bytes(), key_, timestamp_);
          e.valid()) {
        diagnostic::error(std::move(e)).primary(args_.op).emit(dh_);
      }
    }
  }

private:
  produce_worker(producer p, to_kafka_args const& args, diagnostic_handler& dh)
    : producer_{std::move(p)},
      args_{args},
      dh_{dh},
      key_{args.key ? args.key->inner : ""},
      timestamp_{args.timestamp ? args.timestamp->inner : time{}},
      printer_{try_make_json_printer(args.message)} {
  }

  producer producer_;
  to_kafka_args const& args_;
  diagnostic_handler& dh_;
  std::string key_;
  time timestamp_;
  std::optional<json_printer2> printer_;
};

class to_kafka_operator final : public crtp_operator<to_kafka_operator> {
public:
  to_kafka_operator() = default;

  to_kafka_operator(to_kafka_args args, record config)
    : args_{std::move(args)}, config_{std::move(config)} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    auto& dh = ctrl.diagnostics();
    // Resolve secrets if explicit credentials or role are provided.
    auto resolved_creds = std::optional<tenzir::resolved_aws_credentials>{};
    if (args_.aws
        and (args_.aws->has_explicit_credentials() or args_.aws->role)) {
      resolved_creds.emplace();
      auto requests = args_.aws->make_secret_requests(*resolved_creds, dh);
      co_yield ctrl.resolve_secrets_must_yield(std::move(requests));
    }
    // Use top-level aws_region if provided, otherwise fall back to aws_iam.
    if (args_.aws_region) {
      if (not resolved_creds) {
        resolved_creds.emplace();
      }
      resolved_creds->region = args_.aws_region->inner;
    }
    co_yield {};
    auto config = configuration::make(config_, args_.aws, resolved_creds, dh);
    if (not config) {
      diagnostic::error(std::move(config).error()).primary(args_.op).emit(dh);
      co_return;
    }
    co_yield ctrl.resolve_secrets_must_yield(
      configure_or_request(args_.options, *config, ctrl.diagnostics()));
    if (args_.jobs == 0) {
      // Single-threaded path.
      auto worker = produce_worker::make(std::move(*config), args_, dh);
      if (not worker) {
        co_return;
      }
      for (auto const& slice : input) {
        if (slice.rows() == 0) {
          co_yield {};
          continue;
        }
        worker->send(slice);
      }
    } else {
      // Multi-threaded path.
      auto sync = produce_synchronizer{};
      auto threads = std::vector<std::thread>{};
      threads.reserve(args_.jobs);
      auto guard = detail::scope_guard{[&]() noexcept {
        sync.shutdown();
        for (auto& thread : threads) {
          thread.join();
        }
      }};
      for (auto i = uint64_t{0}; i < args_.jobs; ++i) {
        threads.emplace_back([&, sdh = ctrl.shared_diagnostics()]() mutable {
          caf::detail::set_thread_name("kafka_produce");
          auto worker = produce_worker::make(*config, args_, sdh);
          if (not worker) {
            return;
          }
          while (true) {
            auto slice = sync.take();
            if (slice.rows() == 0) {
              break;
            }
            worker->send(slice);
          }
        });
      }
      for (auto const& slice : input) {
        if (slice.rows() == 0) {
          co_yield {};
          continue;
        }
        for (auto _ : sync.put(slice)) {
          co_yield {};
        }
      }
    }
  }

  auto name() const -> std::string override {
    return "to_kafka";
  }

  auto detached() const -> bool override {
    return true;
  }

  auto optimize(const expression&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, to_kafka_operator& x) -> bool {
    return f.object(x).fields(f.field("args_", x.args_),
                              f.field("config_", x.config_));
  }

private:
  to_kafka_args args_;
  record config_;
};

class to_kafka final : public operator_plugin2<to_kafka_operator> {
  auto initialize(const record& unused_plugin_config,
                  const record& global_config) -> caf::error override {
    if (not unused_plugin_config.empty()) {
      return diagnostic::error("`{}.yaml` is unused; Use `kafka.yaml` instead",
                               this->name())
        .to_error();
    }
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
      config_ = flatten(*kafka_config);
    }();
    if (not config_.contains("bootstrap.servers")) {
      config_["bootstrap.servers"] = "localhost";
    }
    if (not config_.contains("client.id")) {
      config_["client.id"] = "tenzir";
    }
    return caf::none;
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = to_kafka_args{};
    TRY(resolve_entities(args.message, ctx));
    auto iam_opts = std::optional<located<record>>{};
    TRY(argument_parser2::operator_(name())
          .positional("topic", args.topic)
          .named_optional("message", args.message, "blob|string")
          .named("key", args.key)
          .named("timestamp", args.timestamp)
          .named("aws_region", args.aws_region)
          .named("aws_iam", iam_opts)
          .named_optional("options", args.options)
          .named_optional("_jobs", args.jobs)
          .parse(inv, ctx));
    if (iam_opts) {
      TRY(check_sasl_mechanism(args.options, ctx));
      TRY(check_sasl_mechanism(located{config_, iam_opts->source}, ctx));
      args.options.inner["sasl.mechanism"] = "OAUTHBEARER";
      TRY(args.aws, tenzir::aws_iam_options::from_record(
                      std::move(iam_opts).value(), ctx));
      // Region is required for Kafka MSK authentication.
      // Use top-level aws_region if provided, otherwise require aws_iam.region.
      if (not args.aws_region and not args.aws->region) {
        diagnostic::error(
          "`aws_region` is required for Kafka MSK authentication")
          .primary(args.aws->loc)
          .emit(ctx);
        return failure::promise();
      }
    }
    TRY(validate_options(args.options, ctx));
    return std::make_unique<to_kafka_operator>(std::move(args), config_);
  }

private:
  record config_;
};

} // namespace
} // namespace tenzir::plugins::kafka

TENZIR_REGISTER_PLUGIN(tenzir::plugins::kafka::to_kafka)
