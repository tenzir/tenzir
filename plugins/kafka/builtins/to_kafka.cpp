//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "kafka/configuration.hpp"
#include "kafka/operator.hpp"
#include "tenzir/generator.hpp"
#include "tenzir/operator_control_plane.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/resolve.hpp"

#include <tenzir/tql2/plugin.hpp>

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
  std::optional<configuration::aws_iam_options> aws;

  friend auto inspect(auto& f, to_kafka_args& x) -> bool {
    return f.object(x).fields(f.field("op", x.op), f.field("topic", x.topic),
                              f.field("message", x.message),
                              f.field("key", x.key),
                              f.field("timestamp", x.timestamp),
                              f.field("options", x.options),
                              f.field("aws", x.aws));
  }
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
    co_yield {};
    auto& dh = ctrl.diagnostics();
    auto config = configuration::make(config_, args_.aws, dh);
    if (not config) {
      diagnostic::error(std::move(config).error()).primary(args_.op).emit(dh);
      co_return;
    }
    co_yield ctrl.resolve_secrets_must_yield(
      configure_or_request(args_.options, *config, ctrl.diagnostics()));
    auto p = producer::make(std::move(*config));
    if (not p) {
      diagnostic::error(std::move(p).error()).primary(args_.op).emit(dh);
      co_return;
    }
    const auto guard = detail::scope_guard([&] noexcept {
      TENZIR_DEBUG("[to_kafka] waiting 10 seconds to flush pending messages");
      if (const auto err = p->flush(10s)) {
        TENZIR_WARN(err);
      }
      const auto num_messages = p->queue_size();
      if (num_messages > 0) {
        TENZIR_ERROR("[to_kafka] {} messages were not delivered", num_messages);
      }
    });
    const auto key = args_.key ? args_.key->inner : "";
    const auto timestamp = args_.timestamp ? args_.timestamp->inner : time{};
    // Create default expression if message not provided
    auto default_message = ast::function_call{
      ast::entity{{ast::identifier{"print_json", location::unknown}}},
      {ast::this_{location::unknown}},
      location::unknown,
      true // method call
    };
    for (const auto& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      const auto& ms = eval(args_.message, slice, dh);
      for (const auto& s : ms) {
        match(
          *s.array,
          [&](const concepts::one_of<arrow::BinaryArray,
                                     arrow::StringArray> auto& array) {
            for (auto i = int64_t{}; i < array.length(); ++i) {
              if (array.IsNull(i)) {
                diagnostic::warning("expected `string` or `blob`, got `null`")
                  .primary(args_.message)
                  .emit(dh);
                continue;
              }
              if (auto e = p->produce(args_.topic, as_bytes(array.Value(i)),
                                      key, timestamp)) {
                diagnostic::error(std::move(e)).primary(args_.op).emit(dh);
              }
            }
          },
          [&](const auto&) {
            diagnostic::warning("expected `string` or `blob`, got `{}`",
                                s.type.kind())
              .primary(args_.message)
              .emit(dh);
          });
      }
      p->poll(0ms);
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
          .named("aws_iam", iam_opts)
          .named_optional("options", args.options)
          .parse(inv, ctx));
    if (iam_opts) {
      TRY(check_sasl_mechanism(args.options, ctx));
      TRY(check_sasl_mechanism(located{config_, iam_opts->source}, ctx));
      args.options.inner["sasl.mechanism"] = "OAUTHBEARER";
      TRY(args.aws, configuration::aws_iam_options::from_record(
                      std::move(iam_opts).value(), ctx));
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
