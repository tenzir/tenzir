//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "kafka/operator.hpp"
#include "tenzir/tql2/eval.hpp"

#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::kafka {
namespace {

constexpr auto stringify = detail::overload{
  [](const concepts::arithmetic auto& value) -> std::optional<std::string> {
    return fmt::to_string(value);
  },
  [](std::string value) -> std::optional<std::string> {
    return value;
  },
  [](const auto&) -> std::optional<std::string> {
    return std::nullopt;
  },
};

class load_plugin final
  : public virtual operator_plugin2<loader_adapter<kafka_loader>> {
public:
  auto initialize(const record& config,
                  const record& /* global_config */) -> caf::error override {
    config_ = config;
    if (!config_.contains("bootstrap.servers")) {
      config_["bootstrap.servers"] = "localhost";
    }
    if (!config_.contains("client.id")) {
      config_["client.id"] = "tenzir";
    }
    return caf::none;
  }

  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto args = loader_args{};
    auto offset = std::optional<ast::expression>{};
    auto options = std::optional<located<record>>{};
    TRY(argument_parser2::operator_(name())
          .add("topic", args.topic)
          .add("count", args.count)
          .add("exit", args.exit)
          .add("offset", offset)
          .add("options", options)
          .parse(inv, ctx));
    if (offset) {
      TRY(auto evaluated, const_eval(offset.value(), ctx.dh()));
      constexpr auto f = detail::overload{
        [](const std::integral auto& value) -> std::optional<std::string> {
          return fmt::to_string(value);
        },
        [](const std::string& value) -> std::optional<std::string> {
          return value;
        },
        [](const auto&) -> std::optional<std::string> {
          return std::nullopt;
        }};
      auto result = caf::visit(f, evaluated);
      if (not result) {
        diagnostic::error("expected `string` or `int`")
          .primary(offset->get_location())
          .emit(ctx);
        return failure::promise();
      }
      if (not offset_parser()(result.value())) {
        diagnostic::error("invalid `offset` value")
          .primary(offset->get_location())
          .note(
            "must be `beginning`, `end`, `store`, `<offset>` or `-<offset>`")
          .emit(ctx);
        return failure::promise();
      }
      args.offset = located{std::move(result).value(), offset->get_location()};
    }
    if (options) {
      for (auto& [k, v] : options->inner) {
        if (auto str = caf::visit(stringify, v)) {
          args.options.inner.emplace_back(k, std::move(str).value());
          continue;
        }
        diagnostic::error(
          "expected type `number`, `bool` or `string` for option")
          .primary(options->source)
          .emit(ctx);
        return failure::promise();
      }
    }
    return std::make_unique<loader_adapter<kafka_loader>>(
      kafka_loader{std::move(args), config_});
  }

private:
  record config_;
};

class save_plugin final
  : public virtual operator_plugin2<saver_adapter<kafka_saver>> {
  auto initialize(const record& config,
                  const record& /* global_config */) -> caf::error override {
    config_ = config;
    if (!config_.contains("bootstrap.servers")) {
      config_["bootstrap.servers"] = "localhost";
    }
    if (!config_.contains("client.id")) {
      config_["client.id"] = "tenzir";
    }
    return caf::none;
  }

  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto args = saver_args{};
    auto ts = std::optional<located<time>>{};
    auto options = std::optional<located<record>>{};
    TRY(argument_parser2::operator_(name())
          .add("topic", args.topic)
          .add("key", args.key)
          .add("timestamp", ts)
          .add("options", options)
          .parse(inv, ctx));
    // HACK: Should directly accept a time
    if (ts) {
      args.timestamp = located{fmt::to_string(ts->inner), ts->source};
    }
    if (options) {
      for (auto& [k, v] : options->inner) {
        if (auto str = caf::visit(stringify, v)) {
          args.options.inner.emplace_back(k, std::move(str).value());
          continue;
        }
        diagnostic::error(
          "expected type `number`, `bool` or `string` for option")
          .primary(options->source)
          .emit(ctx);
        return failure::promise();
      }
    }
    return std::make_unique<saver_adapter<kafka_saver>>(
      kafka_saver{std::move(args), config_});
  }

private:
  record config_;
};

} // namespace
} // namespace tenzir::plugins::kafka

TENZIR_REGISTER_PLUGIN(tenzir::plugins::kafka::load_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::kafka::save_plugin)
