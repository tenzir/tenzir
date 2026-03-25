//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"

#include <tenzir/argument_parser2.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/secret.hpp>
#include <tenzir/secret_resolution.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::secret {

namespace {

struct AssertSecretArgs {
  located<class secret> secret;
  located<data> expected;
};

class AssertSecret final : public Operator<table_slice, table_slice> {
public:
  explicit AssertSecret(AssertSecretArgs args)
    : secret_{std::move(args.secret)}, expected_{std::move(args.expected)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    // Resolve the secret using the new async API
    auto result = resolved_secret_value{};
    auto requests = std::vector<secret_request>{};
    requests.emplace_back(secret_, result);
    auto res = co_await ctx.resolve_secrets(std::move(requests));
    if (not res) {
      co_return;
    }
    // Compare secret to expected value
    const auto s = result.blob();
    auto e = std::span<const std::byte>{};
    if (const auto* b = try_as<blob>(expected_.inner)) {
      e = std::span{*b};
    } else if (const auto* str = try_as<std::string>(expected_.inner)) {
      e = std::span{
        reinterpret_cast<const std::byte*>(str->data()),
        reinterpret_cast<const std::byte*>(str->data() + str->size()),
      };
    }
    if (not std::ranges::equal(s, e)) {
      diagnostic::error("secret does not match expected value")
        .primary(secret_, "['{}']", fmt::join(s, "', '"))
        .primary(expected_, "['{}']", fmt::join(e, "', '"))
        .emit(ctx.dh());
    }
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    // Pass through data (should not be called since we return done)
    co_await push(std::move(input));
  }

  auto state() -> OperatorState override {
    return OperatorState::done;
  }

private:
  located<class secret> secret_;
  located<data> expected_;
};

class assert_secret_operator final
  : public crtp_operator<assert_secret_operator> {
public:
  assert_secret_operator() = default;

  explicit assert_secret_operator(located<class secret> s,
                                  located<data> expected)
    : secret_{std::move(s)}, expected_{std::move(expected)} {
  }

  auto operator()(generator<table_slice>, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto result = resolved_secret_value{};
    co_yield ctrl.resolve_secrets_must_yield({{secret_, result}});
    const auto s = result.blob();
    auto e = std::span<const std::byte>{};
    if (const auto* b = try_as<blob>(expected_.inner)) {
      e = std::span{*b};
    } else if (const auto* s = try_as<std::string>(expected_.inner)) {
      e = std::span{
        reinterpret_cast<const std::byte*>(s->data()),
        reinterpret_cast<const std::byte*>(s->data() + s->size()),
      };
    }
    if (not std::ranges::equal(s, e)) {
      diagnostic::error("secret does not match expected value")
        .primary(secret_, "['{}']", fmt::join(s, "', '"))
        .primary(expected_, "['{}']", fmt::join(e, "', '"))
        .emit(ctrl.diagnostics());
      co_return;
    }
    co_yield {};
  }

  auto name() const -> std::string override {
    return "assert_secret";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)order;
    (void)filter;
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, assert_secret_operator& x) -> bool {
    return f.object(x).fields(f.field("secret_", x.secret_),
                              f.field("value", x.expected_));
  }

private:
  located<class secret> secret_ = {};
  located<data> expected_ = {};
};

class testing_operator_plugin final
  : public virtual operator_plugin2<assert_secret_operator>,
    public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return assert_secret_operator{}.name();
  }

  auto initialize(const record&, const record& global_config)
    -> caf::error override {
    const auto v = try_get_or(global_config,
                              "tenzir.enable-assert-secret-operator", false);
    if (not v) {
      return diagnostic::error(
               "`tenzir.enable-assert-secret-operator` must be a boolean")
        .to_error();
    }
    enabled_ = *v;
    return {};
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    if (not enabled_) {
      diagnostic::error("the `{}` operator is disabled ", name())
        .primary(inv.self.get_location())
        .emit(ctx);
      return failure::promise();
    }
    auto secret = located<class secret>{};
    auto expected = located<data>{};
    argument_parser2::operator_(name())
      .named("secret", secret)
      .named("expected", expected)
      .parse(inv, ctx)
      .ignore();
    return std::make_unique<assert_secret_operator>(std::move(secret),
                                                    std::move(expected));
  }

  auto describe() const -> Description override {
    auto d = Describer<AssertSecretArgs, AssertSecret>{};
    auto arg = d.named("secret", &AssertSecretArgs::secret);
    d.named("expected", &AssertSecretArgs::expected);
    d.validate(
      [enabled = enabled_, name = name(), arg](DescribeCtx& ctx) -> Empty {
        if (not enabled) {
          diagnostic::error("the `{}` operator is disabled ", name)
            .primary(*ctx.get_location(arg))
            .emit(ctx);
        }
        return {};
      });
    return d.without_optimize();
  }

private:
  bool enabled_ = false;
};

} // namespace

} // namespace tenzir::plugins::secret

TENZIR_REGISTER_PLUGIN(tenzir::plugins::secret::testing_operator_plugin)
