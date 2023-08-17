//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/data.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/plugin.hpp>

#include <fluent-bit/fluent-bit-minimal.h>

using namespace std::chrono_literals;

namespace tenzir::plugins::fluentbit {

namespace {

struct operator_args {
  located<std::vector<std::string>> args;

  template <class Inspector>
  friend auto inspect(Inspector& f, operator_args& x) -> bool {
    return f.object(x)
      .pretty_name("operator_args")
      .fields(f.field("args", x.args));
  }
};

class fluent_bit_operator final : public crtp_operator<fluent_bit_operator> {
public:
  fluent_bit_operator() = default;

  explicit fluent_bit_operator(operator_args args) : args_{std::move(args)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto ctx = flb_create();
    auto destroyer = caf::detail::make_scope_guard([ctx] {
      flb_destroy(ctx);
    });
    if (ctx == nullptr)
      diagnostic::error("failed to create fluent bit context")
        .hint("flb_create returned nullptr")
        .emit(ctrl.diagnostics());
    // Set services properties.
    auto result = flb_service_set(ctx, "flush", "1", "grace", "1", nullptr);
    if (result != 0)
      diagnostic::error("failed to set flush interval")
        .hint("flb_service_set returned {}", result)
        .emit(ctrl.diagnostics());
    // FIXME: debugging only
    // Enable input plugin.
    auto in_ffd = flb_input(ctx, "random", nullptr);
    if (in_ffd < 0)
      diagnostic::error("failed to create input plugin descriptor")
        .hint("flb_input returned {}", in_ffd)
        .emit(ctrl.diagnostics());
    // Set input properties.
    result = flb_input_set(ctx, in_ffd, "tag", "tenzir", nullptr);
    if (result != 0)
      diagnostic::error("failed to set input properties")
        .hint("flb_input_set returned {}", result)
        .emit(ctrl.diagnostics());
    // FIXME: debugging only
    // Enable output plugin.
    auto out_ffd = flb_output(ctx, "stdout", nullptr);
    if (out_ffd < 0)
      diagnostic::error("failed to create output plugin descriptor")
        .hint("flb_output returned {}", in_ffd)
        .emit(ctrl.diagnostics());
    result = flb_output_set(ctx, out_ffd, "match", "tenzir", nullptr);
    if (result != 0)
      diagnostic::error("failed to set input properties")
        .hint("flb_output_set returned {}", result)
        .emit(ctrl.diagnostics());
    // Start the engine.
    result = flb_start(ctx);
    if (result != 0)
      diagnostic::error("failed to start fluent bit engine")
        .hint("flb_start returned {}", result)
        .emit(ctrl.diagnostics());
    // Stop the engine.
    auto stopper = caf::detail::make_scope_guard([ctx] {
      auto result = flb_stop(ctx);
      if (result != 0)
        TENZIR_WARN("flb_stop returned {}", result);
    });
    // FIXME: keep us alive
    while (true) {
      co_yield {};
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }

  auto name() const -> std::string override {
    return "fluentbit";
  }

  auto detached() const -> bool override {
    return false;
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

  friend auto inspect(auto& f, fluent_bit_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  operator_args args_;
};

class plugin final : public operator_plugin<fluent_bit_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{
      name(),
      fmt::format("https://docs.tenzir.com/docs/connectors/{}", name())};
    auto args = operator_args{};
    parser.parse(p);
    return std::make_unique<fluent_bit_operator>(std::move(args));
  }

  auto name() const -> std::string override {
    return "fluentbit";
  }
};

} // namespace

} // namespace tenzir::plugins::fluentbit

TENZIR_REGISTER_PLUGIN(tenzir::plugins::fluentbit::plugin)
