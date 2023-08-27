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

/// A RAII-style wrapper around the Fluent Bit engine.
class engine {
public:
  static auto make() -> std::optional<engine> {
    auto* ctx = flb_create();
    if (ctx == nullptr)
      return std::nullopt;
    return engine{ctx};
  }

  ~engine() {
    stop();
    TENZIR_ASSERT(ctx_ != nullptr);
    // FIXME: destroying the library context currently yields a segfault.
    // flb_destroy(ctx_);
  }

  // The engine is a move-only handle type.
  engine(engine&&) = default;
  auto operator=(engine&&) -> engine& = default;
  engine(const engine&) = delete;
  auto operator=(const engine&) -> engine& = delete;

  auto start() -> bool {
    if (running_)
      return true;
    auto success = flb_start(ctx_) == 0;
    if (success)
      running_ = true;
    return success;
  }

  auto stop() -> bool {
    if (not running_)
      return false;
    return flb_stop(ctx_) == 0;
  }

  auto context() -> flb_ctx_t* {
    return ctx_;
  }

private:
  explicit engine(flb_ctx_t* ctx) : ctx_{ctx} {
    TENZIR_ASSERT(ctx != nullptr);
  }

  flb_ctx_t* ctx_{nullptr};
  bool running_{false};
};

class fluent_bit_operator final : public crtp_operator<fluent_bit_operator> {
public:
  fluent_bit_operator() = default;

  explicit fluent_bit_operator(operator_args args) : args_{std::move(args)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto engine = engine::make();
    if (not engine) {
      diagnostic::error("failed to create fluent bit context")
        .hint("flb_create returned nullptr")
        .emit(ctrl.diagnostics());
      co_return;
    }
    // Set services properties.
    auto result
      = flb_service_set(engine->context(), "flush", "1", "grace", "1", nullptr);
    if (result != 0) {
      diagnostic::error("failed to set flush interval")
        .hint("flb_service_set returned {}", result)
        .emit(ctrl.diagnostics());
      co_return;
    }
    // Enable an input plugin.
    auto in_ffd = flb_input(engine->context(), "random", nullptr);
    if (in_ffd < 0) {
      diagnostic::error("failed to create input plugin descriptor")
        .hint("flb_input returned {}", in_ffd)
        .emit(ctrl.diagnostics());
      co_return;
    }
    result = flb_input_set(engine->context(), in_ffd, "tag", "tenzir", nullptr);
    if (result != 0) {
      diagnostic::error("failed to set input properties")
        .hint("flb_input_set returned {}", result)
        .emit(ctrl.diagnostics());
      co_return;
    }
    // Enable an output plugin.
    auto out_ffd = flb_output(engine->context(), "stdout", nullptr);
    if (out_ffd < 0) {
      diagnostic::error("failed to create output plugin descriptor")
        .hint("flb_output returned {}", in_ffd)
        .emit(ctrl.diagnostics());
      co_return;
    }
    result
      = flb_output_set(engine->context(), out_ffd, "match", "tenzir", nullptr);
    if (result != 0) {
      diagnostic::error("failed to set input properties")
        .hint("flb_output_set returned {}", result)
        .emit(ctrl.diagnostics());
      co_return;
    }
    // Start the engine.
    if (not engine->start()) {
      diagnostic::error("failed to start fluent bit engine")
        .hint("flb_start", result)
        .emit(ctrl.diagnostics());
      co_return;
    }
    // Keep the executor pumping.
    while (true) {
      co_yield {};
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    // TODO: implement
    for (auto&& slice : input) {
      (void)slice;
      co_yield {};
    }
  }

  auto name() const -> std::string override {
    return "fluentbit";
  }

  auto detached() const -> bool override {
    // Fluent Bit comes with its own threaded context, no need to add another
    // thread on our end.
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
