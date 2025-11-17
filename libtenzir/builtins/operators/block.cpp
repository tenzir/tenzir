//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::block {

namespace {

class block_operator final : public crtp_operator<block_operator> {
public:
  block_operator() = default;

  explicit block_operator(duration d) : duration_{d} {
  }

  template <operator_input_batch T>
  auto operator()(generator<T> input, operator_control_plane& ctrl) const
    -> generator<T> {
    co_yield {};
    ctrl.set_waiting(true);
    ctrl.self().run_delayed_weak(duration_, [&] {
      ctrl.set_waiting(false);
    });
    co_yield {};
    for (auto&& x : input) {
      co_yield std::move(x);
    }
  }

  auto name() const -> std::string override {
    return "_block";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    return optimize_result{filter, order, copy()};
  }

  friend auto inspect(auto& f, block_operator& x) -> bool {
    return f.apply(x.duration_);
  }

private:
  duration duration_{};
};

class plugin final : public virtual operator_plugin<block_operator>,
                     operator_factory_plugin {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto d = duration{};
    argument_parser2::operator_(name())
      .positional("duration", d)
      .parse(inv, ctx)
      .ignore();
    return std::make_unique<block_operator>(d);
  }
};

} // namespace

} // namespace tenzir::plugins::block

TENZIR_REGISTER_PLUGIN(tenzir::plugins::block::plugin)
