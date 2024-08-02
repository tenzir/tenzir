//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/detail/alarm_clock.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql/fwd.hpp>
#include <tenzir/tql/parser.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <algorithm>
#include <ranges>

namespace tenzir::plugins::throttle {

class throttle_operator final : public crtp_operator<throttle_operator> {
public:
  throttle_operator() = default;

  explicit throttle_operator(double max_bandwidth)
    : max_bandwidth_(max_bandwidth) {
  }

  auto operator()(generator<chunk_ptr> input,
                  operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    using float_seconds = std::chrono::duration<double>;
    auto alarm_clock = ctrl.self().spawn(detail::make_alarm_clock);
    auto last = std::chrono::system_clock::now();
    double budget = 0.0; // measured in bits
    for (auto&& bytes : input) {
      if (not bytes) {
        co_yield {};
        continue;
      }
      auto now = std::chrono::system_clock::now();
      auto incoming_bits = 8 * static_cast<double>(bytes->size());
      auto additional_budget
        = duration_cast<float_seconds>(now - last).count() * max_bandwidth_;
      budget = std::min(max_bandwidth_, budget + additional_budget);
      last = now;
      if (budget < incoming_bits) {
        auto delay = float_seconds{(incoming_bits - budget) / max_bandwidth_};
        ctrl.self()
          .request(alarm_clock, caf::infinite,
                   duration_cast<caf::timespan>(delay))
          .await(
            [&]() {
              // nop
            },
            [&ctrl](const caf::error& err) {
              diagnostic::error("throttle operator failed to delay")
                .note("encountered error: {}", err)
                .emit(ctrl.diagnostics());
            });
      }
      budget -= incoming_bits;
      co_yield std::move(bytes);
    }
    co_return;
  }

  auto name() const -> std::string override {
    return "throttle";
  }

  auto optimize(expression const& filter,
                event_order order) const -> optimize_result override {
    (void)filter, (void)order;
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, throttle_operator& x) -> bool {
    return f.apply(x.max_bandwidth_);
  }

private:
  double max_bandwidth_ = 0.0; // bits/s
};

class throttle_plugin final
  : virtual public operator_plugin<throttle_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto const* docs = "https://docs.tenzir.com/operators/throttle";
    auto parser = argument_parser{"throttle", docs};
    auto max_bandwidth = std::optional<uint64_t>{};
    parser.add(max_bandwidth, "<max_bandwidth>");
    parser.parse(p);
    if (not max_bandwidth) {
      diagnostic::error("`max_bandwidth` must be a numeric value")
        .note("the unit of measurement is bits/second", name())
        .throw_();
    }
    return std::make_unique<throttle_operator>(*max_bandwidth);
  }
};

} // namespace tenzir::plugins::throttle

TENZIR_REGISTER_PLUGIN(tenzir::plugins::throttle::throttle_plugin)
