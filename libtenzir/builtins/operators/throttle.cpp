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

namespace {

using float_seconds = std::chrono::duration<double>;

auto split_chunk(const chunk_ptr& in,
                 size_t position) -> std::pair<chunk_ptr, chunk_ptr> {
  if (position >= in->size()) {
    return {in, chunk::make_empty()};
  }
  return {in->slice(0, position), in->slice(position)};
}

} // namespace

class throttle_operator final : public crtp_operator<throttle_operator> {
public:
  throttle_operator() = default;

  explicit throttle_operator(double max_bandwidth, float_seconds window)
    : bandwidth_per_second_(max_bandwidth / window.count()), window_(window) {
  }

  // TODO: Currently the operator only handles byte stream, but in the future
  // we also want to be able to handle events as input.
  auto operator()(generator<chunk_ptr> input,
                  operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    auto alarm_clock = ctrl.self().spawn(detail::make_alarm_clock);
    auto last_timestamp = std::chrono::system_clock::now();
    auto bytes_per_window = bandwidth_per_second_ * window_.count();
    if (bytes_per_window == size_t{0}) {
      ++bytes_per_window; // Enforce at least some progress every window.
    }
    double budget = 0.0;
    for (auto&& bytes : input) {
      if (not bytes) {
        co_yield {};
        continue;
      }
      auto now = std::chrono::system_clock::now();
      auto additional_budget
        = duration_cast<float_seconds>(now - last_timestamp).count()
          * bandwidth_per_second_;
      budget = std::min(bytes_per_window, budget + additional_budget);
      auto split_position = static_cast<size_t>(budget);
      auto head = chunk::make_empty();
      auto tail = chunk::make_empty();
      std::tie(head, tail) = split_chunk(bytes, split_position);
      budget -= static_cast<double>(head->size());
      co_yield std::move(head);
      // If we didn't have enough budget to send everything in one go,
      // send the remainder in intervals according to the configured
      // window.
      while (tail->size() > 0) {
        budget = 0;
        ctrl.set_waiting(true);
        ctrl.self()
          .request(alarm_clock, caf::infinite,
                   duration_cast<caf::timespan>(window_))
          .await(
            [&]() {
              ctrl.set_waiting(false);
            },
            [&ctrl](const caf::error& err) {
              diagnostic::error("throttle operator failed to delay")
                .note("encountered error: {}", err)
                .emit(ctrl.diagnostics());
            });
        std::tie(head, tail)
          = split_chunk(tail, static_cast<size_t>(bytes_per_window));
        co_yield {}; // Await the alarm clock.
        co_yield std::move(head);
      }
      last_timestamp = std::chrono::system_clock::now();
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
    return f.apply(x.bandwidth_per_second_);
  }

private:
  double bandwidth_per_second_ = 0.0; // bytes
  float_seconds window_ = {};
};

class throttle_plugin final : virtual public operator_plugin<throttle_operator>,
                              operator_factory_plugin {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto const* docs = "https://docs.tenzir.com/operators/throttle";
    auto parser = argument_parser{"throttle", docs};
    auto max_bandwidth = std::optional<uint64_t>{};
    // TODO: Add option to set window size.
    auto window = float_seconds{1};
    parser.add(max_bandwidth, "<max_bandwidth>");
    parser.parse(p);
    if (not max_bandwidth) {
      diagnostic::error("`max_bandwidth` must be a numeric value")
        .note("the unit of measurement is bytes/second")
        .throw_();
    }
    return std::make_unique<throttle_operator>(*max_bandwidth, window);
  }

  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto max_bandwidth = std::optional<uint64_t>{};
    argument_parser2::operator_("throttle")
      .add("max_bandwith", max_bandwidth)
      .parse(inv, ctx)
      .ignore();
    if (not max_bandwidth) {
      diagnostic::error("`max_bandwidth` must be a numeric value")
        .note("the unit of measurement is bytes/second")
        .emit(ctx);
      return failure::promise();
    }
    // TODO: Add option to set window size.
    auto window = float_seconds{1};
    return std::make_unique<throttle_operator>(*max_bandwidth, window);
  }
};

} // namespace tenzir::plugins::throttle

TENZIR_REGISTER_PLUGIN(tenzir::plugins::throttle::throttle_plugin)
