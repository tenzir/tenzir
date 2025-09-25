//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
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

auto split_chunk(const chunk_ptr& in, size_t head_offset, size_t position)
  -> std::pair<chunk_ptr, chunk_ptr> {
  if (head_offset >= in->size()) {
    return {chunk::make_empty(), chunk::make_empty()};
  }
  if (head_offset + position >= in->size()) {
    return {in->slice(head_offset, position), chunk::make_empty()};
  }
  return {in->slice(head_offset, position), in->slice(head_offset + position)};
}

} // namespace

class throttle_operator final : public crtp_operator<throttle_operator> {
public:
  throttle_operator() = default;

  explicit throttle_operator(uint64_t bandwidth, duration window)
    : bandwidth_(bandwidth), window_(window) {
  }

  // TODO: Currently the operator only handles byte stream, but in the future
  // we also want to be able to handle events as input.
  auto operator()(generator<chunk_ptr> input,
                  operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    auto bytes_in_current_window = uint64_t{0};
    auto window_start = std::chrono::steady_clock::now();
    for (auto&& bytes : input) {
      if (not bytes) {
        co_yield {};
        continue;
      }
      // Process the chunk, potentially splitting it if needed
      auto current_chunk = bytes;
      while (current_chunk && current_chunk->size() > 0) {
        // Check if we need to reset the window
        auto now = std::chrono::steady_clock::now();
        auto elapsed = now - window_start;
        if (elapsed >= window_) {
          // Reset the window
          window_start = now;
          bytes_in_current_window = 0;
        }
        // Calculate how many bytes we can send in the current window
        auto remaining_allowance = (bandwidth_ > bytes_in_current_window)
                                     ? bandwidth_ - bytes_in_current_window
                                     : uint64_t{0};
        if (remaining_allowance == 0) {
          // Need to wait until the next window
          auto wake_time = window_start + window_;
          ctrl.self().run_scheduled_weak(wake_time, [&] {
            ctrl.set_waiting(false);
          });
          ctrl.set_waiting(true);
          co_yield {};
          // After waiting, reset the window
          window_start = std::chrono::steady_clock::now();
          bytes_in_current_window = 0;
          remaining_allowance = bandwidth_;
        }
        // Split the chunk if necessary
        auto [to_send, rest]
          = split_chunk(current_chunk, 0, remaining_allowance);
        if (to_send and to_send->size() > 0) {
          // Send what we can
          bytes_in_current_window += to_send->size();
          co_yield std::move(to_send);
        }
        // Update current_chunk to the remainder
        current_chunk = std::move(rest);
      }
    }
  }

  auto name() const -> std::string override {
    return "throttle";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter, (void)order;
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, throttle_operator& x) -> bool {
    return f.object(x).fields(f.field("bandwidth", x.bandwidth_),
                              f.field("window", x.window_));
  }

private:
  uint64_t bandwidth_ = 0; // bytes per window
  duration window_ = {};
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
    auto bandwidth = located<uint64_t>{};
    auto window = std::optional<located<duration>>{};
    parser.add(bandwidth, "<bandwidth>");
    parser.add("--within", window, "<duration>");
    parser.parse(p);
    if (bandwidth.inner == 0) {
      diagnostic::error("`bandwidth` must be a positive number")
        .primary(bandwidth.source)
        .throw_();
    }
    if (window and window->inner <= duration::zero()) {
      diagnostic::error("duration must be greater than zero")
        .primary(window->source)
        .throw_();
    }
    return std::make_unique<throttle_operator>(
      bandwidth.inner,
      window ? window->inner : duration{std::chrono::seconds{1}});
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto bandwidth = located<uint64_t>{};
    auto window = std::optional<located<duration>>{};
    argument_parser2::operator_("throttle")
      .positional("bandwidth", bandwidth)
      .named("within", window)
      .parse(inv, ctx)
      .ignore();
    if (bandwidth.inner == 0) {
      diagnostic::error("`bandwidth` must be a positive value")
        .primary(bandwidth.source)
        .emit(ctx);
    }
    if (window and window->inner <= duration::zero()) {
      diagnostic::error("duration must be greater than zero")
        .primary(window->source)
        .emit(ctx);
    }
    return std::make_unique<throttle_operator>(
      bandwidth.inner,
      window ? window->inner : duration{std::chrono::seconds{1}});
  }
};

} // namespace tenzir::plugins::throttle

TENZIR_REGISTER_PLUGIN(tenzir::plugins::throttle::throttle_plugin)
