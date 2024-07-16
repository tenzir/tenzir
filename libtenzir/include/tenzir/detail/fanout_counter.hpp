//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/response_promise.hpp>
#include <caf/typed_response_promise.hpp>

namespace tenzir::detail {

struct fanout_empty_state {};

/// A counter that can be used to keep track of N fan-out requests
/// and trigger a continuation after all of them have returned.
/// If any of the requests returned an error, the error continuation
/// will be triggered instead.
/// Can optionally take a state that can be shared between all of
/// the individual requests.
/// The success continuation has the signature `void()` and the error
/// continuation has the signature `void(caf::error&&)`, or `void(State&&)`
/// and `void(State&&, caf::error&&)` if a state is used.
/// It is assumed that all calls to `fanout_counter` will come from
/// the same actor context, so no attempt at synchronization is made.
template <typename State, typename SuccessContinuation,
          typename ErrorContinuation>
  requires(std::is_default_constructible_v<State>)
struct fanout_counter {
public:
  fanout_counter(size_t expected, SuccessContinuation then,
                 ErrorContinuation error)
    : success_count(0),
      error_count(0),
      expected(expected),
      last_error(caf::none),
      state_(State{}),
      then(then),
      error(error) {
  }

  void receive_success() {
    ++success_count;
    if (success_count + error_count == expected)
      finish();
  }

  void receive_error(caf::error error) {
    ++error_count;
    last_error = std::move(error);
    if (success_count + error_count == expected)
      finish();
  }

  State& state() {
    return state_;
  }

private:
  void finish() {
    if constexpr (std::is_same_v<State, fanout_empty_state>) {
      if (error_count > 0)
        error(std::move(last_error));
      else
        then();
    } else {
      if (error_count > 0)
        error(std::move(state_), std::move(last_error));
      else
        then(std::move(state_));
    }
  }

  size_t success_count;
  size_t error_count;
  size_t expected;
  caf::error last_error;
  State state_;
  SuccessContinuation then;
  ErrorContinuation error;
};

template <typename Continuation, typename ErrorContinuation>
auto make_fanout_counter(size_t expected, Continuation&& then,
                         ErrorContinuation&& error) {
  using counter
    = fanout_counter<fanout_empty_state, Continuation, ErrorContinuation>;
  return std::make_shared<counter>(expected, std::forward<Continuation>(then),
                                   std::forward<ErrorContinuation>(error));
}

template <typename State, typename Continuation, typename ErrorContinuation>
auto make_fanout_counter(size_t expected, Continuation&& then,
                         ErrorContinuation&& error) {
  using counter = fanout_counter<State, Continuation, ErrorContinuation>;
  return std::make_shared<counter>(expected, std::forward<Continuation>(then),
                                   std::forward<ErrorContinuation>(error));
}

// template <typename State>
// auto make_fanout_counter(size_t expected, caf::typed_response_promise<State>
// rp) {
//   return make_fanout_counter<State>(expected,
//     [rp](State&& state) mutable { rp.deliver(std::move(state)); },
//     [rp](State&&, caf::error&& e) mutable { rp.deliver(e); });
// }

inline auto
make_fanout_counter(size_t expected, caf::typed_response_promise<void> rp) {
  return make_fanout_counter(
    expected,
    [rp]() mutable {
      rp.deliver();
    },
    [rp](caf::error&& e) mutable {
      rp.deliver(e);
    });
}

} // namespace tenzir::detail
