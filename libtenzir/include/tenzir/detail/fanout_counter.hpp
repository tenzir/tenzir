//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/assert.hpp"

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
/// If the state is empty, the success continuation has the form `void()`, while
/// the error continuation is `void(Error&&)` or `void(span<Error>)`. If the
/// state is not empty, the success continuation has the form `void(State&&)`,
/// while the error continuation is `void(State&&,Error&&)` or `void(State&&,
/// span<Error>)`.
///
/// It is assumed that all calls to `fanout_counter` will come from the same
/// actor context, so no attempt at synchronization is made.
template <typename State, typename Error, typename SuccessContinuation,
          typename ErrorContinuation>
  requires std::is_default_constructible_v<State>
struct fanout_counter {
  constexpr static bool single_error_continuation
    = std::same_as<State, fanout_empty_state>
        ? std::is_invocable_v<ErrorContinuation, Error&&>
        : std::is_invocable_v<ErrorContinuation, State&&, Error&&>;

public:
  fanout_counter(size_t expected, SuccessContinuation then,
                 ErrorContinuation error)
    : success_count(0),
      error_count(0),
      expected(expected),
      state_(State{}),
      then(then),
      error(error) {
  }

  void receive_success() {
    ++success_count;
    if (success_count + error_count == expected)
      finish();
  }

  void receive_error(Error error) {
    ++error_count;
    if (not single_error_continuation or errors.empty()) {
      errors.push_back(std::move(error));
    } else if constexpr (single_error_continuation) {
      last_error() = std::move(error);
    }
    if (success_count + error_count == expected)
      finish();
  }

  State& state() {
    return state_;
  }

private:
  auto& last_error() {
    TENZIR_ASSERT(error_count > 0);
    return errors.back();
  }
  void finish() {
    if constexpr (std::is_same_v<State, fanout_empty_state>) {
      if (error_count > 0) {
        if constexpr (single_error_continuation) {
          error(std::move(last_error()));
        } else {
          error(errors);
        }
      }
      then();
    } else {
      if (error_count > 0) {
        if constexpr (single_error_continuation) {
          error(std::move(state_), std::move(last_error()));
        } else {
          error(std::move(state_), errors);
        }
      }
      then(std::move(state_));
    }
  }

  size_t success_count;
  size_t error_count;
  size_t expected;
  std::vector<Error> errors;
  State state_;
  SuccessContinuation then;
  ErrorContinuation error;
};

template <typename Continuation, typename ErrorContinuation>
auto make_fanout_counter(size_t expected, Continuation&& then,
                         ErrorContinuation&& error) {
  using counter = fanout_counter<fanout_empty_state, caf::error, Continuation,
                                 ErrorContinuation>;
  return std::make_shared<counter>(expected, std::forward<Continuation>(then),
                                   std::forward<ErrorContinuation>(error));
}

template <typename State, typename Continuation, typename ErrorContinuation>
auto make_fanout_counter(size_t expected, Continuation&& then,
                         ErrorContinuation&& error) {
  using counter
    = fanout_counter<State, caf::error, Continuation, ErrorContinuation>;
  return std::make_shared<counter>(expected, std::forward<Continuation>(then),
                                   std::forward<ErrorContinuation>(error));
}

template <typename Error, typename Continuation, typename ErrorContinuation>
auto make_fanout_counter_with_error(size_t expected, Continuation&& then,
                                    ErrorContinuation&& error) {
  using counter
    = fanout_counter<fanout_empty_state, Error, Continuation, ErrorContinuation>;
  return std::make_shared<counter>(expected, std::forward<Continuation>(then),
                                   std::forward<ErrorContinuation>(error));
}

template <typename State, typename Error, typename Continuation,
          typename ErrorContinuation>
auto make_fanout_counter_with_error(size_t expected, Continuation&& then,
                                    ErrorContinuation&& error) {
  using counter = fanout_counter<State, Error, Continuation, ErrorContinuation>;
  return std::make_shared<counter>(expected, std::forward<Continuation>(then),
                                   std::forward<ErrorContinuation>(error));
}

inline auto make_fanout_counter(size_t expected,
                                const caf::typed_response_promise<void>& rp) {
  return make_fanout_counter(
    expected,
    [rp = rp]() mutable {
      rp.deliver();
    },
    [rp = rp](caf::error e) mutable {
      rp.deliver(std::move(e));
    });
}

} // namespace tenzir::detail
