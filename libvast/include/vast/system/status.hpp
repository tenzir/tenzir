//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/data.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/inspection_common.hpp"
#include "vast/logger.hpp"
#include "vast/system/actors.hpp"

#include <caf/settings.hpp>
#include <caf/timespan.hpp>
#include <caf/typed_response_promise.hpp>

namespace vast::system {

/// The verbosity level of a status request.
enum class status_verbosity { info, detailed, debug };

template <class Inspector>
auto inspect(Inspector& f, status_verbosity& x) {
  return detail::inspect_enum(f, x);
}

template <class Ptr, class Result>
struct status_request_state_base {
  // The regular constructor to be used with the `shared_ptr` from
  // `make_status_request_state`.
  status_request_state_base(Ptr self,
                            caf::typed_response_promise<Result> promise)
    : self{self}, promise{std::move(promise)}, content{} {
  }

  // The copy constructor is intentionally deleted.
  status_request_state_base(const status_request_state_base&) = delete;
  status_request_state_base& operator=(const status_request_state_base&)
    = delete;

  // Moving is still allowed.
  status_request_state_base(status_request_state_base&&) noexcept = default;
  status_request_state_base&
  operator=(status_request_state_base&&) noexcept = default;

  ~status_request_state_base() = default;

  // The actor handling the original request.
  Ptr self;
  // Promise to the original request.
  caf::typed_response_promise<Result> promise;
  // Maps nodes to a map associating components with status information.
  record content;
};

struct no_extra {
  static void deliver(caf::typed_response_promise<record>&& rp, record&& s) {
    rp.deliver(std::move(s));
  }
};

template <class Ptr, class Result, class Extra>
struct status_request_state : status_request_state_base<Ptr, Result>, Extra {};

template <class Extra, class Result = record, class Ptr>
auto make_status_request_state(Ptr self) {
  using state_type = status_request_state<Ptr, Result, Extra>;
  // We need a custom deleter to deliver the promise, so we can't use
  // make_shared here.
  auto rs = std::shared_ptr<state_type>{
    new state_type{{self, self->template make_response_promise<Result>()},
                   Extra{}},
    [](state_type* x) {
      x->Extra::deliver(std::move(x->promise), std::move(x->content));
      delete x; // NOLINT
    }};
  return rs;
}

template <class Ptr>
std::shared_ptr<status_request_state<Ptr, record, no_extra>>
make_status_request_state(Ptr self) {
  return make_status_request_state<no_extra>(self);
}

/// Requests a status response from another actor.
/// @tparam F The callback for a successful response.
/// @tparam Fe The callback for a failed request.
/// @tparam Ptr The type of the `self` pointer parameter.
/// @tparam Result The type that the promise delivers.
/// @tparam Extra User supplied extra fields for accumulation.
/// @tparam Resp The handle type for the responder actor.
/// @param rs A shared-pointer to the request state.
/// @param timeout The timeout for the request.
/// @param verbosity The requested verbosity level.
/// @param responder The actor to retrieve additional status from.
/// @param f The callback for a successful response.
/// @param fe The callback for a failed request.
// clang-format off
template <class F, class Fe, class Ptr, class Result, class Extra, class Resp>
requires(std::is_invocable_v<F, record&>)
void collect_status(
  const std::shared_ptr<status_request_state<Ptr, Result, Extra>>& rs,
  std::chrono::milliseconds timeout, status_verbosity verbosity, Resp responder,
  F&& f, Fe&& fe) {
  // The overload for 'request(...)' taking a 'std::chrono::duration' does not
  // respect the specified message priority, so we convert to 'caf::timespan' by
  // hand.
  rs->self
    ->template request<caf::message_priority::high>(
      responder, caf::timespan{timeout}, atom::status_v, verbosity)
    .then(
      [rs, f = std::forward<F>(f)](record& response) mutable {
        f(response);
      },
      [rs, fe = std::forward<Fe>(fe)](caf::error& err) mutable {
        fe(err);
      });
}
// clang-format on

/// Requests a status response from another actor. Convenience overload for
/// cases without extra state.
/// @tparam Ptr The type of the `self` pointer parameter.
/// @tparam Result The type that the promise delivers.
/// @tparam Extra User supplied extra fields for accumulation.
/// @tparam Resp The handle type for the responder actor.
/// @param rs A shared-pointer to the request state.
/// @param timeout The timeout for the request.
/// @param verbosity The requested verbosity level.
/// @param responder The actor to retrieve additional status from.
/// @param s The record to insert the response into.
/// @param key The key at which the response shall be inserted.
template <class Ptr, class Result, class Extra, class Resp>
void collect_status(
  const std::shared_ptr<status_request_state<Ptr, Result, Extra>>& rs,
  std::chrono::milliseconds timeout, status_verbosity verbosity, Resp responder,
  record& s, std::string_view key) {
  collect_status(
    rs, timeout, verbosity, responder,
    [key = std::string{key}, &s](record& response) {
      s[key] = std::move(response);
    },
    [self = rs->self, key = std::string{key}, &s](const caf::error& err) {
      VAST_WARN("{} failed to retrieve status for the key {}: {}", *self, key,
                err);
      s[key] = fmt::to_string(err);
    });
}

} // namespace vast::system
