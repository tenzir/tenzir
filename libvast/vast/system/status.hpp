//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/defaults.hpp"
#include "vast/logger.hpp"
#include "vast/system/actors.hpp"

#include <caf/duration.hpp>
#include <caf/settings.hpp>
#include <caf/typed_response_promise.hpp>

namespace vast::system {

/// The verbosity level of a status request.
enum class status_verbosity { info, detailed, debug };

template <class Ptr, class Result>
struct status_request_state_base {
  // The copy constructor is intentionally deleted.
  status_request_state_base(const status_request_state_base&) = delete;
  status_request_state_base& operator=(const status_request_state_base&)
    = delete;

  // Moving is still allowed.
  status_request_state_base(status_request_state_base&&) = default;
  status_request_state_base& operator=(status_request_state_base&&) = default;

  ~status_request_state_base() = default;

  // The actor handling the original request.
  Ptr self;
  // Promise to the original request.
  caf::typed_response_promise<Result> promise;
  // Maps nodes to a map associating components with status information.
  caf::settings content;
};

struct no_extra {
  static void
  deliver(caf::typed_response_promise<caf::settings>&& rp, caf::settings&& s) {
    rp.deliver(std::move(s));
  }
};

template <class Ptr, class Result, class Extra>
struct status_request_state : status_request_state_base<Ptr, Result>, Extra {};

template <class Extra, class Result = caf::settings, class Ptr>
auto make_status_request_state(Ptr self) {
  using state_type = status_request_state<Ptr, Result, Extra>;
  // We need a custom deleter to deliver the promise, so we can't use
  // make_shared here.
  auto rs = std::shared_ptr<state_type>{
    new state_type{{self, self->template make_response_promise<Result>(), {}},
                   Extra{}},
    [](state_type* x) {
      x->Extra::deliver(std::move(x->promise), std::move(x->content));
      delete x; // NOLINT
    }};
  return rs;
}

template <class Ptr>
std::shared_ptr<status_request_state<Ptr, caf::settings, no_extra>>
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
template <class F, class Fe, class Ptr, class Result, class Extra, class Resp,
          class = std::enable_if_t<std::is_invocable_v<F, caf::settings&>>>
void collect_status(
  const std::shared_ptr<status_request_state<Ptr, Result, Extra>>& rs,
  std::chrono::milliseconds timeout, status_verbosity verbosity, Resp responder,
  F&& f, Fe&& fe) {
  // The overload for 'request(...)' taking a 'std::chrono::duration' does not
  // respect the specified message priority, so we convert to 'caf::duration' by
  // hand.
  rs->self
    ->template request<caf::message_priority::high>(
      responder, caf::duration{timeout}, atom::status_v, verbosity)
    .then(
      [rs, f = std::forward<F>(f)](caf::settings& response) {
        f(response);
      },
      [rs, fe = std::forward<Fe>(fe)](caf::error& err) {
        fe(err);
      });
}

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
/// @param s The settings object to insert the response into.
/// @param key The key at which the response shall be inserted.
template <class Ptr, class Result, class Extra, class Resp>
void collect_status(
  const std::shared_ptr<status_request_state<Ptr, Result, Extra>>& rs,
  std::chrono::milliseconds timeout, status_verbosity verbosity, Resp responder,
  caf::settings& s, std::string_view key) {
  collect_status(
    rs, timeout, verbosity, responder,
    [key, &s](caf::settings& response) {
      put(s, std::string_view{key}, std::move(response));
    },
    [self = rs->self, key, &s](const caf::error& err) {
      VAST_WARN("{} failed to retrieve status for the key {}: {}", self, key,
                err);
      put(s, std::string_view{key}, fmt::to_string(err));
    });
}

} // namespace vast::system
