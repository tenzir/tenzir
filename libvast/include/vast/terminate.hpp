//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/atoms.hpp"
#include "vast/defaults.hpp"
#include "vast/logger.hpp"
#include "vast/terminator.hpp"

#include <caf/actor_cast.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/fwd.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/stateful_actor.hpp>

#include <vector>

namespace vast::policy {

struct sequential;
struct parallel;

} // namespace vast::policy

namespace vast {

/// Wrapper which extends terminator's lifetime to the response handlers.
template <class ResponseHandle>
class terminate_result {
public:
  terminate_result(terminator_actor terminator, ResponseHandle response)
    : terminator_{std::move(terminator)}, response_{std::move(response)} {
  }

  template <class ResponseHandler, class ErrorHandler>
  decltype(auto)
  then(ResponseHandler responseHandler, ErrorHandler errorHandler) {
    return response_.then(
      [f = std::move(responseHandler), t = terminator_](atom::done) mutable {
        std::move(f)(atom::done_v);
      },
      [f = std::move(errorHandler),
       t = terminator_](const caf::error& e) mutable {
        f(e);
      });
  }

  template <class ResponseHandler, class ErrorHandler>
  decltype(auto)
  receive(ResponseHandler responseHandler, ErrorHandler errorHandler) {
    return response_.receive(
      [f = std::move(responseHandler), t = terminator_](atom::done) mutable {
        f(atom::done_v);
      },
      [f = std::move(errorHandler),
       t = terminator_](const caf::error& e) mutable {
        f(e);
      });
  }

private:
  terminator_actor terminator_;
  ResponseHandle response_;
};

/// Performs an asynchronous shutdown of a set of actors by sending an EXIT
/// message, configurable either in sequential or parallel mode of operation.
/// As soon as all actors have terminated, the returned promise gets fulfilled.
/// This function is the lower-level interface for bringing down actors. The
/// function `shutdown` uses this function internally to implement a more
/// convenient one-stop solution.
/// @param self The actor to terminate.
/// @param xs The actors to terminate.
/// @returns A response handle that is replied to when the termination succeeded
/// or failed.
/// @relates shutdown
template <class Policy, class Actor>
[[nodiscard]] auto terminate(Actor&& self, std::vector<caf::actor> xs) {
  auto t = self->spawn(terminator<Policy>);
  return terminate_result{t, self->request(t, caf::infinite, atom::shutdown_v,
                                           std::move(xs))};
}

template <class Policy, class Actor>
[[nodiscard]] auto terminate(Actor&& self, caf::actor x) {
  return terminate<Policy>(std::forward<Actor>(self),
                           std::vector{std::move(x)});
}

} // namespace vast
