/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/system/shutdown.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/die.hpp"
#include "vast/fwd.hpp"
#include "vast/logger.hpp"
#include "vast/system/terminate.hpp"
#include "vast/system/terminator.hpp"

#include <caf/response_promise.hpp>

namespace vast::system {

template <class Policy>
void shutdown(caf::event_based_actor* self, std::vector<caf::actor> xs,
              std::chrono::seconds clean_exit_timeout,
              std::chrono::seconds kill_exit_timeout) {
  // Ignore duplicate EXIT messages except for hard kills.
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    if (msg.reason == caf::exit_reason::kill) {
      VAST_WARNING(self, "received hard kill and terminates immediately");
      self->quit(msg.reason);
    } else {
      VAST_DEBUG(self, "ignores duplicate EXIT message from", msg.source);
    }
  });
  // Terminate actors as requested.
  terminate<Policy>(self, std::move(xs), clean_exit_timeout + kill_exit_timeout)
    .then(
      [=](atom::done) {
        VAST_DEBUG(self, "terminates after shutting down all dependents");
        self->quit(caf::exit_reason::user_shutdown);
      },
      [=](const caf::error& err) {
        VAST_ERROR(self, "failed to cleanly terminate dependent actors", err);
        die("failed to terminate dependent actors in given time window");
      });
}

template void
shutdown<policy::sequential>(caf::event_based_actor*, std::vector<caf::actor>,
                             std::chrono::seconds, std::chrono::seconds);

template void
shutdown<policy::parallel>(caf::event_based_actor*, std::vector<caf::actor>,
                           std::chrono::seconds, std::chrono::seconds);

template <class Policy>
void shutdown(caf::scoped_actor& self, std::vector<caf::actor> xs,
              std::chrono::seconds clean_exit_timeout,
              std::chrono::seconds kill_exit_timeout) {
  auto t
    = self->spawn(terminator<Policy>, clean_exit_timeout, kill_exit_timeout);
  auto shutdown_timeout = clean_exit_timeout + kill_exit_timeout;
  self->request(std::move(t), shutdown_timeout, std::move(xs))
    .receive(
      [&](atom::done) {
        VAST_DEBUG(self, "terminates after shutting down all dependents");
      },
      [&](const caf::error& err) {
        VAST_ERROR_ANON("failed to terminated all dependent actors", err);
        die("failed to terminate dependent actors in given time window");
      });
}

template void
shutdown<policy::sequential>(caf::scoped_actor&, std::vector<caf::actor>,
                             std::chrono::seconds, std::chrono::seconds);

template void
shutdown<policy::parallel>(caf::scoped_actor&, std::vector<caf::actor>,
                           std::chrono::seconds, std::chrono::seconds);

} // namespace vast::system
