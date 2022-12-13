//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/signal_reflector.hpp"

#include <caf/blocking_actor.hpp>

#include <csignal>
#include <unistd.h>

namespace vast::system {

namespace {

void signal_listener(caf::blocking_actor* self,
                     const signal_reflector_actor& reflector, sigset_t sigset) {
  int signum = 0;
  sigwait(&sigset, &signum);
  self->send<caf::message_priority::high>(reflector, atom::internal_v,
                                          atom::signal_v, signum);
}

} // namespace

sigset_t termsigset() {
  sigset_t sigset;
  sigemptyset(&sigset);
  sigaddset(&sigset, SIGINT);
  sigaddset(&sigset, SIGTERM);
  return sigset;
}

signal_reflector_actor::behavior_type signal_reflector(
  signal_reflector_actor::stateful_pointer<signal_reflector_state> self,
  sigset_t sigset) {
  self->set_exit_handler([=](const caf::exit_msg&) {
    // If we haven't got a signal yet we need to raise one to unblock the signal
    // listener.
    if (!self->state.got_signal)
      kill(getpid(), SIGTERM);
    return self->quit();
  });
  self->spawn(signal_listener, self, sigset);
  return {
    [self](atom::internal, atom::signal, int signum) {
      self->state.got_signal = true;
      // Unblock the termination signals so a second signal leads to immediate
      // termination.
      auto sigset = termsigset();
      pthread_sigmask(SIG_UNBLOCK, &sigset, nullptr);
      // If no actor registered itself we emulate the default behavior.
      if (!self->state.handler) {
        if (raise(signum) != 0) {
          // We don't want a backtrace when we get here.
          std::signal(SIGABRT, SIG_DFL); // NOLINT
          std::terminate();
        }
        __builtin_unreachable();
      }
      std::cerr << "\rinitiating graceful shutdown... (repeat request to "
                   "terminate immediately)\n";
      self->send(self->state.handler, atom::signal_v, signum);
    },
    [self](atom::subscribe) {
      self->state.handler
        = caf::actor_cast<termination_handler_actor>(self->current_sender());
    },
  };
}

} // namespace vast::system
