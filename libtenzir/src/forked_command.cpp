//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/forked_command.hpp"

#include "tenzir/fwd.hpp"

#include "tenzir/application.hpp"
#include "tenzir/atoms.hpp"
#include "tenzir/command.hpp"
#include "tenzir/config.hpp"
#include "tenzir/endpoint.hpp"
#include "tenzir/logger.hpp"

#include <caf/actor_system_config.hpp>
#include <caf/io/middleman.hpp>
#include <caf/openssl/all.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <csignal>

namespace tenzir {

auto forked_command(const invocation& inv, caf::actor_system& sys)
  -> caf::message {
  TENZIR_TRACE_SCOPE("{} {}", TENZIR_ARG(inv.options),
                     TENZIR_ARG("args", inv.arguments.begin(),
                                inv.arguments.end()));
  // Get a convenient and blocking way to interact with actors.
  caf::scoped_actor self{sys};
  auto& mm = sys.middleman();
  auto bound_port = mm.open(0);
  if (!bound_port)
    return caf::make_message(std::move(bound_port.error()));
  std::cout << *bound_port << std::endl;
  TENZIR_INFO("listening on {}", *bound_port);
  auto stop = false;
  self
    ->do_receive([&](atom::signal, int signal) {
      TENZIR_DEBUG("{} got {}", *self, ::strsignal(signal));
      stop = true;
    })
    .until([&] {
      return stop;
    });
  return caf::make_message(caf::none);
}

} // namespace tenzir
