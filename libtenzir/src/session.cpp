//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/session.hpp"

#include "tenzir/tql2/registry.hpp"

namespace tenzir {

auto session_provider::make(diagnostic_handler& dh) -> session_provider {
  return session_provider{dh};
}

auto session_provider::as_session() & -> session {
  return session{*this};
}

session_provider::diagnostic_ctx::diagnostic_ctx(diagnostic_handler& dh)
  : dh_{dh} {
}

void session_provider::diagnostic_ctx::emit(diagnostic d) {
  if (d.severity == severity::error) {
    failure_ = failure::promise();
  }
  dh_.emit(d);
}

session_provider::session_provider(diagnostic_handler& dh) : dh_{dh} {
}

session::session(session_provider& provider) : provider_{provider} {
}

auto session::get_failure() const -> std::optional<failure> {
  return provider_.dh_.failure_;
}

auto session::has_failure() const -> bool {
  return get_failure().has_value();
}

auto session::reg() -> const registry& {
  // TODO: The registry should be attached to a session instead.
  return global_registry();
}

auto session::dh() -> diagnostic_handler& {
  return provider_.dh_;
}

session::operator diagnostic_handler&() {
  return dh();
}

} // namespace tenzir
