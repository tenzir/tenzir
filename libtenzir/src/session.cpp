//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/session.hpp"

#include "tenzir/tql2/registry.hpp"

namespace tenzir {

auto session_provider::make(diagnostic_handler& dh, registry const& reg)
  -> session_provider {
  return session_provider{dh, reg};
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

session_provider::session_provider(diagnostic_handler& dh, registry const& reg)
  : dh_{dh}, reg_{reg} {
}

session::session(session_provider& provider) : provider_{provider} {
}

auto session::get_failure() const -> Option<failure> {
  return provider_.dh_.failure_;
}

auto session::has_failure() const -> bool {
  return get_failure().has_value();
}

auto session::reg() -> const registry& {
  return provider_.reg_.get();
}

auto session::dh() -> diagnostic_handler& {
  return provider_.dh_;
}

session::operator diagnostic_handler&() {
  return dh();
}

session::operator registry const&() {
  return reg();
}

} // namespace tenzir
