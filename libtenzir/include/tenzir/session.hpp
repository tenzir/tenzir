//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/diagnostics.hpp"
#include "tenzir/option.hpp"
#include "tenzir/ref.hpp"

namespace tenzir {

class registry;

class session_provider {
public:
  /// Makes a provider for sessions that resolve against `reg`, which must
  /// outlive the provider.
  static auto make(diagnostic_handler& dh, registry const& reg)
    -> session_provider;

  auto as_session() & -> session;
  auto as_session() && -> session = delete;

private:
  friend class session;

  class diagnostic_ctx final : public diagnostic_handler {
  public:
    explicit diagnostic_ctx(diagnostic_handler& dh);

    void emit(diagnostic d) override;

    Option<failure> failure_;
    diagnostic_handler& dh_;
  };

  session_provider(diagnostic_handler& dh, registry const& reg);

  diagnostic_ctx dh_;
  Ref<registry const> reg_;
};

/// This is meant to be used as a value type.
class session {
public:
  explicit session(session_provider& provider);

  auto get_failure() const -> Option<failure>;

  auto has_failure() const -> bool;

  auto reg() -> const registry&;

  auto dh() -> diagnostic_handler&;

  explicit(false) operator diagnostic_handler&();

  explicit(false) operator registry const&();

private:
  session_provider& provider_;
};

} // namespace tenzir
