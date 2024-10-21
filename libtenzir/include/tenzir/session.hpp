//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/tql2/registry.hpp"

namespace tenzir {

class session_provider {
public:
  static auto make(diagnostic_handler& dh) -> session_provider;

  auto as_session() & -> session;
  auto as_session() && -> session = delete;

private:
  friend class session;

  class diagnostic_ctx final : public diagnostic_handler {
  public:
    explicit diagnostic_ctx(diagnostic_handler& dh);

    void emit(diagnostic d) override;

    std::optional<failure> failure_;
    diagnostic_handler& dh_;
  };

  explicit session_provider(diagnostic_handler& dh);

  diagnostic_ctx dh_;
};

/// This is meant to be used as a value type.
class session {
public:
  explicit session(session_provider& provider);

  auto get_failure() const -> std::optional<failure>;

  auto has_failure() const -> bool;

  auto reg() -> const registry&;

  auto dh() -> diagnostic_handler&;

  explicit(false) operator diagnostic_handler&();

private:
  session_provider& provider_;
};

} // namespace tenzir
