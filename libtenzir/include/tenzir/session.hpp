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
  static auto make(diagnostic_handler& dh) -> session_provider {
    return session_provider{dh};
  }

  auto as_session() & -> session;
  auto as_session() && -> session = delete;

private:
  friend class session;

  class diagnostic_ctx final : public diagnostic_handler {
  public:
    explicit diagnostic_ctx(diagnostic_handler& dh) : dh_{dh} {
    }

    void emit(diagnostic d) override {
      if (d.severity == severity::error) {
        failure_ = failure::promise();
      }
      dh_.emit(d);
    }

    std::optional<failure> failure_;
    diagnostic_handler& dh_;
  };

  explicit session_provider(diagnostic_handler& dh) : dh_{dh} {
  }

  diagnostic_ctx dh_;
};

/// This is meant to be used as a value type.
class session {
public:
  explicit session(session_provider& provider) : provider_{provider} {
  }

  auto get_failure() const -> std::optional<failure> {
    return provider_.dh_.failure_;
  }

  auto has_failure() const -> bool {
    return get_failure().has_value();
  }

  auto reg() -> const registry&;

  auto dh() -> diagnostic_handler& {
    return provider_.dh_;
  }

  operator diagnostic_handler&() {
    return dh();
  }

private:
  session_provider& provider_;
};

inline auto session_provider::as_session() & -> session {
  return session{*this};
}

} // namespace tenzir
