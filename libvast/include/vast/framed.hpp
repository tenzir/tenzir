//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

namespace vast {

/// A CAF-streaming friendly vwrapper type that supports sending a sentinel
/// value in-stream to signal the end of a strem.
template <class T>
class framed final {
public:
  framed() noexcept = default;

  explicit(false) framed(std::optional<T> value) noexcept
    : value_{value ? std::make_shared<T>(*std::move(value)) : nullptr} {
  }

  auto is_sentinel() const -> bool {
    return value_ == nullptr;
  }

  auto value() const -> const T& {
    return *value_;
  }

  auto value() -> T& {
    return *value_;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, framed& x) -> bool {
    if constexpr (Inspector::is_loading) {
      auto is_sentinel = false;
      if (not f.apply(is_sentinel))
        return false;
      if (is_sentinel) {
        x = framed<T>{std::nullopt};
        return true;
      }
      auto value = T{};
      if (not f.apply(value))
        return false;
      x = framed<T>{std::move(value)};
      return true;
    } else {
      auto is_sentinel = x.is_sentinel();
      if (not f.apply(is_sentinel))
        return false;
      if (is_sentinel)
        return true;
      auto& value = x.value();
      return f.apply(value);
    }
  }

private:
  std::shared_ptr<T> value_;
};

} // namespace vast
