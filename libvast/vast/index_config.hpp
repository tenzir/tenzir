//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/stable_map.hpp"
#include "vast/type.hpp"

#include <caf/optional.hpp>
#include <fmt/core.h>

#include <optional>
#include <string>

namespace vast {

/// The configuration for `vast.index`.
struct index_config {
  // TODO(MV): establish a convention on how to handle defaults in a scalable
  // fashion. Rather than keeping all defaults globally, we should perhaps move
  // to a more local scope, e.g., have every option block come with their own
  // struct.
  struct defaults {
    static constexpr double fp_rate = 0.01;
  };

  struct rule {
    std::vector<std::string> targets;
    double fp_rate = defaults::fp_rate;

    template <class Inspector>
    friend auto inspect(Inspector& f, rule& x) {
      return f(x.targets, x.fp_rate);
    }

    static inline const record_type& layout() noexcept {
      static auto result = record_type{
        {"targets", list_type{string_type{}}},
        {"fp-rate", real_type{}},
      };
      return result;
    }
  };

  std::vector<rule> rules;

  template <class Inspector>
  friend auto inspect(Inspector& f, index_config& x) {
    return f(x.rules);
  }

  static inline const record_type& layout() noexcept {
    static auto result = record_type{
      {"rules", list_type{rule::layout()}},
    };
    return result;
  }
};

} // namespace vast

// TODO(MV): replace with Dominik's convenient helpers.
namespace fmt {

template <>
struct formatter<struct vast::index_config::rule>
  : formatter<std::string_view> {
  template <class FormatContext>
  constexpr auto
  format(const struct vast::index_config::rule& x, FormatContext& ctx)
    -> decltype(ctx.out()) {
    return formatter<std::string_view>::format(caf::deep_to_string(x), ctx);
  }
};

template <>
struct formatter<vast::index_config> : formatter<std::string_view> {
  template <class FormatContext>
  constexpr auto format(const struct vast::index_config& x, FormatContext& ctx)
    -> decltype(ctx.out()) {
    return formatter<std::string_view>::format(caf::deep_to_string(x), ctx);
  }
};

} // namespace fmt
