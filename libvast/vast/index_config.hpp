//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/type.hpp"

#include <string>
#include <vector>

namespace vast {

/// The configuration that defines VAST's indexing behavior.
struct index_config {
  static constexpr bool use_deep_to_string_formatter = true;

  struct rule {
    std::vector<std::string> targets = {};
    double fp_rate = 0.01;

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

  std::vector<rule> rules = {};

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
