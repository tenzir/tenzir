//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/defaults.hpp"
#include "vast/detail/inspection_common.hpp"
#include "vast/type.hpp"

#include <string>
#include <vector>

namespace vast {

/// The configuration that defines VAST's indexing behavior.
struct index_config {
  static constexpr bool use_deep_to_string_formatter = true;

  struct rule {
    std::vector<std::string> targets = {};
    double fp_rate = defaults::system::fp_rate;
    bool create_partition_index = defaults::system::create_partition_index;

    template <class Inspector>
    friend auto inspect(Inspector& f, rule& x) {
      return detail::apply_all(f, x.targets, x.fp_rate,
                               x.create_partition_index);
    }

    static inline const record_type& layout() noexcept {
      static auto result = record_type{
        {"targets", list_type{string_type{}}},
        {"fp-rate", real_type{}},
        {"partition-index", bool_type{}},
      };
      return result;
    }
  };

  std::vector<rule> rules = {};
  double default_fp_rate = defaults::system::fp_rate;

  template <class Inspector>
  friend auto inspect(Inspector& f, index_config& x) {
    return detail::apply_all(f, x.rules, x.default_fp_rate);
  }

  static inline const record_type& layout() noexcept {
    static auto result = record_type{
      {"rules", list_type{rule::layout()}},
      {"default-fp-rate", real_type{}},
    };
    return result;
  }
};

bool should_create_partition_index(const qualified_record_field& index_qf,
                                   const std::vector<index_config::rule>& rules);

} // namespace vast
