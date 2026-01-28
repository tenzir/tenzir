//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/defaults.hpp"
#include "tenzir/detail/inspection_common.hpp"
#include "tenzir/type.hpp"

#include <caf/error.hpp>

#include <string>
#include <vector>

namespace tenzir {

/// The configuration that defines Tenzir's indexing behavior.
struct index_config {
  static constexpr bool use_deep_to_string_formatter = true;

  struct rule {
    std::vector<std::string> targets = {};
    double fp_rate = defaults::fp_rate;
    bool create_partition_index = defaults::create_partition_index;

    template <class Inspector>
    friend auto inspect(Inspector& f, rule& x) {
      return detail::apply_all(f, x.targets, x.fp_rate,
                               x.create_partition_index);
    }

    static inline const record_type& schema() noexcept {
      static auto result = record_type{
        {"targets", list_type{string_type{}}},
        {"fp-rate", double_type{}},
        {"partition-index", bool_type{}},
      };
      return result;
    }
  };

  std::vector<rule> rules = {};
  double default_fp_rate = defaults::fp_rate;

  template <class Inspector>
  friend auto inspect(Inspector& f, index_config& x) {
    return detail::apply_all(f, x.rules, x.default_fp_rate);
  }

  static inline const record_type& schema() noexcept {
    static auto result = record_type{
      {"rules", list_type{rule::schema()}},
      {"default-fp-rate", double_type{}},
    };
    return result;
  }
};

bool should_create_partition_index(const qualified_record_field& index_qf,
                                   const std::vector<index_config::rule>& rules);

/// Converts data (record from YAML/config) to index_config.
/// This is a targeted conversion that avoids the expensive generic match()
/// in concept/convertible/data.hpp.
/// @param src The source data, expected to be a record.
/// @param dst The destination index_config to populate.
/// @returns An error if conversion fails, or caf::none on success.
caf::error convert(const data& src, index_config& dst);

} // namespace tenzir
