//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
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

  /// The number of worker threads used to load partition synopses from disk
  /// when the index starts up. Zero selects a default derived from the hardware
  /// concurrency. Loading is dominated by I/O latency (especially on networked
  /// storage such as NFS), so values above the core count can further reduce
  /// startup time by keeping more requests in flight.
  size_t load_concurrency = 0;

  /// When non-zero, opaque partition synopses (most notably Bloom filters)
  /// whose serialized payload exceeds this many bytes are not deserialized when
  /// loading the catalog at startup. The corresponding fields are still
  /// registered, but with no synopsis, so the catalog conservatively treats
  /// predicates on them as candidates (it may return false positives, never
  /// false negatives). This drastically lowers resident memory and startup cost
  /// for nodes with very many partitions, at the price of coarser pruning for
  /// equality predicates on high-cardinality fields. Min/max and time synopses
  /// are always loaded, so range pruning (e.g. on a timestamp field) is
  /// unaffected.
  size_t lazy_sketch_threshold = 0;

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
/// Uses explicit field extraction for efficient deserialization.
/// @param src The source data, expected to be a record.
/// @param dst The destination index_config to populate.
/// @returns An error if conversion fails, or caf::none on success.
caf::error convert(const data& src, index_config& dst);

} // namespace tenzir
