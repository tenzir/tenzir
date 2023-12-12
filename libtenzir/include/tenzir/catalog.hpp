//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/detail/flat_map.hpp"
#include "tenzir/detail/heterogeneous_string_hash.hpp"
#include "tenzir/detail/inspection_common.hpp"
#include "tenzir/expression.hpp"
#include "tenzir/module.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/taxonomies.hpp"
#include "tenzir/time_synopsis.hpp"
#include "tenzir/uuid.hpp"

#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <map>
#include <string>
#include <vector>

namespace tenzir {

/// The result of a catalog query.
struct legacy_catalog_lookup_result {
  struct candidate_info {
    expression exp;
    std::vector<partition_info> partition_infos;

    template <class Inspector>
    friend auto inspect(Inspector& f, candidate_info& x) {
      return f.object(x)
        .pretty_name("tenzir.system.catalog_result.candidate_info")
        .fields(f.field("expression", x.exp),
                f.field("partition-infos", x.partition_infos));
    }
  };

  enum kind {
    exact,
    probabilistic,
  };

  enum kind kind { kind::exact };

  std::unordered_map<type, candidate_info> candidate_infos;

  [[nodiscard]] bool empty() const noexcept {
    return candidate_infos.empty();
  }

  [[nodiscard]] size_t size() const noexcept {
    return std::accumulate(candidate_infos.begin(), candidate_infos.end(),
                           size_t{0}, [](auto i, const auto& cat_result) {
                             return std::move(i)
                                    + cat_result.second.partition_infos.size();
                           });
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, enum kind& x) {
    return detail::inspect_enum(f, x);
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, legacy_catalog_lookup_result& x) {
    return f.object(x)
      .pretty_name("tenzir.system.legacy_catalog_lookup_result")
      .fields(f.field("kind", x.kind),
              f.field("candidate-infos", x.candidate_infos));
  }
};

/// The state of the CATALOG actor.
struct catalog_state {
public:
  // -- constructor ------------------------------------------------------------

  catalog_state() = default;

  // -- concepts ---------------------------------------------------------------

  constexpr static auto name = "catalog";

  // -- utility functions ------------------------------------------------------

  /// Adds new synopses for a partition in bulk. Used when
  /// re-building the catalog state at startup.
  void
  create_from(std::unordered_map<uuid, partition_synopsis_ptr>&& partitions);

  /// Add a new partition synopsis.
  void merge(const uuid& uuid, partition_synopsis_ptr partition);

  /// Erase this partition from the catalog.
  void erase(const uuid& partition);

  /// Atomically replace partitions in the catalog.
  void replace(const std::vector<uuid>& old_uuids,
               std::vector<partition_synopsis_pair> new_partitions);

  /// @returns A best-effort estimate of the amount of memory used for this
  /// catalog (in bytes).
  [[nodiscard]] size_t memusage() const;

  /// Update the list of fields that should not be touched by the pruner.
  void update_unprunable_fields(const partition_synopsis& ps);

  /// Get a list of known schemas from the registry.
  [[nodiscard]] type_set schemas() const;

  /// Sends metrics to the accountant.
  void emit_metrics() const;

  // -- data members -----------------------------------------------------------

  /// A pointer to the parent actor.
  catalog_actor::pointer self = {};

  /// An actor handle to the accountant.
  accountant_actor accountant = {};

  // A list of partitions kept in reverse-chronological order (sorted by
  // max-import-time).
  std::deque<partition_synopsis_pair> partitions = {};

  /// The set of fields that should not be touched by the pruner.
  detail::heterogeneous_string_hashset unprunable_fields;

  tenzir::taxonomies taxonomies = {};
};

/// The CATALOG is the first index actor that queries hit. The result
/// represents a list of candidate partition IDs that may contain the desired
/// data. The CATALOG may return false positives but never false negatives.
/// @param self The actor handle.
/// @param accountant An actor handle to the accountant.
catalog_actor::behavior_type
catalog(catalog_actor::stateful_pointer<catalog_state> self,
        accountant_actor accountant);

} // namespace tenzir
