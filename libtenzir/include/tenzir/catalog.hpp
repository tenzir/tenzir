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
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/taxonomies.hpp"
#include "tenzir/uuid.hpp"

#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <vector>

namespace tenzir {

/// The result of a catalog query.
struct catalog_lookup_result {
  struct candidate_info {
    expression exp;
    std::vector<partition_info> partition_infos;

    template <class Inspector>
    friend auto inspect(Inspector& f, candidate_info& x) -> bool {
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

  auto empty() const noexcept -> bool;

  auto size() const noexcept -> size_t;

  template <class Inspector>
  friend auto inspect(Inspector& f, enum kind& x) -> bool {
    return detail::inspect_enum(f, x);
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, catalog_lookup_result& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.catalog_lookup_result")
      .fields(f.field("kind", x.kind),
              f.field("candidate-infos", x.candidate_infos));
  }
};

/// The state of the CATALOG actor.
struct catalog_state {
public:
  catalog_state() = default;

  constexpr static auto name = "catalog";

  /// Creates the catalog from a set of partition synopses.
  auto initialize(
    std::shared_ptr<std::unordered_map<uuid, partition_synopsis_ptr>> ps)
    -> caf::result<atom::ok>;

  /// Add a new partition synopsis.
  auto merge(std::vector<partition_synopsis_pair> partitions)
    -> caf::result<atom::ok>;

  /// Erase this partition from the catalog.
  void erase(const uuid& partition);

  /// Retrieves the list of candidate partition IDs for a given expression.
  /// @param expr The expression to lookup.
  /// @returns A lookup result of candidate partitions categorized by type.
  auto lookup(expression expr) const -> caf::expected<catalog_lookup_result>;

  auto lookup_impl(const expression& expr, const type& schema) const
    -> catalog_lookup_result::candidate_info;

  /// @returns A best-effort estimate of the amount of memory used for this
  /// catalog (in bytes).
  auto memusage() const -> size_t;

  /// Update the list of fields that should not be touched by the pruner.
  void update_unprunable_fields(const partition_synopsis& ps);

  // -- data members -----------------------------------------------------------

  /// A pointer to the parent actor.
  catalog_actor::pointer self = {};

  /// For each type, maps a partition ID to the synopses for that partition.
  // We mainly iterate over the whole map and return a sorted set, for which
  // the `flat_map` proves to be much faster than `std::{unordered_,}set`.
  // See also ae9dbed.
  std::unordered_map<tenzir::type,
                     detail::flat_map<uuid, partition_synopsis_ptr>>
    synopses_per_type = {};

  /// The set of fields that should not be touched by the pruner.
  detail::heterogeneous_string_hashset unprunable_fields;

  tenzir::taxonomies taxonomies = {};
};

/// The CATALOG is the first index actor that queries hit. The result
/// represents a list of candidate partition IDs that may contain the desired
/// data. The CATALOG may return false positives but never false negatives.
/// @param self The actor handle.
auto catalog(catalog_actor::stateful_pointer<catalog_state> self)
  -> catalog_actor::behavior_type;

} // namespace tenzir
