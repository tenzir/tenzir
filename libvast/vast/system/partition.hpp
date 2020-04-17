/******************************************************************************

 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include "vast/detail/stable_map.hpp"
#include "vast/fwd.hpp"
#include "vast/ids.hpp"
#include "vast/qualified_record_field.hpp"
#include "vast/system/index_common.hpp"
#include "vast/system/instrumentation.hpp"
#include "vast/type.hpp"
#include "vast/uuid.hpp"

#include <caf/event_based_actor.hpp>
#include <caf/stream_slot.hpp>

#include <unordered_map>

namespace vast::system {

class indexer_downstream_manager;

/// The horizontal data scaling unit of the index. A partition represents a
/// slice of indexes for a specific ID interval.
class partition {
public:
  // -- member types -----------------------------------------------------------

  /// Persistent meta state for the partition.
  struct meta_data {
    /// Maps type digests (used as directory names) to layouts (i.e. record
    /// types).
    std::unordered_map<std::string, record_type> types;

    /// Maps type names to ids.
    std::unordered_map<std::string, ids> type_ids;

    /// Stores whether the partition has been mutated in memory.
    bool dirty = false;
  };

  /// A path that connects the incoming stream of table slices to an indexer.
  struct wrapped_indexer {
    /// Mutable because it can be initalized layzily.
    mutable caf::actor indexer;

    /// Only used during ingestion.
    caf::stream_slot slot;

    /// The message queue of the downstream indexer.
    /// Only used during ingestion.
    caf::outbound_path* outbound;

    /// A buffer to avoid overloading the indexer.
    /// Only used during ingestion.
    std::vector<table_slice_column> buf;
  };

  // -- constructors, destructors, and assignment operators --------------------

  /// @param self The parent actor.
  /// @param id Unique identifier for this partition.
  /// @param max_capacity The amount of events this partition can hold.
  /// @pre `self != nullptr`
  /// @pre `factory != nullptr`
  partition(index_state* state, uuid id, size_t max_capacity);

  ~partition() noexcept;

  // -- persistence ------------------------------------------------------------

  /// Materializes the partition layouts from disk.
  /// @returns an error if I/O operations fail.
  caf::error init();

  /// Persists the partition layouts to disk.
  /// @returns an error if I/O operations fail.
  caf::error flush_to_disk();

  // -- properties -------------------------------------------------------------

  /// @returns the unique ID of the partition.
  auto& id() const noexcept {
    return id_;
  }

  /// @returns the state of the owning INDEX actor.
  auto& state() noexcept {
    return *state_;
  }

  /// @returns the remaining capacity in this partition.
  auto capacity() const noexcept {
    return capacity_;
  }

  /// @returns a record type containing all columns of this partition.
  // TODO: Should this be renamed to layout()?
  record_type combined_type() const;

  /// @returns the directory for persistent state.
  path base_dir() const;

  /// @returns the file name for saving or loading the ::meta_data.
  path meta_file() const;

  indexer_downstream_manager& out() const;

  /// @returns the file name for `column`.
  path column_file(const qualified_record_field& field) const;

  // -- operations -------------------------------------------------------------

  void finalize();

  /// @moves a slice into the partition.
  void add(table_slice_ptr slice);

  caf::expected<std::pair<caf::actor, bool>> get(const record_field& field);

  caf::actor& indexer_at(size_t position);

  caf::actor fetch_indexer(const data_extractor& dx, relational_operator op,
                           const data& x);

  caf::actor fetch_indexer(const attribute_extractor& ex,
                           relational_operator op, const data& x);

  /// @returns all INDEXER actors required for a query.
  evaluation_triples eval(const expression& expr);

  // -- members ----------------------------------------------------------------

  /// State of the INDEX actor that owns this partition.
  index_state* state_;

  /// Keeps track of row types in this partition.
  meta_data meta_data_;

  /// Uniquely identifies this partition.
  uuid id_;

  /// A map to the indexers
  detail::stable_map<qualified_record_field, wrapped_indexer> indexers_;

  /// Instrumentation data store, one entry for each INDEXER.
  std::unordered_map<size_t, atomic_measurement> measurements_;

  /// Remaining capacity in this partition.
  size_t capacity_;

  std::vector<table_slice_ptr> inbound;

  friend class index_state;
  friend class indexer_downstream_manager;
};

// -- related types ------------------------------------------------------------

/// @relates partition
using partition_ptr = std::unique_ptr<partition>;

// -- free functions -----------------------------------------------------------

/// @relates partition::meta_data
template <class Inspector>
auto inspect(Inspector& f, partition::meta_data& x) {
  return f(x.types, x.type_ids);
}

} // namespace vast::system

namespace std {

template <>
struct hash<vast::system::partition_ptr> {
  size_t operator()(const vast::system::partition_ptr& ptr) const;
};

} // namespace std
