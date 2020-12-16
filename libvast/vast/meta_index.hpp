/******************************************************************************
 *                    _   _____   __________                                  *
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

#include "vast/fbs/index.hpp"
#include "vast/fbs/partition.hpp"
#include "vast/fwd.hpp"
#include "vast/ids.hpp"
#include "vast/qualified_record_field.hpp"
#include "vast/synopsis.hpp"
#include "vast/time_synopsis.hpp"
#include "vast/type.hpp"
#include "vast/uuid.hpp"

#include <caf/fwd.hpp>
#include <caf/settings.hpp>

#include <flatbuffers/flatbuffers.h>

#include <functional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace vast {

namespace system {

// Forward declaration to be able to `friend` this function.
caf::expected<flatbuffers::Offset<fbs::Partition>>
pack(flatbuffers::FlatBufferBuilder& builder,
     const system::active_partition_state& x);

} // namespace system

/// Contains one synopsis per partition column.
//  TODO: Move this into a separate `partition_synopsis.hpp` header.
struct partition_synopsis {
  /// Optimizes the partition synopsis contents for size.
  void shrink();

  void add(const table_slice& slice, const caf::settings& synopsis_options);

  /// @returns A best-effort estimate of the amount of memory used by this
  ///          synopsis.
  size_t size_bytes() const;

  /// Synopsis data structures for types.
  std::unordered_map<type, synopsis_ptr> type_synopses_;

  /// Synopsis data structures for individual columns.
  std::unordered_map<qualified_record_field, synopsis_ptr> field_synopses_;
};

/// The meta index is the first data structure that queries hit. The result
/// represents a list of candidate partition IDs that may contain the desired
/// data. The meta index may return false positives but never false negatives.
class meta_index {
public:
  /// Adds all data from a table slice belonging to a given partition to the
  /// index.
  /// @param slice The table slice to extract data from.
  /// @param partition The partition ID that *slice* belongs to.
  void add(const uuid& partition, const table_slice& slice);

  /// Adds new synopses for a partition in bulk. Used when
  /// re-building the meta index state at startup.
  void merge(const uuid& partition, partition_synopsis&&);

  /// Replaces an existing partition synopsis. Does nothing
  /// if `partition` does not exist as a key.
  void replace(const uuid& partition, std::unique_ptr<partition_synopsis>);

  /// Returns the partition synopsis for a specific partition.
  /// Note that most callers will prefer to use `lookup()` instead.
  /// @pre `partition` must be a valid key for this meta index.
  partition_synopsis& at(const uuid& partition);

  /// Erase this partition from the meta index.
  void erase(const uuid& partition);

  /// Retrieves the list of candidate partition IDs for a given expression.
  /// @param expr The expression to lookup.
  /// @returns A vector of UUIDs representing candidate partitions.
  std::vector<uuid> lookup(const expression& expr) const;

  /// @returns A best-effort estimate of the amount of memory used for this meta
  /// index (in bytes).
  size_t size_bytes() const;

  /// Gets the options for the synopsis factory.
  /// @returns A reference to the synopsis options.
  caf::settings& factory_options();

  // -- concepts ---------------------------------------------------------------

  // Allow debug printing meta_index instances.
  template <class Inspector>
  friend auto inspect(Inspector& f, meta_index& x) {
    return f(x.synopsis_options_, x.synopses_);
  }

  // Allow the partition to directly serialize the relevant synopses.
  friend caf::expected<flatbuffers::Offset<fbs::Partition>>
  vast::system::pack(flatbuffers::FlatBufferBuilder& builder,
                     const system::active_partition_state& x);

private:
  /// Maps a partition ID to the synopses for that partition.
  std::unordered_map<uuid, partition_synopsis> synopses_;

  /// Settings for the synopsis factory.
  caf::settings synopsis_options_;
};

// -- flatbuffer ---------------------------------------------------------------

caf::expected<flatbuffers::Offset<fbs::partition_synopsis::v0>>
pack(flatbuffers::FlatBufferBuilder& builder, const partition_synopsis&);

caf::error unpack(const fbs::partition_synopsis::v0&, partition_synopsis&);

} // namespace vast
