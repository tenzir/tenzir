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
#include "vast/qualified_record_field.hpp"
#include "vast/synopsis.hpp"
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
caf::expected<flatbuffers::Offset<fbs::v1::Partition>>
pack(flatbuffers::FlatBufferBuilder& builder,
     const system::active_partition_state& x);

} // namespace system

/// Contains one synopsis per partition column.
//  TODO: Turn this into a proper struct with its own `add()` function.
//        Then we could store this in the `active_partition_state` directly
//        instead of using a meta_index with only one entry.
struct partition_synopsis
  : public std::unordered_map<qualified_record_field, synopsis_ptr> {};

/// The meta index is the first data structure that queries hit. The result
/// represents a list of candidate partition IDs that may contain the desired
/// data. The meta index may return false positives but never false negatives.
class meta_index {
public:
  /// Adds an (empty) entry for the given partition.
  void add(const uuid& partition);

  /// Adds all data from a table slice belonging to a given partition to the
  /// index.
  /// @param slice The table slice to extract data from.
  /// @param partition The partition ID that *slice* belongs to.
  void add(const uuid& partition, const table_slice& slice);

  /// Adds new synopses for a partition in bulk. Used when
  /// re-building the meta index state at startup.
  void merge(const uuid& partition, partition_synopsis&&);

  /// Retrieves the list of candidate partition IDs for a given expression.
  /// @param expr The expression to lookup.
  /// @returns A vector of UUIDs representing candidate partitions.
  std::vector<uuid> lookup(const expression& expr) const;

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
  friend caf::expected<flatbuffers::Offset<fbs::v1::Partition>>
  vast::system::pack(flatbuffers::FlatBufferBuilder& builder,
                     const system::active_partition_state& x);

private:
  /// Maps a partition ID to the synopses for that partition.
  std::unordered_map<uuid, partition_synopsis> synopses_;

  /// Settings for the synopsis factory.
  caf::settings synopsis_options_;
};

// -- flatbuffer ---------------------------------------------------------------

// TODO: Move these into some 'legacy' flatbuffer section
caf::expected<flatbuffers::Offset<fbs::v0::MetaIndex>>
pack(flatbuffers::FlatBufferBuilder& builder, const meta_index& x);

caf::error unpack(const fbs::v0::MetaIndex& x, meta_index& y);

caf::expected<flatbuffers::Offset<fbs::v0::PartitionSynopsis>>
pack(flatbuffers::FlatBufferBuilder& builder, const partition_synopsis&);

caf::error unpack(const fbs::v0::PartitionSynopsis&, partition_synopsis&);

} // namespace vast
