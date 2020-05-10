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

#include "vast/fbs/meta_index.hpp"
#include "vast/fwd.hpp"
#include "vast/qualified_record_field.hpp"
#include "vast/synopsis.hpp"
#include "vast/type.hpp"
#include "vast/uuid.hpp"

#include <caf/fwd.hpp>
#include <caf/settings.hpp>

#include <functional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace vast {

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

private:
  /// Contains one synopsis per partition column.
  using partition_synopsis
    = std::unordered_map<qualified_record_field, synopsis_ptr>;

  /// Maps a partition ID to the synopses for that partition.
  std::unordered_map<uuid, partition_synopsis> synopses_;

  /// Settings for the synopsis factory.
  caf::settings synopsis_options_;
};

// -- flatbuffer ---------------------------------------------------------------

caf::expected<flatbuffers::Offset<fbs::MetaIndex>>
pack(flatbuffers::FlatBufferBuilder& builder, const meta_index& x);

caf::error unpack(const fbs::MetaIndex& x, meta_index& y);

} // namespace vast
