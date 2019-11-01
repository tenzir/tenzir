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

#include "vast/value_index.hpp"
#include "vast/view.hpp"

#include <caf/expected.hpp>

#include <string>
#include <type_traits>
#include <vector>

namespace vast {

/// An index that only supports equality lookup by hashing its data.
class hash_index : public value_index {
public:
  /// Constructs a hash index for a particular type and digest cutoff.
  /// @param t The type associated with this index.
  /// @param digest_bytes The number of bytes to keep of a hash digest.
  explicit hash_index(vast::type t, size_t digest_bytes);

  caf::error serialize(caf::serializer& sink) const override;

  caf::error deserialize(caf::deserializer& source) override;

private:
  bool append_impl(data_view x, id) override;

  caf::expected<ids>
  lookup_impl(relational_operator op, data_view x) const override;

  std::vector<byte> digests_;
  const size_t digest_bytes_;
  size_t num_digests_ = 0;
};

} // namespace vast
