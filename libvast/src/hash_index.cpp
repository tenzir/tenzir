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

#include "vast/hash_index.hpp"

#include "vast/concept/hashable/uhash.hpp"
#include "vast/concept/hashable/xxhash.hpp"
#include "vast/detail/assert.hpp"

#include <caf/deserializer.hpp>
#include <caf/serializer.hpp>

#include <cstring>

namespace vast {

namespace {

auto hash(data_view x) {
  return uhash<xxhash64>{}(x);
}

} // namespace

hash_index::hash_index(vast::type t, size_t digest_bytes)
  : value_index{std::move(t)}, digest_bytes_{digest_bytes} {
  VAST_ASSERT(digest_bytes > 0);
  VAST_ASSERT(digest_bytes <= 8);
}

caf::error hash_index::serialize(caf::serializer& sink) const {
  return caf::error::eval([&] { return value_index::serialize(sink); },
                          [&] { return sink(digests_); });
}

caf::error hash_index::deserialize(caf::deserializer& source) {
  return caf::error::eval([&] { return value_index::deserialize(source); },
                          [&] { return source(digests_); });
}

bool hash_index::append_impl(data_view x, id) {
  auto digest = hash(x);
  digests_.resize(digests_.size() + digest_bytes_);
  auto ptr = digests_.data() + num_digests_ * digest_bytes_;
  std::memcpy(ptr, &digest, digest_bytes_);
  ++num_digests_;
  return true;
}

caf::expected<ids>
hash_index::lookup_impl(relational_operator op, data_view x) const {
  VAST_ASSERT(rank(this->mask()) == num_digests_);
  if (!(op == equal || op == not_equal))
    return make_error(ec::unsupported_operator, op);
  auto digest = hash(x);
  ewah_bitmap result;
  auto rng = select(this->mask());
  if (rng.done())
    return result;
  for (size_t i = 0, last_match = 0; i < num_digests_; ++i) {
    auto ptr = digests_.data() + i * digest_bytes_;
    if (std::memcmp(ptr, &digest, digest_bytes_) == 0) {
      auto digests_since_last_match = i - last_match;
      if (digests_since_last_match > 0)
        rng.next(digests_since_last_match);
      result.append(false, rng.get() - result.size());
      result.append<true>();
      last_match = i;
    }
  }
  return result;
}

} // namespace vast
