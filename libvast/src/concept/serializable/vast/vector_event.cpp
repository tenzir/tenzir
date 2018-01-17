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

#include "vast/util/assert.hpp"
#include "vast/util/flat_set.hpp"

#include "vast/concept/serializable/vast/data.hpp"
#include "vast/concept/serializable/vast/type.hpp"
#include "vast/concept/serializable/vast/vector_event.hpp"

namespace vast {

void serialize(caf::serializer& sink, std::vector<event> const& events) {
  util::flat_set<type::hash_type::digest_type> digests;
  auto size = events.size();
  sink.begin_sequence(size);
  for (auto& e : events) {
    auto digest = e.type().digest();
    sink << digest;
    if (digests.count(digest) == 0) {
      digests.insert(digest);
      sink << e.type();
    }
    sink << e.data() << e.id() << e.timestamp();
  }
  sink.end_sequence();
}

void serialize(caf::deserializer& source, std::vector<event>& events) {
  std::map<type::hash_type::digest_type, type> types;
  size_t size;
  source.begin_sequence(size);
  events.resize(size);
  for (auto& e : events) {
    type::hash_type::digest_type digest;
    source >> digest;
    auto i = types.find(digest);
    if (i == types.end()) {
      type t;
      source >> t;
      VAST_ASSERT(digest == t.digest());
      i = types.emplace(digest, std::move(t)).first;
    }
    data d;
    event_id id;
    time::point ts;
    source >> d >> id >> ts;
    e = value{std::move(d), i->second};
    e.id(id);
    e.timestamp(ts);
  }
  source.end_sequence();
}

} // namespace vast
