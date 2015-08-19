#ifndef VAST_CONCEPT_SERIALIZABLE_VAST_VECTOR_EVENT_H
#define VAST_CONCEPT_SERIALIZABLE_VAST_VECTOR_EVENT_H

#include <map>

#include "vast/concept/serializable/state.h"
#include "vast/concept/serializable/caf/adapters.h"
#include "vast/concept/serializable/vast/data.h"
#include "vast/concept/serializable/vast/type.h"
#include "vast/concept/state/event.h"
#include "vast/concept/state/value.h"
#include "vast/concept/state/time.h"
#include "vast/util/assert.h"
#include "vast/util/flat_set.h"

namespace vast {

template <typename Serializer>
void serialize(Serializer& sink, std::vector<event> const& events) {
  util::flat_set<type::hash_type::digest_type> digests;
  sink << uint64_t{events.size()};
  for (auto& e : events) {
    auto digest = e.type().digest();
    sink << digest;
    if (digests.count(digest) == 0) {
      digests.insert(digest);
      sink << e.type();
    }
    sink << e.data() << e.id() << e.timestamp();
  }
}

template <typename Deserializer>
void deserialize(Deserializer& source, std::vector<event>& events) {
  std::map<type::hash_type::digest_type, type> types;
  uint64_t size;
  source >> size;
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
}

} // namespace vast

#endif
