#ifndef VAST_CONCEPT_SERIALIZABLE_VAST_VECTOR_EVENT_HPP
#define VAST_CONCEPT_SERIALIZABLE_VAST_VECTOR_EVENT_HPP

#include <map>

#include "vast/concept/serializable/state.hpp"
#include "vast/concept/serializable/caf/adapters.hpp"
#include "vast/concept/serializable/vast/data.hpp"
#include "vast/concept/serializable/vast/type.hpp"
#include "vast/concept/state/event.hpp"
#include "vast/concept/state/value.hpp"
#include "vast/concept/state/time.hpp"
#include "vast/util/assert.hpp"
#include "vast/util/flat_set.hpp"

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
