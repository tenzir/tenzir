#ifndef VAST_PARTITION_H
#define VAST_PARTITION_H

#include "vast/actor.h"
#include "vast/bitmap_indexer.h"
#include "vast/file_system.h"
#include "vast/schema.h"
#include "vast/string.h"
#include "vast/time.h"
#include "vast/uuid.h"
#include "vast/util/result.h"

namespace vast {

class segment;

/// A horizontal partition of the index.
class partition
{
public:
  static path const part_meta_file;

  struct meta_data : util::equality_comparable<meta_data>
  {
    meta_data() = default;
    meta_data(uuid id);

    void update(segment const& s);

    uuid id;
    time_point first_event = time_range{};
    time_point last_event = time_range{};
    time_point last_modified = now();

    void serialize(serializer& sink) const;
    void deserialize(deserializer& source);
    friend bool operator==(meta_data const& x, meta_data const& y);
  };

  /// Constructs a partition.
  /// @id The UUID to use for this partition.
  partition(uuid id);

  /// Updates the partition meta data with an indexed segment.
  void update(segment const& s);

  /// Retrieves the partition meta data.
  meta_data const& meta() const;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  meta_data meta_;
};

struct partition_actor : actor<partition_actor>
{
  struct indexer_state
  {
    struct statistics
    {
      uint64_t values = 0;
      uint64_t rate = 0;
      uint64_t mean = 0;
    };

    cppa::actor_ptr actor;
    type_const_ptr type;
    offset off;
    statistics stats;
  };

  partition_actor(path dir, size_t batch_size, uuid id = uuid::random());

  void act();
  char const* description() const;

  template <typename Bitstream = default_bitstream>
  trial<cppa::actor_ptr> load_indexer(path const& p)
  {
    auto i = indexers_.find(p);
    if (i == indexers_.end())
      return error{"no such path: " + to<std::string>(p.str())};

    if (i->second.actor)
      return i->second.actor;

    return create_indexer<Bitstream>(p, i->second.off, i->second.type);
  }

  template <typename Bitstream = default_bitstream>
  trial<cppa::actor_ptr>
  create_indexer(path const& p, offset const& o, type_const_ptr t)
  {
    auto& state = indexers_[p];
    assert(! state.actor);

    auto abs = dir_ / "types" / p / "data.idx";
    auto a = make_event_data_indexer<Bitstream>(abs, t, o);
    if (! a)
      return a;

    monitor(*a);

    state.actor = *a;
    state.type = t;
    state.off = o;

    return *a;
  }

  path dir_;
  size_t batch_size_;
  partition partition_;
  schema schema_;
  cppa::actor_ptr time_indexer_;
  cppa::actor_ptr name_indexer_;
  std::unordered_map<path, indexer_state> indexers_;
};

} // namespace vast

#endif
