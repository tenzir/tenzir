#ifndef VAST_PARTITION_H
#define VAST_PARTITION_H

#include <queue>
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

struct partition_actor : actor_base
{
  struct indexer_state
  {
    struct statistics
    {
      uint64_t backlog = 0;         // Number of outstanding batches.
      uint64_t value_total = 0;     // Total values indexed.
      uint64_t value_rate = 0;      // Last indexing rate (values/sec).
      uint64_t value_rate_mean = 0; // Mean indexing rate (values/sec).
    };

    cppa::actor actor;
    type_const_ptr type;
    offset off;
    statistics stats;
  };

  partition_actor(path dir, size_t batch_size, uuid id = uuid::random());

  cppa::partial_function act() final;
  std::string describe() const final;

  template <typename Bitstream = default_bitstream>
  trial<void> load_data_indexer(path const& p)
  {
    auto i = indexers_.find(p);
    if (i == indexers_.end())
      return error{"no such path:", };

    if (i->second.actor)
      return nothing;

    return create_data_indexer<Bitstream>(p, i->second.off, i->second.type);
  }

  template <typename Bitstream = default_bitstream>
  trial<void>
  create_data_indexer(path const& p, offset const& o, type_const_ptr t)
  {
    assert(t);
    auto& state = indexers_[p];
    if (state.type && *state.type != *t)
      return error{"type mismatch:", *state.type, " vs", *t};

    if (state.actor)
      return nothing;

    VAST_LOG_ACTOR_DEBUG("creates indexer at " << p <<
                         " @" << o << " with type " << *t);

    auto abs = dir_ / p / "data.idx";
    auto a = make_event_data_indexer<Bitstream>(abs, t, o);
    if (! a)
      return error{"failed to construct data indexer:", a.error()};

    monitor(*a);

    state.actor = *a;
    state.type = t;
    state.off = o;

    return nothing;
  }

  path dir_;
  bool updated_ = false;
  size_t batch_size_;
  uint64_t max_backlog_ = 0;
  uint32_t exit_reason_ = 0;
  partition partition_;
  schema schema_;
  std::unordered_map<path, indexer_state> indexers_;
  std::queue<cppa::any_tuple> segments_;
  cppa::actor unpacker_;
};

} // namespace vast

#endif
