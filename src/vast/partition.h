#ifndef VAST_PARTITION_H
#define VAST_PARTITION_H

#include "vast/actor.h"
#include "vast/bitmap_indexer.h"
#include "vast/file_system.h"
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
  static path const event_data_dir;

  struct meta_data : util::equality_comparable<meta_data>
  {
    meta_data() = default;
    meta_data(uuid id);

    void update(segment const& s);

    uuid id;
    time_point first_event = time_range{};
    time_point last_event = time_range{};
    time_point last_modified = now();
    bitstream coverage;

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
    value_type type;
    cppa::actor_ptr actor;
  };

  struct indexer_stats
  {
    uint64_t values = 0;
    uint64_t rate = 0;
    uint64_t mean = 0;
  };

  partition_actor(path dir, size_t batch_size, uuid id = uuid::random());

  void act();
  char const* description() const;

  template <typename Bitstream = default_bitstream>
  result<cppa::actor_ptr> load_indexer(string const& e, offset const& o)
  {
    auto i = indexers_.find(e);
    if (i == indexers_.end())
      return {};

    auto j = i->second.find(o);
    if (j == i->second.end())
      return {};

    if (j->second.actor)
      return j->second.actor;

    auto a = create_indexer<Bitstream>(e, o, j->second.type);
    if (! a)
      return a.failure();

    monitor(*a);
    j->second.actor = *a;

    return *a;
  }

  template <typename Bitstream = default_bitstream>
  trial<cppa::actor_ptr> create_indexer(string const& e, offset const& o, value_type t)
  {
    auto& is = indexers_[e][o];
    assert(! is.actor);

    auto p = dir_ / partition::event_data_dir / e / (to<string>(o) + ".idx");
    auto a = make_indexer<Bitstream>(t, std::move(p), o);
    if (! a)
      return a;

    monitor(*a);
    is.actor = *a;
    is.type = t;

    return *a;
  }

  path dir_;
  size_t batch_size_;
  partition partition_;
  cppa::actor_ptr time_indexer_;
  cppa::actor_ptr name_indexer_;
  std::unordered_map<string, std::map<offset, indexer_state>> indexers_;
  std::unordered_map<cppa::actor_ptr, indexer_stats> stats_;
};

} // namespace vast

#endif
