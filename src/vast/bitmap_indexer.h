#ifndef VAST_BITMAP_INDEXER_H
#define VAST_BITMAP_INDEXER_H

#include <cppa/cppa.hpp>
#include "vast/actor.h"
#include "vast/bitmap_index.h"
#include "vast/cow.h"
#include "vast/event.h"
#include "vast/expression.h"
#include "vast/file_system.h"
#include "vast/offset.h"
#include "vast/uuid.h"
#include "vast/io/serialization.h"
#include "vast/util/accumulator.h"

namespace vast {

/// Indexes a certain aspect of events with a single bitmap index.
/// @tparam Derived The CRTP client.
/// @tparam BitmapIndex The bitmap index type.
template <typename Derived, typename BitmapIndex>
class bitmap_indexer : public actor<bitmap_indexer<Derived, BitmapIndex>>
{
public:
  /// Spawns a bitmap indexer.
  /// @param path The absolute file path on the file system.
  bitmap_indexer(path path)
    : path_{std::move(path)},
      stats_{std::chrono::seconds{1}}
  {
    bmi_.append(1, false); // Event ID 0 is not a valid event.
    bmi_.checkpoint();
  }

  void act()
  {
    using namespace cppa;

    this->trap_exit(true);
    this->chaining(false);

    if (exists(path_))
    {
      io::unarchive(path_, bmi_);
      VAST_LOG_ACTOR_DEBUG("loaded bitmap index from " << path_ <<
                           " (" << bmi_.size() << " bits)");
    }

    auto flush = [=]
    {
      if (bmi_.appended() > 0)
      {
        io::archive(path_, bmi_);
        bmi_.checkpoint();
        VAST_LOG_ACTOR_DEBUG("flushed bitmap index to " << path_ <<
                             " (" << bmi_.size() << " bits)");
      }
    };

    become(
        on(atom("EXIT"), arg_match) >> [=](uint32_t reason)
        {
          if (reason != exit::kill)
            flush();

          this->quit(reason);
        },
        on(atom("flush")) >> flush,
        on_arg_match >> [=](std::vector<cow<event>> const& events)
        {
          uint64_t n = 0;
          for (auto& e : events)
            if (auto v = static_cast<Derived*>(this)->extract(*e))
              if (bmi_.push_back(*v, e->id()))
                ++n;

          stats_.increment(n);

          return make_any_tuple(atom("stats"), n, stats_.last(), stats_.mean());
        },
        on_arg_match >> [=](expr::ast const& pred, uuid const& part,
                            actor_ptr const& sink)
        {
          assert(pred.is_predicate());

          auto o = pred.find_operator();
          if (! o)
          {
            VAST_LOG_ACTOR_ERROR("failed to extract operator from " << pred);
            send(sink, pred, part, bitstream{});
            this->quit(exit::error);
            return;
          }

          auto c = pred.find_constant();
          if (! c)
          {
            VAST_LOG_ACTOR_ERROR("failed to extract constant from " << pred);
            send(sink, pred, part, bitstream{});
            this->quit(exit::error);
            return;
          }

          auto r = bmi_.lookup(*o, *c);
          if (! r)
          {
            VAST_LOG_ACTOR_ERROR(r.failure().msg());
            send(sink, pred, part, bitstream{});
            return;
          }

          send(sink, pred, part, std::move(*r));
        });
  }

  char const* description()
  {
    return "bitmap-indexer";
  }

private:
  BitmapIndex bmi_;
  path const path_;
  util::rate_accumulator<uint64_t> stats_;
};

template <typename Bitstream>
struct event_name_indexer
  : bitmap_indexer<
      event_name_indexer<Bitstream>,
      string_bitmap_index<Bitstream>
    >
{
  using bitmap_indexer<
    event_name_indexer<Bitstream>,
    string_bitmap_index<Bitstream>
  >::bitmap_indexer;

  string const* extract(event const& e) const
  {
    return &e.name();
  }
};

template <typename Bitstream>
struct event_time_indexer
  : bitmap_indexer<
      event_time_indexer<Bitstream>,
      arithmetic_bitmap_index<Bitstream, time_point_type>
    >
{
  using bitmap_indexer<
    event_time_indexer<Bitstream>,
    arithmetic_bitmap_index<Bitstream, time_point_type>
  >::bitmap_indexer;

  time_point const* extract(event const& e)
  {
    timestamp_ = e.timestamp();
    return &timestamp_;
  }

  time_point timestamp_;
};

template <typename BitmapIndex>
struct event_data_indexer
  : bitmap_indexer<event_data_indexer<BitmapIndex>, BitmapIndex>
{
  using super = bitmap_indexer<event_data_indexer<BitmapIndex>, BitmapIndex>;

  event_data_indexer(path p, offset o)
    : super{std::move(p)},
      offset_{std::move(o)}
  {
  }

  value const* extract(event const& e) const
  {
    return e.at(offset_);
  }

  offset offset_;
};

/// Factory to construct an indexer based on value type.
template <typename Bitstream, typename... Args>
trial<cppa::actor_ptr> make_indexer(value_type t, Args&&... args)
{
  // FIXME: why do these template aliases cause a compile error? It would be
  // nice to have 'em in order to reduce the boilerplate below.
  //template <value_type T>
  //using basic_indexer = event_data_indexer<
  //  arithmetic_bitmap_index<Bitstream, T>
  //>;

  //template <template <typename> class BitmapIndex>
  //using indexer = event_data_indexer<
  //  BitmapIndex<Bitstream>
  //>;

  using cppa::spawn;

  switch (t)
  {
    default:
      return error{"unspported value type: " + to_string(t)};
    case bool_type:
      return spawn<event_data_indexer<arithmetic_bitmap_index<Bitstream, bool_type>>>(std::forward<Args>(args)...);
    case int_type:
      return spawn<event_data_indexer<arithmetic_bitmap_index<Bitstream, int_type>>>(std::forward<Args>(args)...);
    case uint_type:
      return spawn<event_data_indexer<arithmetic_bitmap_index<Bitstream, uint_type>>>(std::forward<Args>(args)...);
    case double_type:
      return spawn<event_data_indexer<arithmetic_bitmap_index<Bitstream, double_type>>>(std::forward<Args>(args)...);
    case time_range_type:
      return spawn<event_data_indexer<arithmetic_bitmap_index<Bitstream, time_range_type>>>(std::forward<Args>(args)...);
    case time_point_type:
      return spawn<event_data_indexer<arithmetic_bitmap_index<Bitstream, time_point_type>>>(std::forward<Args>(args)...);
    case string_type:
      return spawn<event_data_indexer<string_bitmap_index<Bitstream>>>(std::forward<Args>(args)...);
    case address_type:
      return spawn<event_data_indexer<address_bitmap_index<Bitstream>>>(std::forward<Args>(args)...);
    case port_type:
      return spawn<event_data_indexer<port_bitmap_index<Bitstream>>>(std::forward<Args>(args)...);
  }
}

} // namespace vast

#endif
