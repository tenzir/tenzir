#ifndef VAST_INDEXER_H
#define VAST_INDEXER_H

#include <caf/all.hpp>
#include "vast/actor.h"
#include "vast/bitmap_index.h"
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
class indexer : public actor_mixin<indexer<Derived, BitmapIndex>, sentinel>
{
public:
  /// Spawns a bitmap indexer.
  /// @param path The absolute file path on the file system.
  /// @param bmi The bitmap index.
  indexer(path path, BitmapIndex bmi = {})
    : path_{std::move(path)},
      bmi_{std::move(bmi)},
      stats_{std::chrono::seconds{1}}
  {
  }

  caf::message_handler make_handler()
  {
    using namespace caf;

    this->trap_exit(true);

    if (exists(path_))
    {
      auto attempt = io::unarchive(path_, last_flush_, bmi_);
      if (! attempt)
        VAST_LOG_ACTOR_ERROR("failed to load bitmap index from " << path_ <<
                             ": " << attempt.error());
      else
        VAST_LOG_ACTOR_DEBUG("loaded bitmap index from " << path_ <<
                             " (" << bmi_.size() << " bits)");
    }

    auto flush = [=]
    {
      auto size = static_cast<decltype(last_flush_)>(bmi_.size());
      if (size > last_flush_)
      {
        auto attempt = io::archive(path_, size, bmi_);
        if (! attempt)
        {
          VAST_LOG_ACTOR_ERROR("failed to flush " << (size - last_flush_) <<
                               " bits to " << path_ << ": " <<
                               attempt.error());
          this->quit(exit::error);
        }
        else
        {
          VAST_LOG_ACTOR_DEBUG(
              "flushed bitmap index to " << path_ << " (" <<
              (size - last_flush_) << '/' << size << " new/total bits)");

          last_flush_ = size;
        }
      }
    };

    this->attach_functor(
        [=](uint32_t reason)
        {
          if (reason != exit::kill)
            flush();
        });

    return
    {
      [=](exit_msg const& e)
      {
        this->quit(e.reason);
      },
      on(atom("flush"), arg_match) >> [=](actor task_tree)
      {
        flush();
        this->send(task_tree, atom("done"));
      },
      [=](std::vector<event> const& events)
      {
        uint64_t n = 0;
        uint64_t total = events.size();
        for (auto& e : events)
        {
          auto t = static_cast<Derived*>(this)->append(bmi_, e);
          if (t)
            ++n;
          else
            VAST_LOG_ACTOR_ERROR("failed to append event " << e.id() << ": " <<
                                 t.error());
        }

        stats_.increment(n);

        return make_message(total, n, stats_.last(), stats_.mean());
      },
      [=](expression const& pred, uuid const& part, actor sink)
      {
        auto p = get<predicate>(pred);
        assert(p);

        auto r = bmi_.lookup(p->op, *get<data>(p->rhs));
        if (! r)
        {
          VAST_LOG_ACTOR_ERROR(r.error());
          this->send(sink, pred, part, bitstream{});
          return;
        }

        this->send(sink, pred, part, bitstream{std::move(*r)});
      }
    };
  }

private:
  path const path_;
  BitmapIndex bmi_;
  uint64_t last_flush_ = 1;
  util::rate_accumulator<uint64_t> stats_;
};

template <typename Bitstream>
struct event_name_indexer
  : indexer<
      event_name_indexer<Bitstream>,
      string_bitmap_index<Bitstream>
    >
{
  using indexer<
    event_name_indexer<Bitstream>,
    string_bitmap_index<Bitstream>
  >::indexer;

  template <typename BitmapIndex>
  trial<void> append(BitmapIndex& bmi, event const& e)
  {
    if (bmi.push_back(e.type().name(), e.id()))
      return nothing;
    else
      return error{"failed to append event name: ", e.type().name()};
  }

  std::string name() const
  {
    return "name-bitmap-indexer";
  }
};

template <typename Bitstream>
struct event_time_indexer
  : indexer<
      event_time_indexer<Bitstream>,
      arithmetic_bitmap_index<Bitstream, time_point>
    >
{
  using indexer<
    event_time_indexer<Bitstream>,
    arithmetic_bitmap_index<Bitstream, time_point>
  >::indexer;

  template <typename BitmapIndex>
  trial<void> append(BitmapIndex& bmi, event const& e)
  {
    if (bmi.push_back(e.timestamp(), e.id()))
      return nothing;
    else
      return error{"failed to append event timestamp: ", e.timestamp()};
  }

  std::string name() const
  {
    return "time-bitmap-indexer";
  }
};

template <typename BitmapIndex>
struct event_data_indexer
  : indexer<event_data_indexer<BitmapIndex>, BitmapIndex>
{
  using super = indexer<event_data_indexer<BitmapIndex>, BitmapIndex>;

  event_data_indexer(type t, path p, offset o, BitmapIndex bmi = {})
    : super{std::move(p), std::move(bmi)},
      event_type_{std::move(t)},
      offset_{std::move(o)}
  {
  }

  trial<void> append(BitmapIndex& bmi, event const& e)
  {
    // Bail out if we're not responsible.
    if (e.type() != event_type_)
      return nothing;

    auto r = get<record>(e);
    if (! r)
    {
      assert(offset_.empty());
      if (bmi.push_back(e.data(), e.id()))
        return nothing;
      else
        return error{"push_back failed for ", e.data(), ", id", e.id()};
    }

    if (auto d = r->at(offset_))
    {
      if (bmi.push_back(*d, e.id()))
        return nothing;
      else
        return error{"push_back failed for ", *d, ", id", e.id()};
    }
    else
    {
      // If there is no data at a given offset, it means that an intermediate
      // record is nil but we're trying to access a deeper field.
      if (bmi.push_back(nil, e.id()))
        return nothing;
      else
        return error{"push_back failed for nil, id ", e.id()};
    }
  }

  std::string name() const
  {
    return "data-bitmap-indexer(" + to_string(offset_) + ')';
  }

  type event_type_;
  offset offset_;
};

namespace detail {

template <typename Bitstream>
struct event_data_index_factory
{
  event_data_index_factory(path const& p, offset const& o, type const& t)
    : path_{p},
      off_{o},
      event_type_{t}
  {
  }

  template <typename T>
  trial<caf::actor> operator()(T const&) const
  {
    return spawn<arithmetic_bitmap_index<Bitstream, type::to_data<T>>>();
  }

  trial<caf::actor> operator()(type::address const&) const
  {
    return spawn<address_bitmap_index<Bitstream>>();
  }

  trial<caf::actor> operator()(type::subnet const&) const
  {
    return spawn<subnet_bitmap_index<Bitstream>>();
  }

  trial<caf::actor> operator()(type::port const&) const
  {
    return spawn<port_bitmap_index<Bitstream>>();
  }

  trial<caf::actor> operator()(type::string const&) const
  {
    return spawn<string_bitmap_index<Bitstream>>();
  }

  trial<caf::actor> operator()(type::enumeration const&) const
  {
    return spawn<string_bitmap_index<Bitstream>>();
  }

  trial<caf::actor> operator()(type::vector const& t) const
  {
    return spawn<sequence_bitmap_index<Bitstream>>(t.elem());
  }

  trial<caf::actor> operator()(type::set const& t) const
  {
    return spawn<sequence_bitmap_index<Bitstream>>(t.elem());
  }

  trial<caf::actor> operator()(none const&) const
  {
    return error{"bitmap index for invalid type not supported"};
  }

  trial<caf::actor> operator()(type::pattern const&) const
  {
    return error{"regular expressions not yet supported"};
  }

  trial<caf::actor> operator()(type::table const&) const
  {
    return error{"tables not yet supported"};
  }

  trial<caf::actor> operator()(type::record const&) const
  {
    return error{"records shall be unrolled"};
  }

  trial<caf::actor> operator()(type::alias const& a) const
  {
    return visit(*this, a.type());
  }

  template <typename BitmapIndex, typename... Args>
  caf::actor spawn(Args&&... args) const
  {
    using indexer_type = event_data_indexer<BitmapIndex>;
    return caf::spawn<indexer_type>(
        event_type_, path_, off_, BitmapIndex{std::forward<Args>(args)...});
  }

  path const& path_;
  offset const& off_;
  type const& event_type_;
};

} // namespace detail

/// Factory to construct an indexer based on a given type.
template <typename Bitstream>
trial<caf::actor>
make_event_data_indexer(path const& p, type const& et, type const& t,
                        offset const& o)
{
  return visit(detail::event_data_index_factory<Bitstream>{p, o, et}, t);
}

} // namespace vast

#endif
