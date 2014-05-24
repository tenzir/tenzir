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
class bitmap_indexer : public actor_base
{
public:
  /// Spawns a bitmap indexer.
  /// @param path The absolute file path on the file system.
  /// @param bmi The bitmap index.
  bitmap_indexer(path path, BitmapIndex bmi = {})
    : path_{std::move(path)},
      bmi_{std::move(bmi)},
      stats_{std::chrono::seconds{1}}
  {
    bmi_.stretch(1); // Event ID 0 is not a valid event.
  }

  cppa::behavior act() final
  {
    using namespace cppa;

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
          quit(exit::error);
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

    attach_functor(
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
      on(atom("flush")) >> flush,
      [=](std::vector<event> const& events)
      {
        uint64_t n = 0;
        uint64_t total = events.size();
        for (auto& e : events)
          if (auto v = static_cast<Derived*>(this)->extract(e))
          {
            if (bmi_.push_back(*v, e.id()))
              ++n;
            else
              VAST_LOG_ACTOR_ERROR("failed to append value: " << *v);
          }

        stats_.increment(n);

        return make_any_tuple(total, n, stats_.last(), stats_.mean());
      },
      [=](expr::ast const& pred, uuid const& part, actor sink)
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
          VAST_LOG_ACTOR_ERROR(r.error());
          send(sink, pred, part, bitstream{});
          return;
        }

        send(sink, pred, part, std::move(*r));
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

  std::string describe() const final
  {
    return "name-bitmap-indexer";
  }
};

template <typename Bitstream>
struct event_time_indexer
  : bitmap_indexer<
      event_time_indexer<Bitstream>,
      arithmetic_bitmap_index<Bitstream, time_point_value>
    >
{
  using bitmap_indexer<
    event_time_indexer<Bitstream>,
    arithmetic_bitmap_index<Bitstream, time_point_value>
  >::bitmap_indexer;

  time_point const* extract(event const& e)
  {
    timestamp_ = e.timestamp();
    return &timestamp_;
  }

  std::string describe() const final
  {
    return "time-bitmap-indexer";
  }

  time_point timestamp_;
};

template <typename BitmapIndex>
struct event_data_indexer
  : bitmap_indexer<event_data_indexer<BitmapIndex>, BitmapIndex>
{
  using super = bitmap_indexer<event_data_indexer<BitmapIndex>, BitmapIndex>;

  event_data_indexer(path p, type_const_ptr t, offset o, BitmapIndex bmi = {})
    : super{std::move(p), std::move(bmi)},
      type_{std::move(t)},
      offset_{std::move(o)}
  {
  }

  value const* extract(event const& e) const
  {
    return e.name() == type_->name() ? e.at(offset_) : nullptr;
  }

  std::string describe() const final
  {
    return "data-bitmap-indexer("
      + to_string(*type_) + ':' + to_string(offset_) + ')';
  }

  type_const_ptr type_;
  offset offset_;
};

namespace detail {

template <typename Bitstream>
struct event_data_index_factory
{
  event_data_index_factory(path const& p, type_const_ptr const& t,
                           offset const& o)
    : path_{p},
      type_{t},
      off_{o}
  {
  }

  template <typename T>
  trial<cppa::actor> operator()(T const&) const
  {
    using bmi_t = arithmetic_bitmap_index<Bitstream, to_type_tag<T>::value>;
    return spawn<bmi_t>();
  }

  trial<cppa::actor> operator()(address_type const&) const
  {
    return spawn<address_bitmap_index<Bitstream>>();
  }

  trial<cppa::actor> operator()(port_type const&) const
  {
    return spawn<port_bitmap_index<Bitstream>>();
  }

  trial<cppa::actor> operator()(string_type const&) const
  {
    return spawn<string_bitmap_index<Bitstream>>();
  }

  trial<cppa::actor> operator()(enum_type const&) const
  {
    return spawn<string_bitmap_index<Bitstream>>();
  }

  trial<cppa::actor> operator()(set_type const& t) const
  {
    return spawn<sequence_bitmap_index<Bitstream>>(t.elem_type->tag());
  }

  trial<cppa::actor> operator()(vector_type const& t) const
  {
    return spawn<sequence_bitmap_index<Bitstream>>(t.elem_type->tag());
  }

  trial<cppa::actor> operator()(invalid_type const&) const
  {
    return error{"bitmap index for invalid type not supported"};
  }

  trial<cppa::actor> operator()(regex_type const&) const
  {
    return error{"regular expressions not yet supported"};
  }

  trial<cppa::actor> operator()(prefix_type const&) const
  {
    return error{"prefixes not yet supported"};
  }

  trial<cppa::actor> operator()(table_type const&) const
  {
    return error{"tables not yet supported"};
  }

  trial<cppa::actor> operator()(record_type const&) const
  {
    return error{"records should be unrolled"};
  }

  template <typename BitmapIndex, typename... Args>
  cppa::actor spawn(Args&&... args) const
  {
    using indexer_type = event_data_indexer<BitmapIndex>;

    return cppa::spawn<indexer_type>(path_, type_, off_,
                                     BitmapIndex{std::forward<Args>(args)...});
  }

  path const& path_;
  type_const_ptr const& type_;
  offset const& off_;
};

} // namespace detail

/// Factory to construct an indexer based on a given type.
template <typename Bitstream>
trial<cppa::actor>
make_event_data_indexer(path const& p, type_const_ptr const& t, offset const& o)
{
  detail::event_data_index_factory<Bitstream> v{p, t, o};

  auto inner = t->at(o);
  if (! inner)
    return error{"no type at offset " + to_string(o) + " in type " +
                 to_string(*inner)};

  return apply_visitor(v, inner->info());
}

} // namespace vast

#endif
