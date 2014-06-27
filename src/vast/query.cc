#include "vast/query.h"

#include <cppa/cppa.hpp>
#include "vast/event.h"
#include "vast/logger.h"

using namespace cppa;

namespace vast {


query::query(actor archive, actor sink, expr::ast ast)
  : archive_{std::move(archive)},
    sink_{std::move(sink)},
    ast_{std::move(ast)}
{
  auto incorporate_hits = [=](bitstream const& hits)
  {
    assert(hits);

    VAST_LOG_ACTOR_DEBUG("got index hit covering [" << hits.find_first()
                         << ',' << hits.find_last() << ']');

    hits_ |= hits;
    unprocessed_ = hits_ - processed_;

    if (inflight_)
      return;

    if (! current())
    {
      auto last = unprocessed_.find_last();
      if (last != bitstream::npos)
      {
        VAST_LOG_ACTOR_DEBUG("prefetches segment for ID " << last);
        send(archive_, last);
        inflight_ = true;
      }
    }
    else
    {
      auto next = unprocessed_.find_next(current()->base() +
                                         current()->events());
      if (next != bitstream::npos)
      {
        VAST_LOG_ACTOR_DEBUG("prefetches segment for ID " << next);
        send(archive_, next);
        inflight_ = true;
      }
      else
      {
        auto prev = unprocessed_.find_prev(current()->base());
        if (prev != 0 && prev != bitstream::npos)
        {
          // The case of prev == 0 may occur when we negate queries, but it's not
          // a valid event ID and we thus need to ignore it.
          VAST_LOG_ACTOR_DEBUG("prefetches segment for ID " << prev);
          send(archive_, prev);
          inflight_ = true;
        }
      }
    }
  };

  auto handle_progress =
    on(atom("progress"), arg_match) >> [=](double progress, uint64_t hits)
    {
      send(sink_, atom("progress"), progress, hits);

      if (progress == 1.0)
      {
        VAST_LOG_ACTOR_DEBUG("completed hits (" << hits << ")");
        send(this, atom("done"));
      }
    };

  idle_ = (
    handle_progress,
    [=](bitstream const& hits)
    {
      incorporate_hits(hits);

      if (inflight_)
        become(waiting_);
    },
    on(atom("done")) >> [=]
    {
      send_tuple(sink_, last_dequeued());
      quit(exit::done);
    });

  waiting_ = (
    handle_progress,
    incorporate_hits,
    [=](segment const& s)
    {
      inflight_ = false;
      segment_ = last_dequeued();

      VAST_LOG_ACTOR_DEBUG(
          "got segment " << s.id() <<
          " [" << s.base() << ", " << s.base() + s.events() << ")");

      assert(! reader_);
      reader_ = std::make_unique<segment::reader>(current());

      become(extracting_);
    });

  extracting_ = (
    handle_progress,
    incorporate_hits,
    on(atom("extract"), arg_match) >> [=](uint64_t n)
    {
      VAST_LOG_ACTOR_DEBUG("got request to extract " << n << " events (" <<
                            requested_ << " outstanding)");

      assert(n > 0);

      if (requested_ == 0)
        send(this, atom("extract"));

      requested_ += n;
    },
    on(atom("extract")) >> [=]
    {
      assert(reader_);
      assert(requested_ > 0);

      bitstream mask = bitstream{bitstream_type{}};
      mask.append(current()->base(), false);
      mask.append(current()->events(), true);
      mask &= unprocessed_;

      uint64_t n = 0;
      event_id last = 0;
      for (auto id : mask)
      {
        last = id;
        if (auto e = reader_->read(id))
        {
          if (evaluate(ast_, *e).get<bool>())
          {
            send(sink_, std::move(*e));
            if (++n == requested_)
              break;
          }
          else
          {
            VAST_LOG_ACTOR_WARN("ignores false positive : " << *e);
          }
        }
        else
        {
          VAST_LOG_ACTOR_ERROR("failed to extract event " << id);
          quit(exit::error);
          return;
        }
      }

      requested_ -= n;
      VAST_LOG_ACTOR_DEBUG("extracted " << n << " events");

      bitstream partial = bitstream{bitstream_type{last + 1, true}};
      partial &= mask;
      processed_ |= partial;
      unprocessed_ -= partial;
      mask -= partial;

      if (mask.find_first() != bitstream::npos)
      {
        if (requested_ > 0)
          send_tuple(this, last_dequeued());

        return;
      }

      reader_.reset();

      auto more = unprocessed_.find_first() == bitstream::npos;
      become(more ? idle_ : waiting_);

      VAST_LOG_ACTOR_DEBUG("switches state to " << (more ? "idle" : "waiting"));
    });
}

partial_function query::act()
{
  attach_functor(
      [=](uint32_t)
      {
        archive_ = invalid_actor;
        sink_ = invalid_actor;
      });

  return idle_;
}

std::string query::describe() const
{
  return "query";
}

segment const* query::current() const
{
  return segment_.empty() ? nullptr : &get<0>(*tuple_cast<segment>(segment_));
}

} // namespace vast
