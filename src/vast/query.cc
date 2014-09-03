#include "vast/query.h"

#include <caf/all.hpp>
#include "vast/event.h"
#include "vast/logger.h"
#include "vast/expr/evaluator.h"
#include "vast/expr/resolver.h"

using namespace caf;

namespace vast {

query::query(actor archive, actor sink, expression ast)
  : archive_{std::move(archive)},
    sink_{std::move(sink)},
    ast_{std::move(ast)}
{
  // Prefetches the next segment. If we don't have a segment yet, we look for
  // the segment corresponding to the last unprocessed hit. If we have a
  // segment, we try to get the next segment in the ID space. If no such
  // segment exists, we try to get a segment located before the current one. If
  // neither exist, we fail.
  auto prefetch = [=]
  {
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
      VAST_LOG_ACTOR_DEBUG(
          "looks for next unprocessed ID after " <<
          current()->meta().base + current()->meta().events - 1);

      auto next = unprocessed_.find_next(current()->meta().base +
                                         current()->meta().events - 1);

      if (next != bitstream::npos)
      {
        VAST_LOG_ACTOR_DEBUG("prefetches segment for next ID " << next);
        send(archive_, next);
        inflight_ = true;
      }
      else
      {
        auto prev = unprocessed_.find_prev(current()->meta().base);
        if (prev != bitstream::npos)
        {
          VAST_LOG_ACTOR_DEBUG("prefetches segment for previous ID " << prev);
          send(archive_, prev);
          inflight_ = true;
        }
      }
    }
  };

  auto incorporate_hits = [=](bitstream const& hits)
  {
    assert(hits);

    VAST_LOG_ACTOR_DEBUG("got index hit covering [" << hits.find_first()
                         << ',' << hits.find_last() << ']');

    hits_ |= hits;
    unprocessed_ = hits_ - processed_;

    prefetch();
  };

  auto handle_progress =
    on(atom("progress"), arg_match) >> [=](double progress, uint64_t hits)
    {
      if (progress != progress_)
        send(sink_, atom("progress"), progress, hits);

      progress_ = progress;

      if (progress == 1.0)
      {
        VAST_LOG_ACTOR_DEBUG("completed index interaction (" << hits << " hits)");

        if (processed_.count() == hits)
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
          "got segment " << s.meta().id << " [" << s.meta().base <<
          ", " << s.meta().base + s.meta().events << ")");

      assert(! reader_);
      reader_ = std::make_unique<segment::reader>(*current());

      become(extracting_);

      if (requested_ > 0)
        send(this, atom("extract"));

      prefetch();
    });

  extracting_ = (
    handle_progress,
    incorporate_hits,
    on(atom("extract"), arg_match) >> [=](uint64_t n)
    {
      VAST_LOG_ACTOR_DEBUG(
          "got request to extract " << (n == 0 ? "all" : to_string(n)) <<
          " events (" << (n == 0 ? uint64_t(-1) : requested_ + n) << " total)");

      // If the query did not extract events this request, we start the
      // extraction process now.
      if (requested_ == 0)
        send(this, atom("extract"));

      requested_ = n == 0 ? -1 : requested_ + n;
    },
    on(atom("extract")) >> [=]
    {
      assert(reader_);
      assert(requested_ > 0);

      // We construct a new mask for each request, because the hits
      // continuously update in every state.
      bitstream mask = bitstream{bitstream_type{}};
      mask.append(current()->meta().base, false);
      mask.append(current()->meta().events, true);
      mask &= unprocessed_;
      assert(mask.count() > 0);

      uint64_t n = 0;
      event_id last = 0;
      for (auto id : mask)
      {
        last = id;
        auto e = reader_->read(id);
        if (e)
        {
          auto& candidate_checker = checkers_[e->type()];
          if (is<none>(candidate_checker))
            candidate_checker = visit(expr::type_resolver{e->type()}, ast_);

          if (visit(expr::evaluator{*e}, candidate_checker))
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
          if (e.empty())
            VAST_LOG_ACTOR_ERROR("failed to extract event " << id);
          else
            VAST_LOG_ACTOR_ERROR("failed to extract event " << id << ": " <<
                                 e.error());

          quit(exit::error);
          return;
        }
      }

      requested_ -= n;

      bitstream partial = bitstream{bitstream_type{last + 1, true}};
      partial &= mask;
      processed_ |= partial;
      unprocessed_ -= partial;
      mask -= partial;

      VAST_LOG_ACTOR_DEBUG("extracted " << n << " events (" <<
                           partial.count() << '/' << mask.count() <<
                           " processed/remaining hits)");

      if (! mask.all_zero())
      {
        // We continue extractining until we have processed all requested
        // events.
        if (requested_ > 0)
          send_tuple(this, last_dequeued());

        return;
      }

      reader_.reset();
      segment_ = {};

      become(inflight_ ? waiting_ : idle_);

      if (inflight_)
      {
        VAST_LOG_ACTOR_DEBUG("becomes waiting");
        become(waiting_);
      }
      else
      {
        // No segment in-flight implies no more unprocessed hits, because the
        // arrival of new hits automatically triggers prefetching.
        assert(unprocessed_.all_zero());

        VAST_LOG_ACTOR_DEBUG("becomes idle");
        become(idle_);

        if (progress_ == 1.0 && unprocessed_.count() == 0)
          send(this, atom("done"));
      }
    });
}

message_handler query::act()
{
  catch_all(false);

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
  if (segment_.empty())
    return nullptr;

  return reinterpret_cast<segment const*>(segment_.at(0));
}

} // namespace vast
