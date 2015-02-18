#include "vast/actor/query.h"

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
  trap_exit(false);
  trap_unexpected(false);
  attach_functor(
    [=](uint32_t)
    {
      archive_ = invalid_actor;
      sink_ = invalid_actor;
    });

  auto incorporate_hits = [=](bitstream_type const& hits)
  {
    VAST_DEBUG(this, "got index hit covering", '[' << hits.find_first() << ','
               << (hits.find_last() + 1) << ')');
    assert(! hits.all_zeros());
    hits_ |= hits;
    unprocessed_ = hits_ - processed_;
    prefetch();
  };

  auto handle_progress =
    [=](progress_atom, uint64_t remaining, uint64_t total)
    {
      assert(last_sender() == task_->address());
      progress_ = (total - double(remaining)) / total;
      send(sink_, progress_atom::value, progress_);
    };

  idle_ = {
    handle_progress,
    [=](actor const& task)
    {
      VAST_TRACE(this, "received task from index");
      send(task, subscriber_atom::value, this);
      task_ = task;
    },
    [=](bitstream_type const& hits)
    {
      incorporate_hits(hits);
      if (inflight_)
        become(waiting_);
    },
    [=](done_atom)
    {
      assert(last_sender() == this);
      auto runtime = time::snapshot() - start_time_;
      send(sink_, done_atom::value, runtime);
      VAST_INFO(this, "took", runtime, "to answer query:", ast_);
      quit(exit::done);
    },
    [=](done_atom, time::extent runtime, expression const&) // from INDEX
    {
      VAST_VERBOSE(this, "completed index interaction in", runtime);
      send(this, done_atom::value);
    }};

  waiting_ = {
    handle_progress,
    incorporate_hits,
    [=](chunk const& chk)
    {
      VAST_DEBUG(this, "got chunk [" << chk.base() << ',' <<
                 (chk.base() + chk.events()) << ")");
      inflight_ = false;
      chunk_ = chk;
      assert(! reader_);
      reader_ = std::make_unique<chunk::reader>(chunk_);
      become(extracting_);
      if (requested_ > 0)
        send(this, extract_atom::value);
      prefetch();
    }};

  extracting_ = {
    handle_progress,
    incorporate_hits,
    [=](extract_atom, uint64_t n)
    {
      VAST_DEBUG(this, "got request to extract",
                 (n == 0 ? "all" : to_string(n)),
                 "events (" << (n == 0 ? uint64_t(-1) : requested_ + n),
                 " total)");
      // If the query did not extract events this request, we start the
      // extraction process now.
      if (requested_ == 0)
        send(this, extract_atom::value);
      requested_ = n == 0 ? -1 : requested_ + n;
    },
    [=](extract_atom)
    {
      VAST_DEBUG(this, "extracts events (" << requested_, "requested)");
      assert(reader_);
      assert(requested_ > 0);
      // We construct a new mask for each extraction request, because hits may
      // continuously update in every state.
      bitstream_type mask{chunk_.meta().ids};
      mask &= unprocessed_;
      assert(mask.count() > 0);
      // Go through the current chunk and perform a candidate check for a hit,
      // and relay the event to the sink on success.
      uint64_t n = 0;
      event_id last = 0;
      for (auto id : mask)
      {
        last = id;
        auto e = reader_->read(id);
        if (e)
        {
          auto& ast = expressions_[e->type()];
          if (is<none>(ast))
          {
            auto t = visit(expr::schema_resolver{e->type()}, ast_);
            if (! t)
            {
              VAST_ERROR(this, "failed to resolve", ast_ << ',', t.error());
              quit(exit::error);
              return;
            }
            ast = visit(expr::type_resolver{e->type()}, *t);
            VAST_DEBUG(this, "resolved AST for type", e->type() << ':', ast);
          }
          if (visit(expr::event_evaluator{*e}, ast))
          {
            send(sink_, std::move(*e));
            if (++n == requested_)
              break;
          }
          else
          {
            VAST_WARN(this, "ignores false positive:", *e);
          }
        }
        else
        {
          if (e.empty())
            VAST_ERROR(this, "failed to extract event", id);
          else
            VAST_ERROR(this, "failed to extract event", id << ':', e.error());
          quit(exit::error);
          return;
        }
      }
      requested_ -= n;
      bitstream_type partial{last + 1, true};
      partial &= mask;
      processed_ |= partial;
      unprocessed_ -= partial;
      mask -= partial;
      VAST_DEBUG(this, "extracted", n, "events " << partial.count() << '/'
                 << mask.count(), "processed/remaining hits)");
      assert(! mask.empty());
      if (! mask.all_zeros())
      {
        // We continue extracting until we have processed all requested
        // events.
        if (requested_ > 0)
          send(this, last_dequeued());
        return;
      }
      reader_.reset();
      chunk_ = {};
      become(inflight_ ? waiting_ : idle_);
      if (inflight_)
      {
        VAST_DEBUG(this, "becomes waiting");
        become(waiting_);
      }
      else
      {
        // No in-flight chunk implies that we have no more unprocessed hits,
        // because arrival of new hits automatically triggers prefetching.
        assert(! unprocessed_.empty());
        assert(unprocessed_.all_zeros());
        VAST_DEBUG(this, "becomes idle");
        become(idle_);
        if (progress_ == 1.0 && unprocessed_.count() == 0)
          send(this, done_atom::value);
      }
    }};
}

message_handler query::make_handler()
{
  start_time_ = time::snapshot();
  return idle_;
}

std::string query::name() const
{
  return "query";
}

void query::prefetch()
{
  if (inflight_)
    return;
  if (chunk_.events() == 0)
  {
    auto last = unprocessed_.find_last();
    if (last != bitstream_type::npos)
    {
      VAST_DEBUG(this, "prefetches chunk for ID", last);
      send(archive_, last);
      inflight_ = true;
    }
  }
  else
  {
    VAST_DEBUG(this, "looks for next unprocessed ID after",
               chunk_.meta().ids.find_last());
    auto next = unprocessed_.find_next(chunk_.meta().ids.find_last());
    if (next != bitstream_type::npos)
    {
      VAST_DEBUG(this, "prefetches chunk for next ID", next);
      send(archive_, next);
      inflight_ = true;
    }
    else
    {
      auto prev = unprocessed_.find_prev(chunk_.meta().ids.find_first());
      if (prev != bitstream::npos)
      {
        VAST_DEBUG(this, "prefetches chunk for previous ID", prev);
        send(archive_, prev);
        inflight_ = true;
      }
    }
  }
}


} // namespace vast
