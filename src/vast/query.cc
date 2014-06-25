#include "vast/query.h"

#include <cppa/cppa.hpp>
#include "vast/event.h"
#include "vast/logger.h"

namespace vast {

query::query(expr::ast ast, std::function<void(event)> fn)
  : ast_{std::move(ast)},
    fn_{fn},
    processed_{bitstream_type{}},
    unprocessed_{bitstream_type{}},
    masked_{bitstream_type{}}
{
}

query::query_state query::state() const
{
  return state_;
}

size_t query::segments() const
{
  return segments_.size();
}

std::vector<event_id> query::next() const
{
  std::vector<event_id> eids;
  if (segments_.empty())
  {
    if (auto id = unprocessed_.find_last())
      if (id != bitstream::npos)
        eids.push_back(id);
  }
  else
  {
    segments_.each(
        [&](event_id l, event_id r, cow<segment> const&)
        {
          // 0 may occur when results get bitwise flipped.
          auto prev = unprocessed_.find_prev(l);
          if (prev != 0 && prev != bitstream::npos && ! segments_.lookup(prev))
            eids.push_back(prev);

          auto next = unprocessed_.find_next(r);
          if (next != bitstream::npos && ! segments_.lookup(next))
            eids.push_back(next);
        });
  }

  std::reverse(eids.begin(), eids.end());

  return eids;
}

void query::add(bitstream hits)
{
  assert(hits);

  if (hits_)
    hits_ |= hits;
  else
    hits_ = std::move(hits);

  unprocessed_ = hits_ - processed_;

  if (unprocessed_.empty())
    state_ = idle;
  else if (state_ == idle)
    state_ = waiting;
}

bool query::add(cow<segment> s)
{
  auto ok = segments_.insert(s->base(), s->base() + s->events(), std::move(s));
  if (! ok)
    return false;

  if (state_ == waiting && instantiate())
    state_ = ready;

  return true;
}

trial<uint64_t> query::extract(uint64_t max)
{
  assert(state_ == ready || state_ == extracting);

  state_ = extracting;

  uint64_t n = 0;
  for (auto id : masked_)
  {
    auto e = reader_->read(id);
    if (! e)
    {
      state_ = failed;
      return error{"query failed to extract event " + to_string(id)};
    }

    if (evaluate(ast_, *e).get<bool>())
    {
      fn_(std::move(*e));
      if (++n == max && id != masked_.find_last())
      {
        bitstream partial = bitstream_type{id + 1, true};
        partial &= masked_;
        processed_ |= partial;
        unprocessed_ -= partial;
        masked_ -= partial;

        return max;
      }
    }
    else
    {
      VAST_LOG_WARN("query " << ast_ << " ignores false positive: " << *e);
    }
  }

  if (n == 0)
    VAST_LOG_WARN("query could not find a single result in segment " <<
                  current_->id());

  processed_ |= masked_;
  unprocessed_ -= masked_;
  masked_.clear();
  reader_.reset();

  if (segments_.erase(current_->base()))
  {
    VAST_LOG_DEBUG("query removed segment with base " << current_->base());
  }
  else
  {
    state_ = failed;
    return error{"failed to remove fully processed segment with base " +
                 to_string(current_->base())};
  }

  // We technically could also instantiate a prefetched segment at this point,
  // but doing so avoids the ability to prefetch another one.
  if (unprocessed_.find_first() == bitstream::npos)
    state_ = finishing_ ? done : idle;
  else if (instantiate())
    state_ = ready;
  else
    state_ = waiting;

  return n;
}

void query::finish()
{
  finishing_ = true;

  if (unprocessed_.find_first() == bitstream::npos && state_ == idle)
    state_ = done;
}

bool query::instantiate()
{
  assert(state_ == waiting || state_ == extracting);
  assert(! unprocessed_.empty());
  assert(! reader_);

  auto last = unprocessed_.find_last();

  if (auto s = segments_.lookup(last))
    current_ = *s;
  else
    return false;

  reader_ = std::make_unique<segment::reader>(&current_.read());
  VAST_LOG_DEBUG("query instantiates reader for segment " <<
                 current_->id() << " [" << current_->base() << ','
                 << current_->base() + current_->events() << ')');

  masked_.append(current_->base(), false);
  masked_.append(current_->events(), true);
  masked_ &= unprocessed_;

  assert(! masked_.empty());

  return true;
}


using namespace cppa;

query_actor::query_actor(actor archive, actor sink, expr::ast ast)
  : archive_{std::move(archive)},
    sink_{std::move(sink)},
    query_{std::move(ast), [=](event e) { send(sink_, std::move(e)); }}
{
}

void query_actor::prefetch(size_t max)
{
  // Don't count the instantiated segment.
  if (query_.state() == query::ready || query_.state() == query::extracting)
    ++max;

  auto segments = query_.segments();
  auto ids = query_.next();
  if (segments >= max)
    return;

  ids.resize(std::min(ids.size(), max - segments));
  for (auto i : ids)
  {
    if (std::find(inflight_.begin(), inflight_.end(), i) == inflight_.end())
    {
      inflight_.push_back(i);
      send(archive_, i);
      VAST_LOG_ACTOR_DEBUG("asks for segment containing id " << i);
    }
  }
}

void query_actor::extract(uint64_t n)
{
  auto s = query_.state();

  if (s == query::idle)
  {
    VAST_LOG_ACTOR_DEBUG("ignores extraction request in idle state");
  }
  else if (s == query::waiting)
  {
    VAST_LOG_ACTOR_DEBUG("scans for new segments");
    prefetch(prefetch_);
  }
  else if (s == query::done)
  {
    VAST_LOG_ACTOR_DEBUG("completed processing");
    send(sink_, atom("done"));
    quit(exit::done);
  }
  else if (s == query::failed)
  {
    send(sink_, atom("failed"));
    quit(exit::error);
  }
  else
  {
    assert(s == query::ready || s == query::extracting);

    auto t = query_.extract(n);
    if (! t)
    {
      VAST_LOG_ACTOR_ERROR(t.error().msg());
      quit(exit::error);
      return;
    }

    VAST_LOG_ACTOR_DEBUG("extracted " << *t << " events");

    // When we finished processing a segment we remove it. If we have more
    // segments left, we land back in "ready" state. At this point we check
    // again because we may continue extracting for a while.
    if (query_.state() == query::ready)
      prefetch(prefetch_);

    requested_ -= std::min(requested_, *t);
    if (n > *t)
      extract(n - *t);
  }
}

behavior query_actor::act()
{
  attach_functor(
      [=](uint32_t)
      {
        archive_ = invalid_actor;
        sink_ = invalid_actor;
      });

  return
  {
    on(atom("progress"), arg_match) >> [=](double progress, uint64_t hits)
    {
      send(sink_, atom("progress"), progress, hits);

      if (progress == 1.0)
      {
        VAST_LOG_ACTOR_DEBUG("completed index interaction with "
                             << hits << " hits");

        query_.finish();
        if (query_.state() == query::done)
        {
          send(sink_, atom("done"));
          quit(exit::done);
        }
      }
    },
    [=](bitstream const& hits)
    {
      assert(hits);
      assert(! hits.empty());
      assert(hits.find_first() != bitstream::npos);

      VAST_LOG_ACTOR_DEBUG("got index hit covering [" << hits.find_first()
                           << ',' << hits.find_last() << ']');

      query_.add(hits);

      if (query_.state() == query::waiting)
        prefetch(prefetch_);
    },
    [=](segment const&)
    {
      cow<segment> s = *tuple_cast<segment>(last_dequeued());

      auto i = std::remove_if(
          inflight_.begin(),
          inflight_.end(),
          [&s](event_id eid) { return s->contains(eid); });

      inflight_.erase(i, inflight_.end());

      if (! query_.add(s))
        VAST_LOG_ACTOR_WARN("ignores duplicate segment " << s->id());
      else
        VAST_LOG_ACTOR_DEBUG(
            "added segment " << s->id() <<
            " [" << s->base() << ", " << s->base() + s->events() << ")");

      prefetch(prefetch_);

      if (requested_ > 0 && query_.state() == query::ready)
        extract(requested_);
    },
    on(atom("1st batch"), arg_match) >> [=](uint64_t n)
    {
      VAST_LOG_ACTOR_DEBUG("sets size of first batch to " << n);
      requested_ = n;
    },
    on(atom("extract"), arg_match) >> [=](uint64_t n)
    {
      requested_ += n;
      extract(n);
    },
    on(atom("no segment"), arg_match) >> [=](event_id eid)
    {
      VAST_LOG_ACTOR_ERROR("could not obtain segment for event ID " << eid);
      quit(exit::error);
    },
    others() >> [=]
    {
      VAST_LOG_ACTOR_ERROR("got unexpected message from " <<
                           last_sender() << ": " << to_string(last_dequeued()));
    }
  };
}

std::string query_actor::describe() const
{
  return "query";
}

} // namespace vast
