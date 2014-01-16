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

void query::update(bitstream hits)
{
  assert(hits);

  if (hits_)
    hits_ |= hits;
  else
    hits_ = std::move(hits);

  unprocessed_ = hits_ - processed_;
}

bool query::add(cow<segment> s)
{
  return segments_.insert(s->base(), s->base() + s->events(), std::move(s));
}

void query::consolidate(size_t before, size_t after)
{
  std::vector<event_id> remove;
  auto id = current_->base();
  segments_.each(
      [&](event_id l, event_id r, cow<segment> const&)
      {
        if (l < id)
        {
          remove.push_back(l);
        }
        else if (id == l)
        {
          if (remove.size() >= before)
            remove.resize(remove.size() - before);
        }
        else if (id < r)
        {
          if (after > 0)
            --after;
          else
            remove.push_back(r);
        }
        else
        {
          assert(! "should never happen");
        }
      });

  for (auto i : remove)
  {
    VAST_LOG_DEBUG("purging segment with base " << i);
    segments_.erase(i);
  }
}

trial<size_t> query::extract()
{
  if (unprocessed_.empty())
    return error{"query has no unprocessed hits available"};

  if (! reader_)
  {
    auto last = unprocessed_.find_last();
    assert(last != bitstream::npos);

    if (auto s = segments_.lookup(last))
      current_ = *s;
    else
      return error{"query has not (yet) a segment for id " + to_string(last)};

    reader_ = make_unique<segment::reader>(&current_.read());
    VAST_LOG_DEBUG("query instantiates reader for segment " <<
                   current_->id() << " [" << current_->base() << ','
                   << current_->base() + current_->events() << ')');

    // The masked bitstream represents all unprocessed hits in the current
    // segment.
    masked_.append(current_->base(), false);
    masked_.append(current_->events(), true);
    masked_ &= unprocessed_;
  }

  if (masked_.empty())
    VAST_LOG_WARN("query has no hits for current segment " << current_->id());

  size_t n = 0;
  for (auto id : masked_)
  {
    auto e = reader_->read(id);
    if (! e)
      return error{"query failed to extract event " + to_string(id)};

    if (evaluate(ast_, *e).get<bool>())
    {
      fn_(std::move(*e));
      ++n;
    }
    else
    {
      VAST_LOG_WARN("query " << ast_ << " ignores false positive: " << *e);
    }
  }

  processed_ |= masked_;
  unprocessed_ -= masked_;
  masked_.clear();

  // We've processed this segment entirely and the next call to extract
  // should instantiate a new reader for a different segment.
  reader_.reset();

  if (n == 0)
    VAST_LOG_WARN("query could not find a single result in segment " <<
                  current_->id());

  return n;
}

std::vector<event_id> query::scan() const
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

  return eids;
}

using namespace cppa;

query_actor::query_actor(actor_ptr archive, actor_ptr sink, expr::ast ast)
  : archive_{std::move(archive)},
    sink_{std::move(sink)},
    query_{std::move(ast), [=](event e) { send(sink_, std::move(e)); }}
{
}

void query_actor::act()
{
  chaining(false);

  auto scan = [=]()
  {
    for (auto eid : query_.scan())
    {
      send(archive_, atom("segment"), eid);
      VAST_LOG_ACTOR_DEBUG("asks for segment containing id " << eid);
    }
  };

  auto extract = [=]()
  {
    auto got = query_.extract();
    if (got)
      VAST_LOG_ACTOR_DEBUG("extracted " << got.value() << " events");
    else
      VAST_LOG_ACTOR_DEBUG("extract failed: " << got.failure());
  };


  become(
      on_arg_match >> [=](bitstream const& hits)
      {
        assert(hits);
        assert(! hits.empty());

        VAST_LOG_ACTOR_DEBUG("got index hit [" << hits.find_first()
                             << ',' << hits.find_last() << ']');

        query_.update(hits);

        scan();
        extract();
      },
      on(atom("no segment"), arg_match) >> [=](event_id eid)
      {
        VAST_LOG_ACTOR_ERROR("could not obtain segment for event ID " << eid);
        quit(exit::error);
      },
      on_arg_match >> [=](segment const&)
      {
        cow<segment> s = *tuple_cast<segment>(last_dequeued());

        if (! query_.add(s))
          VAST_LOG_ACTOR_WARN("ignores duplicate segment " << s->id());
        else
          VAST_LOG_ACTOR_DEBUG(
              "added segment " << s->id() <<
              " [" << s->base() << ", " << s->base() + s->events() << ")");

        query_.consolidate();

        scan();
        extract();
      },
      others() >> [=]
      {
        VAST_LOG_ACTOR_ERROR("got unexpected message from @" <<
                             last_sender()->id() << ": " <<
                             to_string(last_dequeued()));
      });
}

char const* query_actor::description() const
{
  return "query";
}

} // namespace vast
