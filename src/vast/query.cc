#include "vast/query.h"

#include <cppa/cppa.hpp>
#include "vast/event.h"
#include "vast/logger.h"

namespace vast {

namespace {

struct cow_segment_less_than
{
  bool operator()(cow<segment> const& x, cow<segment> const& y) const
  {
    return x->base() < y->base();
  }
};

cow_segment_less_than segment_lt;

} // namespace <anonymous>

query::query(expr::ast ast, std::function<void(event)> fn)
  : ast_{std::move(ast)},
    fn_{fn}
{
  // FIXME: Use something better than a null bitstream.
  processed_ = null_bitstream{};
  extracted_ = null_bitstream{};
}

void query::update(bitstream result)
{
  assert(result);

  if (hits_)
    hits_ |= result;
  else
    hits_ = std::move(result);

  unprocessed_ = hits_ - processed_;
}

bool query::add(cow<segment> s)
{
  auto i = std::lower_bound(segments_.begin(), segments_.end(), s, segment_lt);
  if (! (i == segments_.end() || segment_lt(s, *i)))
    return false;

  segments_.emplace(i, s);
  return true;
}

size_t query::consolidate(size_t before, size_t after)
{
  auto i = std::lower_bound(
      segments_.begin(), segments_.end(), current_, segment_lt);

  assert(i != segments_.end());
  size_t earlier = std::distance(segments_.begin(), i);
  size_t later = std::distance(i, segments_.end()) - 1;
  size_t n = 0;

  if (earlier > before)
  {
    auto purge = earlier - before;
    VAST_LOG_DEBUG("query removes " << purge << " segments before current");
    segments_.erase(segments_.begin(), segments_.begin() + purge);
    n = purge;
  }

  if (later > after)
  {
    auto purge = later - after;
    VAST_LOG_DEBUG("query removes " << purge << " segments after current");
    segments_.erase(segments_.end() - purge, segments_.end());
    n += purge;
  }

  return n;
}

optional<size_t> query::extract()
{
  if (! unprocessed_)
    return {};

  if (! reader_ || ! current_->contains(reader_->position()))
  {
    if (! extracted_.empty())
    {
      processed_ |= extracted_;
      unprocessed_ -= extracted_;
      extracted_ = null_bitstream{};
    }

    auto last = unprocessed_.find_last();
    if (last == bitstream::npos)
      return {};

    auto found = false;
    for (auto i = segments_.rbegin(); i != segments_.rend(); ++i)
    {
      if ((**i).contains(last))
      {
        found = true;
        current_ = *i;
        break;
      }
    }
    if (! found)
    {
      VAST_LOG_DEBUG("query has no segment for id " << last << " available");
      return {};
    }

    reader_ = make_unique<segment::reader>(&current_.read());
    VAST_LOG_DEBUG("query attached reader to segment " << current_->id());
  }

  size_t results = 0;
  auto n = reader_->extract_forward(
      unprocessed_,
      [&](event e)
      {
        extracted_.append(e.id() - extracted_.size(), false);
        extracted_.append(1, true);
        if (evaluate(ast_, e).get<bool>())
        {
          fn_(std::move(e));
          ++results;
        }
        else
        {
          VAST_LOG_WARN("query ignores false positive " << ast_ << ": " << e);
        }
      });

  if (! n)
  {
    VAST_LOG_DEBUG("query finished extraction (" << results << ')');
    if (results > 0)
      return results;
    return {};
  }
  else if (*n == 0)
  {
    VAST_LOG_WARN("query could not extract a single result from segment " <<
                  current_->id());
    return 0;
  }
  else
  {
    return results;
  }
}

std::vector<event_id> query::scan() const
{
  std::vector<event_id> eids;
  for (auto i = segments_.begin(); i != segments_.end(); ++i)
  {
    auto& s = *i;
    auto prev_hit = unprocessed_.find_prev(s->base());
    if (prev_hit != bitstream::npos
        && ! ((i != segments_.begin() && (*(i - 1))->contains(prev_hit))))
      eids.push_back(prev_hit - 1);

    auto next_hit = unprocessed_.find_next(s->base() + s->events());
    if (next_hit != bitstream::npos
        && ! (i + 1 != segments_.end() && (*(i + 1))->contains(next_hit)))
      eids.push_back(next_hit - 1);
  }

  return eids;
}

size_t query::segments() const
{
  return segments_.size();
}

optional<event_id> query::last() const
{
  auto id = unprocessed_.find_last();
  if (id != bitstream::npos)
    return id;
  return {};
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
  become(
      on_arg_match >> [=](bitstream const& bs)
      {
        assert(bs);
        cow<bitstream> cbs = *tuple_cast<bitstream>(last_dequeued());
        VAST_LOG_ACTOR_DEBUG("got new result starting at " <<
                             (cbs->empty() ? 0 : cbs->find_first()));
        query_.update(*cbs);

        send(self, atom("fetch"));
        send(self, atom("extract"));
      },
      on_arg_match >> [=](event_id eid)
      {
        VAST_LOG_ACTOR_ERROR("could not obtain segment for event ID " << eid);
        quit(exit::error);
      },
      on_arg_match >> [=](segment const&)
      {
        cow<segment> s = *tuple_cast<segment>(last_dequeued());
        VAST_LOG_ACTOR_DEBUG("adds segment " << s->id());
        if (! query_.add(s))
          VAST_LOG_ACTOR_WARN("ignores duplicate segment " << s->id());

        auto n = query_.consolidate();
        if (n > 0)
          VAST_LOG_ACTOR_DEBUG("purged " << n << " segments");

        send(self, atom("fetch"));
        send(self, atom("extract"));
      },
      on(atom("fetch")) >> [=]
      {
        if (query_.segments() == 0)
        {
          if (auto last = query_.last())
          {
            VAST_LOG_ACTOR_DEBUG("asks for segment containing id " << *last);
            send(archive_, atom("segment"), *last);
          }
        }
        else if (query_.segments() <= 3) // TODO: Make configurable.
        {
          for (auto eid : query_.scan())
          {
            VAST_LOG_ACTOR_DEBUG("asks for segment containing id " << eid);
            send(archive_, atom("segment"), eid);
          }
        }
      },
      on(atom("extract")) >> [=]
      {
        VAST_LOG_ACTOR_DEBUG("asked to extract events");
        if (auto got = query_.extract())
        {
          VAST_LOG_ACTOR_DEBUG("extracted " << *got << " events");
          if (*got == 0)
            self << last_dequeued();
        }
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
