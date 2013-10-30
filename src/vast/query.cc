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
}

void query::update(bitstream result)
{
  assert(result);

  if (hits_)
    hits_ |= result;
  else
    hits_ = std::move(result);

  unprocessed_ = hits_ - processed_;
  cursor_ = unprocessed_.find_first(); // TODO: make controllable via policy.

  VAST_LOG_DEBUG("query adjusted cursor to " << cursor_);
}

bool query::add(cow<segment> s)
{
  auto i = std::lower_bound(segments_.begin(), segments_.end(), s, segment_lt);
  if (i == segments_.end() || segment_lt(s, *i))
    i = segments_.emplace(i, s);
  else
    return false;

  if (! reader_)
  {
    current_ = s;
    reader_ = make_unique<segment::reader>(&current_.read());
  }

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
    VAST_LOG_DEBUG("query removes " << purge << " segments before cursor");
    segments_.erase(segments_.begin(), segments_.begin() + purge);
    n = purge;
  }

  if (later > after)
  {
    auto purge = later - after;
    VAST_LOG_DEBUG("query removes " << purge << " segments after cursor");
    segments_.erase(segments_.end() - purge, segments_.end());
    n += purge;
  }

  return n;
}

size_t query::extract(size_t n)
{
  event e;
  size_t results = 0;
  while (reader_ && reader_->seek(cursor_) && reader_->read(e))
  {
    processed_.append(e.id() - processed_.size(), false);
    processed_.append(1, true);

    cursor_ = unprocessed_.find_next(cursor_);
    if (! current_->contains(cursor_))
    {
      VAST_LOG_DEBUG("query leaves current segment " << current_->id());
      reader_.reset();
      auto i = std::lower_bound(
          segments_.begin(), segments_.end(), current_, segment_lt);
      while (++i != segments_.end())
        if ((*i)->contains(cursor_))
        {
          current_ = *i;
          reader_ = make_unique<segment::reader>(&current_.read());
          VAST_LOG_DEBUG("query enters next segment " << current_->id());
        }
    }

    if (evaluate(ast_, e).get<bool>())
    {
      fn_(std::move(e));
      if (++results == n)
        break;
    }
    else
    {
      VAST_LOG_WARN("query ignores false positive " << ast_ << ": " << e);
    }
  }

  if (results == 0)
    VAST_LOG_WARN(
        "query could not extract a result from segment " << current_->id());

  return results;
}

std::vector<event_id> query::scan() const
{
  if (segments_.empty())
    return {cursor_};
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

event_id query::cursor() const
{
  return cursor_;
}

size_t query::segments() const
{
  return segments_.size();
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
        VAST_LOG_ACTOR_DEBUG(
            "got new result starting at " <<
            (cbs->empty() ? 0 : cbs->find_first()));
        query_.update(*cbs);
        send(self, atom("fetch"));
        send(self, atom("extract"), batch_size_);
      },
      on_arg_match >> [=](event_id eid)
      {
        if (eid == query_.cursor())
        {
          VAST_LOG_ACTOR_ERROR("could not obtain segment for event ID " << eid);
          quit(exit::error);
        }
      },
      on_arg_match >> [=](segment const&)
      {
        cow<segment> s = *tuple_cast<segment>(last_dequeued());
        VAST_LOG_ACTOR_DEBUG("adds segment " << s->id());
        if (! query_.add(s))
          VAST_LOG_ACTOR_WARN("ignores duplicate segment " << s->id());
        auto n = query_.consolidate(3, 3); // TODO: Make configurable.
        if (n > 0)
          VAST_LOG_ACTOR_DEBUG("purged " << n << " segments");
        send(self, atom("fetch"));
        send(self, atom("extract"), batch_size_);
      },
      on(atom("batch size"), arg_match) >> [=](uint32_t n)
      {
        batch_size_ = n;
      },
      on(atom("extract"), arg_match) >> [=](uint64_t n)
      {
        if (query_.cursor() == bitstream::npos)
        {
          VAST_LOG_ACTOR_DEBUG("ignores extraction request");
          return;
        }
        auto got = query_.extract(n);
        VAST_LOG_ACTOR_DEBUG("extracted " << got << '/' << n << " events");
        if (query_.cursor() == bitstream::npos)
          send(sink_, atom("done"));
        //else if (got < n)
        //  send(self, atom("extract"), n - got);
      },
      on(atom("fetch")) >> [=]
      {
        if (query_.segments() <= 3 + 3) // TODO: Make configurable.
          for (auto eid : query_.scan())
          {
            VAST_LOG_ACTOR_DEBUG("asks for segment containing id " << eid);
            send(archive_, atom("segment"), eid);
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
