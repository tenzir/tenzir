#include "vast/query.h"

#include <cppa/cppa.hpp>
#include "vast/event.h"
#include "vast/logger.h"
#include "vast/segment.h"
#include "vast/uuid.h"

namespace vast {

query::query(expr::ast ast)
  : ast_{std::move(ast)}
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
  current_ = unprocessed_.find_first();
  VAST_LOG_DEBUG("query adjusted cursor to " << current_);
}

bool query::add(cow<segment> s)
{
  return segments_.insert(s->base(), s->base() + s->events(), s);
}

bool query::executable() const
{
  return segments_.lookup(current_) != nullptr;
}

size_t query::process(std::function<void(event)> f)
{
  auto seg = segments_.lookup(current_);
  if (! seg)
  {
    VAST_LOG_ERROR("no segment for current event id " << current_);
    // TODO: seek to next available segment.
    return 0;
  }
  auto& s = **seg;
  segment::reader r{&s};
  event e;
  size_t n = 0;
  while (r.skip_to(current_) && r.read(e))
  {
    if (current_ != bitstream::npos)
      current_ = unprocessed_.find_next(current_);
    processed_.append(e.id() - processed_.size(), false);
    processed_.append(1, true);
    auto v = evaluate(ast_, e);
    assert(v.which() == bool_type);
    if (! v.get<bool>())
    {
      VAST_LOG_WARN("ignoring false positive " << ast_ << ": " << e);
      continue;
    }
    f(std::move(e));
    ++n;
  }
  if (n == 0)
    VAST_LOG_WARN("did not apply function once in segment " << s.id());
  return n;
}

event_id query::current() const
{
  return current_;
}

using namespace cppa;

query_actor::query_actor(actor_ptr archive, actor_ptr sink, expr::ast ast)
  : archive_{std::move(archive)},
    sink_{std::move(sink)},
    query_{std::move(ast)}
{
}

void query_actor::act()
{
  become(
      on_arg_match >> [=](bitstream const& bs)
      {
        assert(bs);
        auto cbs = cow<bitstream>{*tuple_cast<bitstream>(last_dequeued())};
        VAST_LOG_ACTOR_DEBUG(
            "got new result of size " << (cbs->empty() ? 0 : cbs->size() - 1));
        query_.update(*cbs);
        if (! query_.executable())
          send(archive_, atom("segment"), query_.current());
        else if (query_.current() == bitstream::npos)
          VAST_LOG_ACTOR_DEBUG("has no more events to extract");
      },
      on_arg_match >> [=](event_id eid)
      {
        if (eid == query_.current())
        {
          VAST_LOG_ACTOR_ERROR("could not obtain segment for event ID " << eid);
          quit(exit::error);
        }
      },
      on_arg_match >> [=](segment const&)
      {
        auto s = cow<segment>{*tuple_cast<segment>(last_dequeued())};
        VAST_LOG_ACTOR_DEBUG("adds segment " << s->id());
        if (! query_.add(s))
          VAST_LOG_ACTOR_WARN("ignores duplicate segment " << s->id());
        if (query_.executable())
          query_.process([=](event e) { send(sink_, std::move(e)); });
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
