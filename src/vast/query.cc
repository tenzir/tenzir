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
}

void query::update(bitstream result)
{
  if (result_)
    result -= result_;
  result_ = std::move(result);
  current_ = result_.find_first();
  VAST_LOG_DEBUG("query sets current event ID to " << current_);
}

event_id query::advance(size_t n)
{
  if (bitstream::npos - n < current_)
    current_ = bitstream::npos;
  else
    current_ += n;
  return current_;
}

size_t query::apply(cow<segment> const& s, std::function<void(event)> f)
{
  segment::reader r{&s.read()};
  event e;
  size_t n = 0;
  while (r.skip_to(current_) && r.read(e))
  {
    if (result_ && current_ != bitstream::npos)
      current_ = result_.find_next(current_);
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
        VAST_LOG_ACTOR_DEBUG("got new result");
        assert(bs);
        auto cbs = cow<bitstream>{*tuple_cast<bitstream>(last_dequeued())};
        query_.update(*cbs);
        current_ = query_.current();
        send(archive_, current_);
      },
      on_arg_match >> [=](event_id eid)
      {
        if (eid == current_)
        {
          VAST_LOG_ACTOR_ERROR("could not obtain segment for event ID" << eid);
          quit(exit::error);
        }
      },
      on_arg_match >> [=](segment const&)
      {
        auto s = cow<segment>{*tuple_cast<segment>(last_dequeued())};
        VAST_LOG_ACTOR_ERROR("processes segment " << s->id());
        query_.apply(*s, [=](event e) { send(sink_, std::move(e)); });
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
