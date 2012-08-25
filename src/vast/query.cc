#include "vast/query.h"

#include <ze.h>
#include "vast/exception.h"
#include "vast/logger.h"

namespace vast {

using namespace cppa;

void query::window::push(cppa::cow_tuple<segment> s)
{
  segments_.push_back(s);
  if (! current_segment_)
  {
    assert(! reader_);
    current_segment_ = &get<0>(s);
    reader_.reset(new segment::reader(*current_segment_));
  }
}

bool query::window::ready() const
{
  return
    current_segment_ &&
    reader_ &&
    (reader_->events() > 0 || reader_->chunks() > 0);
}

bool query::window::stale() const
{
  return ! ready() && segments_.size() == 1;
}

bool query::window::one(ze::event& event)
{
  if (! ready())
    return false;

  *reader_ >> event;
  return true;
}

size_t query::window::size() const
{
  return segments_.size();
}

bool query::window::advance()
{
  if (segments_.size() < 2)
    return false;

  segments_.pop_front();
  current_segment_ = &get<0>(segments_.front());
  reader_.reset(new segment::reader(*current_segment_));

  return true;
}


query::query(cppa::actor_ptr archive,
             cppa::actor_ptr index,
             cppa::actor_ptr sink,
             expression expr)
  : expr_(std::move(expr))
  , archive_(archive)
  , sink_(sink)
{
  LOG(verbose, query)
    << "spawning query @" << id() << " with sink @" << sink_->id();

  chaining(false);
  init_state = (
      on(atom("start")) >> [=]
      {
        DBG(query) << "query @" << id() << " hits index";
        sync_send(index, atom("hit"), expr_).then(
            on(atom("hit"), arg_match) >> [=](std::vector<ze::uuid> const& ids)
            {
              LOG(info, query)
                << "query @" << id()
                << " received index hit (" << ids.size() << " segments)";

              send(self, ids);
            },
            on(atom("impossible")) >> [=]
            {
              LOG(info, query)
                << "query @" << id() << " cannot use index to speed up answer,"
                << " asking archive @" << archive_->id() << " for all segments";

              send(archive_, atom("get"), atom("ids"));
            },
            on(atom("miss")) >> [=]
            {
              LOG(info, query)
                << "query @" << id() << " received index miss";

              send(sink_, atom("query"), atom("index"), atom("miss"));
              // TODO: Eventually, we want to let the user decide what happens
              // on a index miss.
              send(archive_, atom("get"), atom("ids"));
            },
            after(std::chrono::minutes(1)) >> [=]
            {
              LOG(error, query)
                << "query @" << id()
                << " timed out after waiting one minute for index answer";

              send(sink_, atom("query"), atom("index"), atom("time-out"));
            });
      },
      on_arg_match >> [=](std::vector<ze::uuid> const& ids)
      {
        if (ids.empty())
        {
          LOG(info, query)
            << "query @" << id() << " received empty id set";
          send(self, atom("shutdown"));
          send(sink_, atom("query"), atom("finished"));
          return;
        }

        ids_ = ids;
        head_ = ack_ = ids_.begin();

        size_t first_fetch = std::min(ids_.size(), 3ul); // TODO: make configurable.
        for (size_t i = 0; i < first_fetch; ++i)
        {
          DBG(query) << "query @" << id() << " prefetches segment " << *head_;
          send(archive_, atom("get"), *head_++);
        }
      },
      on_arg_match >> [=](segment const& s)
      {
        // Start extracting result when one of the following conditions hold:
        //
        //  (1) The arriving segment is the first of all arriving segments.
        //  (2) The window was stale prior to the arrival of this segment.
        if (ack_++ == ids_.begin() || window_.stale())
            send(self, atom("get"), atom("results"));

        auto opt = tuple_cast<segment>(last_dequeued());
        assert(opt.valid());
        window_.push(*opt);

        DBG(query)
          << "query @" << id() << " received segment " << s.id();
      },
      on(atom("get"), atom("results"), arg_match) >> [=](uint32_t n)
      {
        extract(n);
      },
      on(atom("get"), atom("statistics")) >> [=]
      {
        reply(atom("statistics"), stats_.processed, stats_.matched);
      },
      on(atom("shutdown")) >> [=]
      {
        quit();
        LOG(verbose, query) << "query @" << id() << " terminated";
      });
}

void query::extract(size_t n)
{
  LOG(debug, query)
    << "query @" << id() << " tries to extract " << n << " results";

  ze::event e;
  size_t i = 0;
  while (i < n)
  {
    ze::event e;
    auto extracted = window_.one(e);
    if (extracted)
    {
      if (match(e))
      {
        send(sink_, std::move(e));
        ++i;
      }
    }
    else if (window_.advance())
    {
      DBG(query)
        << "query @" << id() << " advances to next segment in window";

      // Prefetch another segment if we still need to (and can do so).
      if (head_ - ack_ < window_size_ && head_ != ids_.end())
      {
        DBG(query)
          << "query @" << id() << " prefetches segment " << *head_;

        send(archive_, atom("get"), *head_++);
      }

      extracted = window_.one(e);
      assert(extracted); // By the post-condition of window::advance().

      if (match(e))
      {
        send(sink_, std::move(e));
        ++i;
      }
    }
    else if (ack_ < head_)
    {
      DBG(query)
        << "query @" << id() << " has in-flight segments, trying again later";

      break;
    }
    else if (head_ == ids_.end())
    {
      DBG(query)
        << "query @" << id() << " has no more segments to process";

      send(sink_, atom("query"), atom("finished"));
      break;
    }
    else
    {
      assert(! "should never happen");
      break;
    }
  }
}

bool query::match(ze::event const& event)
{
  ++stats_.processed;
  if (expr_.eval(event))
  {
    ++stats_.matched;
    return true;
  }

  return false;
}

} // namespace vast
