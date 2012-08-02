#include "vast/query.h"

#include <ze/chunk.h>
#include <ze/event.h>
#include <ze/type/regex.h>
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
             cppa::actor_ptr sink)
  : archive_(archive)
  , index_(index)
  , sink_(sink)
{
  LOG(verbose, query)
    << "spawning query @" << id() << " with sink @" << sink_->id();

  chaining(false);
  init_state = (
      on(atom("set"), atom("expression"), arg_match) >> [=](std::string const& expr)
      {
        DBG(query)
          << "query @" << id() << " parses expression '" << expr << "'";

        try
        {
          expr_.parse(expr);
          reply(atom("set"), atom("expression"), atom("success"));

          return;
        }
        catch (error::syntax const& e)
        {
          std::stringstream msg;
          msg << "query @" << id() << " is invalid: " << e.what();

          LOG(error, query) << msg.str();
          reply(atom("set"), atom("expression"), atom("failure"), msg.str());
        }
        catch (error::semantic const& e)
        {
          std::stringstream msg;
          msg << "query @" << id() << " is invalid: " << e.what();

          LOG(error, query) << msg.str();
          reply(atom("set"), atom("expression"), atom("failure"), msg.str());
        }

      },
      on(atom("set"), atom("batch size"), arg_match) >> [=](unsigned batch_size)
      {
        if (batch_size == 0)
        {
          LOG(warn, query)
            << "query @" << id() << " ignore invalid batch size of 0";

          reply(atom("set"), atom("batch size"), atom("failure"));
        }
        else
        {
          LOG(debug, query)
            << "query @" << id() << " sets batch size to " << batch_size;

          batch_size_ = batch_size;
          reply(atom("set"), atom("batch size"), atom("success"));
        }
      },
      on(atom("start")) >> [=]
      {
        DBG(query) << "query @" << id() << " hits index";
        run();
      },
      on_arg_match >> [=](std::vector<ze::uuid> const& ids)
      {
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
        // Extract results when one of the following conditions hold:
        //
        //  - The arriving segment is the first of all arriving segments.
        //  - The window was stale prior to the arrival of this segment.
        if (ack_++ == ids_.begin() || window_.stale())
            send(self, atom("get"), atom("results"));

        auto opt = tuple_cast<segment>(last_dequeued());
        assert(opt.valid());
        window_.push(*opt);

        DBG(query)
          << "query @" << id() << " received segment " << s.id();
      },
      on(atom("get"), atom("results")) >> [=]
      {
        extract(batch_size_);
      },
      on(atom("get"), atom("statistics")) >> [=]
      {
        reply(atom("statistics"), stats_.processed, stats_.matched);
      },
      on(atom("shutdown")) >> [=]
      {
        self->quit();
        LOG(verbose, query) << "query @" << id() << " terminated";
      });
}

void query::run()
{
  auto future = sync_send(index_, atom("hit"), expr_);
  handle_response(future)(
      on(atom("hit"), arg_match) >> [=](std::vector<ze::uuid> const& ids)
      {
        LOG(info, query)
          << "query @" << id() << " received index hit ("
          << ids.size() << " segments)";

        auto opt = tuple_cast<anything, std::vector<ze::uuid>>(last_dequeued());
        assert(opt.valid());
        cow_tuple<std::vector<ze::uuid>> id_tuple(*opt);
        send(self, id_tuple);
      },
      on(atom("impossible")) >> [=]
      {
        LOG(info, query)
          << "query @" << id() << " cannot use index to speed up answer";
        LOG(info, query)
          << "query @" << id() << " asks archive @" << archive_->id() << " for all segments";

        send(archive_, atom("get"), atom("ids"));
      },
      on(atom("miss")) >> [=]
      {
        LOG(info, query)
          << "query @" << id() << " received index miss";

        send(sink_, atom("query"), atom("index"), atom("miss"));
      },
      after(std::chrono::minutes(1)) >> [=]
      {
        LOG(error, query)
          << "query @" << id()
          << " timed out after waiting one minute for index answer";

        send(sink_, atom("query"), atom("index"), atom("time-out"));
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
        ++i;
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
        ++i;
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
    send(sink_, std::move(event));
    ++stats_.matched;
    return true;
  }

  return false;
}

} // namespace vast
