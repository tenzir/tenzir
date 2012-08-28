#include "vast/query.h"

#include <ze.h>
#include <ze/to_string.h>
#include "vast/exception.h"
#include "vast/logger.h"

namespace vast {

using namespace cppa;

bool query::window::ready() const
{
  return reader_.get() != nullptr;
}

void query::window::add(cppa::cow_tuple<segment> s)
{
  segments_.push_back(s);
  if (! reader_)
    reader_.reset(new segment::reader(&get<0>(segments_.front())));
}

bool query::window::extract(ze::event& event)
{
  if (! reader_)
    return false;

  *reader_ >> event;

  if (reader_->events() == 0 && reader_->chunks() == 0)
  {
    reader_.reset();
    segments_.pop_front();
    if (! segments_.empty())
    {
      reader_.reset(new segment::reader(&get<0>(segments_.front())));
      assert(reader_->events() > 0);
      assert(reader_->chunks() > 0);
    }
  }

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
        // TODO: make this an asynchronous.
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

              // TODO: Eventually, we want to let the user decide what happens
              // on a index miss.
              //send(sink_, atom("query"), atom("index"), atom("miss"));
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

        ids_.insert(ids_.end(), ids.begin(), ids.end());
        while (head_ - ack_ < std::min(ids_.size(), window_size_))
        {
          DBG(query)
            << "query @" << id() << " prefetches segment " << ids_[head_];
          send(archive_, atom("get"), ids_[head_]);
          ++head_;
        }
      },
      on_arg_match >> [=](segment const& s)
      {
        ++ack_;

        DBG(query) 
          << "query @" << id() 
          << " received segment " << s.id()
          << " (ack: " << ack_ << " head: " << head_ << ')';

        auto opt = tuple_cast<segment>(last_dequeued());
        assert(opt.valid());
        window_.add(*opt);

        if (running_)
          send(self, atom("results"));
      },
      on(atom("pause")) >> [=]
      {
        if (running_ == false)
        {
          DBG(query) << "query @" << id() << " ignores pause request";
          return;
        }

        DBG(query) << "query @" << id() << " pauses processing";
        running_ = false;
      },
      on(atom("resume")) >> [=]
      {
        if (running_ == true)
        {
          DBG(query) << "query @" << id() << " ignores resume request";
          return;
        }

        DBG(query) << "query @" << id() << " resumes processing";
        running_ = true;
        send(self, atom("results"));
      },
      on(atom("results")) >> [=]
      {
        if (! running_)
          return;

        uint64_t i = 0;
        while (stats_.batch < batch_size_)
        {
          ze::event e;
          if (! window_.extract(e))
            break;

          ++stats_.evaluated;
          if (expr_.eval(e))
          {
            ++i;
            ++stats_.results;
            send(sink_, std::move(e));
          }
        }

        if (i > 0)
        {
          stats_.batch += i;
          LOG(debug, query)
            << "query @" << id()
            << " extracted " << i << " results"
            << " (evaluated " << stats_.evaluated << " events)";
        }

        if (stats_.batch == batch_size_)
        {
          DBG(query)
            << "query @" << id()
            << " extracted full batch"
            << " (ack: " << ack_ << " head: " << head_ << ')';

          stats_.batch = 0;
          send(self, atom("results"));
        }
        else if (head_ - ack_ < window_size_ && head_ < ids_.size())
        {
          DBG(query)
            << "query @" << id()
            << " prefetches segment " << ids_[head_]
            << " (ack: " << ack_ << " head: " << head_ << ')';

          send(archive_, atom("get"), ids_[head_++]);
        }
        else if (ack_ < head_)
        {
          DBG(query)
            << "query @" << id() << " has in-flight segments and tries again later"
            << " (ack: " << ack_ << " head: " << head_ << ')';
        }
        else if (head_ == ids_.size())
        {
          DBG(query)
            << "query @" << id() << " has no more segments to process"
            << " (ack: " << ack_ << " head: " << head_ << ')';

          running_ = false;
          send(sink_, atom("query"), atom("finished"));
        }
      },
      on(atom("statistics")) >> [=]
      {
        reply(atom("statistics"), stats_.evaluated, stats_.results);
      },
      on(atom("shutdown")) >> [=]
      {
        quit();
        LOG(verbose, query) << "query @" << id() << " terminated";
      });
}

} // namespace vast
