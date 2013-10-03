#include "vast/query.h"

#include "vast/event.h"
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
  {
    auto s = &get<0>(segments_.front());
    reader_.reset(new segment::reader(s));
  }
}

bool query::window::extract(event& e)
{
  if (reader_)
    return false;

  if (reader_->read(e))
    return true;
  
  reader_.reset();
  assert(! segments_.empty());
  segments_.pop_front();
  if (! segments_.empty())
  {
    auto s = &get<0>(segments_.front());
    reader_.reset(new segment::reader(s));
  }

  return false;
}


query::query(cppa::actor_ptr archive,
             cppa::actor_ptr index,
             cppa::actor_ptr sink,
             expression expr)
  : expr_(std::move(expr))
  , archive_(archive)
  , sink_(sink)
{
  VAST_LOG_VERBOSE("spawning query @" << id() << " with sink @" << sink_->id());

  chaining(false);
  init_state = (
      on(atom("start")) >> [=]
      {
        VAST_LOG_DEBUG("query @" << id() << " hits index");
        // TODO: make this an asynchronous.
        sync_send(index, atom("hit"), expr_).then(
            on(atom("hit"), arg_match) >> [=](std::vector<uuid> const& ids)
            {
              VAST_LOG_INFO(
                  "query @" << id() <<
                  " received index hit (" << ids.size() << " segments)");

              send(self, ids);
            },
            on(atom("impossible")) >> [=]
            {
              VAST_LOG_INFO("query @" << id() <<
                            " cannot use index to speed up answer," <<
                            " asking archive @" << archive_->id() <<
                            " for all segments");

              send(archive_, atom("get"), atom("ids"));
            },
            on(atom("miss")) >> [=]
            {
              VAST_LOG_VERBOSE("query @" << id() << " received index miss");

              // TODO: Eventually, we want to let the user decide what happens
              // on a index miss.
              //send(sink_, atom("query"), atom("index"), atom("miss"));
              send(archive_, atom("get"), atom("ids"));
            },
            after(std::chrono::minutes(1)) >> [=]
            {
              VAST_LOG_ERROR(
                  "query @" << id() <<
                  " timed out after waiting one minute for index answer");

              send(sink_, atom("query"), atom("index"), atom("time-out"));
            });
      },
      on_arg_match >> [=](std::vector<uuid> const& ids)
      {
        if (ids.empty())
        {
          VAST_LOG_DEBUG("query @" << id() << " received empty id set");
          send(sink_, atom("query"), atom("finished"));
          quit(exit::done);
          return;
        }

        ids_.insert(ids_.end(), ids.begin(), ids.end());
        while (head_ - ack_ < std::min(ids_.size(), window_size_))
        {
          VAST_LOG_DEBUG("query @" << id() <<
                         " prefetches segment " << ids_[head_]);
          send(archive_, atom("get"), ids_[head_]);
          ++head_;
        }
      },
      on_arg_match >> [=](segment const& s)
      {
        ++ack_;

        VAST_LOG_DEBUG("query @" << id() <<
                       " received segment " << s.id() <<
                       " (ack: " << ack_ << " head: " << head_ << ')');

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
          VAST_LOG_DEBUG("query @" << id() << " ignores pause request");
          return;
        }
        VAST_LOG_DEBUG("query @" << id() << " pauses processing");
        running_ = false;
      },
      on(atom("resume")) >> [=]
      {
        if (running_ == true)
        {
          VAST_LOG_DEBUG("query @" << id() << " ignores resume request");
          return;
        }
        VAST_LOG_DEBUG("query @" << id() << " resumes processing");
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
          event e;
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
          VAST_LOG_DEBUG("query @" << id() <<
                         " extracted " << i << " results" <<
                         " (evaluated " << stats_.evaluated << " events)");
        }

        if (stats_.batch == batch_size_)
        {
          VAST_LOG_DEBUG(
              "query @" << id() <<
              " extracted full batch" <<
              " (ack: " << ack_ << " head: " << head_ << ')');

          stats_.batch = 0;
          send(self, atom("results"));
        }
        else if (head_ - ack_ < window_size_ && head_ < ids_.size())
        {
          VAST_LOG_DEBUG("query @" << id() <<
                         " prefetches segment " << ids_[head_] <<
                         " (ack: " << ack_ << " head: " << head_ << ')');

          send(archive_, atom("get"), ids_[head_++]);
        }
        else if (ack_ < head_)
        {
          VAST_LOG_DEBUG("query @" << id() <<
                         " has in-flight segments and tries again later" <<
                         " (ack: " << ack_ << " head: " << head_ << ')');
        }
        else if (head_ == ids_.size())
        {
          VAST_LOG_DEBUG("query @" << id() <<
                         " has no more segments to process" <<
                         " (ack: " << ack_ << " head: " << head_ << ')');

          running_ = false;
          send(sink_, atom("query"), atom("finished"));
        }
      },
      on(atom("statistics")) >> [=]
      {
        reply(atom("statistics"), stats_.evaluated, stats_.results);
      });
}

} // namespace vast
