#include "vast/query_client.h"

#include <iomanip>
#include <ze.h>
#include "vast/exception.h"
#include "vast/logger.h"

namespace vast {

using namespace cppa;

query_client::query_client(actor_ptr search,
                           std::string const& expression,
                           uint32_t batch_size)
  : search_(search)
{
  LOG(verbose, query) << "spawning query client @" << id();

  init_state = (
      on(atom("start")) >> [=]
      {
        send(search_, atom("query"), atom("create"), expression, batch_size);
      },
      on(atom("query"), atom("failure"), arg_match)
        >> [=](std::string const& msg)
      {
        LOG(error, query) << msg;
      },
      on(atom("query"), arg_match) >> [=](actor_ptr query)
      {
        query_ = query;
        LOG(verbose, query)
          << "query client @" << id()
          << " successfully created query @" << query_->id();

        send(query_, atom("start"));
      },
      on(atom("query"), atom("finished")) >> [=]
      {
        LOG(info, query) << "query @" << query_->id() << " has finished";
      },
      on(atom("statistics")) >> [=]
      {
        DBG(query)
          << "query client @" << id()
          << " asks for statistics of query @" << query_->id();

        forward_to(query_);
      },
      on(atom("statistics"), arg_match)
        >> [=](uint64_t processed, uint64_t matched)
      {
        auto selectvity =
          static_cast<double>(processed) / static_cast<double>(matched);

        LOG(info, query)
            << "query @" << query_->id()
            << " processed " << processed << " events,"
            << " matched " << matched << " events"
            << " (selectivity " << std::setprecision(3) << selectvity << "%)";
      },
      on(atom("results")) >> [=]
      {
        size_t i = 0;
        while (! results_.empty() && i < batch_size)
        {
          std::cout << get<0>(results_.front()) << std::endl;
          results_.pop_front();
          ++i;
        }

        DBG(query)
          << "query client @" << id()
          << " printed " << i << " results"
          << " (buffered: " << results_.size() << '/' << buffer_size_ << ')';

        if (! running_ && results_.size() < buffer_size_)
        {
          DBG(query)
            << "query client @" << id()
            << " underflowed local result buffer (" << results_.size() << "),"
            << " resuming query @" << query_->id();
          send(query_, atom("resume"));
          running_ = true;
        }
      },
      on_arg_match >> [=](ze::event const& e)
      {
        auto opt = tuple_cast<ze::event>(last_dequeued());
        assert(opt);
        results_.push_back(*opt);
        if (running_ && results_.size() >= buffer_size_)
        {
          DBG(query)
            << "query client @" << id()
            << " overflowed local result buffer (" << buffer_size_ << "),"
            << " pausing query @" << query_->id();
          send(query_, atom("pause"));
          running_ = false;
        }
      },
      on(atom("kill")) >> [=]
      {
        quit();
        LOG(verbose, query) << "query client @" << id() << " terminated";
      });
}

} // namespace vast
