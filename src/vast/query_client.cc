#include "vast/query_client.h"

#include <cassert>
#include <iomanip>
#include "vast/event.h"
#include "vast/exception.h"
#include "vast/logger.h"

namespace vast {

using namespace cppa;

query_client::query_client(actor_ptr search,
                           std::string const& expression,
                           uint32_t batch_size)
  : search_(search)
{
  VAST_LOG_VERBOSE("spawning query client @" << id());

  init_state = (
      on(atom("start")) >> [=]
      {
        send(search_, atom("query"), atom("create"), expression, batch_size);
      },
      on(atom("query"), atom("failure"), arg_match)
        >> [=](std::string const& msg)
      {
        VAST_LOG_ERROR(msg);
      },
      on(atom("query"), arg_match) >> [=](actor_ptr query)
      {
        query_ = query;
        VAST_LOG_VERBOSE("query client @" << id() <<
                         " successfully created query @" << query_->id());

        send(query_, atom("start"));
      },
      on(atom("query"), atom("finished")) >> [=]
      {
        VAST_LOG_INFO("query @" << query_->id() << " has finished");
      },
      on(atom("statistics")) >> [=]
      {
        VAST_LOG_DEBUG("query client @" << id() <<
                       " asks for statistics of query @" << query_->id());

        forward_to(query_);
      },
      on(atom("statistics"), arg_match)
        >> [=](uint64_t processed, uint64_t matched)
      {
        auto selectvity =
          static_cast<double>(processed) / static_cast<double>(matched);

        VAST_LOG_VERBOSE(
            "query @" << query_->id() <<
            " processed " << processed << " events," <<
            " matched " << matched << " events" <<
            " (selectivity " << std::setprecision(3) << selectvity << "%)");
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

        VAST_LOG_DEBUG(
          "query client @" << id() <<
          " printed " << i << " results" <<
          " (buffered: " << results_.size() << '/' << buffer_size_ << ')');

        if (! running_ && results_.size() < buffer_size_)
        {
          send(query_, atom("resume"));
          running_ = true;
          VAST_LOG_DEBUG(
              "query client @" << id() <<
              " underflowed local result buffer (" << results_.size() << ")," <<
              " resuming query @" << query_->id());
        }
      },
      on_arg_match >> [=](event const& /* e */)
      {
        auto opt = tuple_cast<event>(last_dequeued());
        assert(opt);
        results_.push_back(*opt);
        if (running_ && results_.size() >= buffer_size_)
        {
          send(query_, atom("pause"));
          running_ = false;
          VAST_LOG_DEBUG(
              "query client @" << id() <<
              " overflowed local result buffer (" << buffer_size_ << ")," <<
              " pausing query @" << query_->id());
        }
      },
      on(atom("kill")) >> [=]
      {
        quit();
        VAST_LOG_VERBOSE("query client @" << id() << " terminated");
      });
}

} // namespace vast
