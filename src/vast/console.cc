#include "vast/console.h"

#include <cassert>
#include <iomanip>
#include <cppa/cppa.hpp>
#include "vast/event.h"
#include "vast/exception.h"

namespace vast {

using namespace cppa;

console::console(cppa::actor_ptr search)
  : search_{std::move(search)}
{
  cmdline_.add_mode("main", "main mode", "::: ");
  cmdline_.push_mode("main");
  cmdline_.on_unknown_command(
      "main",
      [=](std::string)
      {
        std::cout << "invalid command, try 'help'" << std::endl;
        return true;
      });

  cmdline_.add_command(
      "main",
      "help",
      [=](std::string)
      {
        std::cout << "coming soon" << std::endl;
        return true;
      });

  cmdline_.add_command(
      "main",
      "query",
      [=](std::string)
      {
        cmdline_.push_mode("query");
        return true;
      });

  cmdline_.add_command(
      "main",
      "exit",
      [=](std::string)
      {
        quit(exit::stop);
        return false;
      });

  cmdline_.add_mode("query", "query mode", "-=> ");
  cmdline_.add_command(
      "query",
      "exit",
      [=](std::string)
      {
        cmdline_.pop_mode();
        return true;
      });

  cmdline_.on_unknown_command(
      "query",
      [=](std::string qry)
      {
        std::cout << "processing query: " << qry << std::endl;
        cmdline_.append_to_history(qry);
        return true;
      });
}

void console::act()
{
  become(
      on(atom("run")) >> [=]
      {
        // The delay allows for logging messages to trickle through first
        // before we print the prompt.
        delayed_send(self, std::chrono::milliseconds(100), atom("prompt"));
      },
      on(atom("prompt")) >> [=]
      {
        bool callback_result;
        if (! cmdline_.process(callback_result) || callback_result)
          self << last_dequeued();
      },
      on(atom("query"), atom("create"), arg_match) >> [=](std::string const& s)
      {
        sync_send(search_, atom("query"), atom("create"), s, self).then(
            on_arg_match >> [=](actor_ptr qry)
            {
              if (qry)
              {
                query_ = qry;
                VAST_LOG_ACTOR_VERBOSE("connected to query @" << query_->id());
                send(query_, atom("start"));
              }
              else
              {
                VAST_LOG_ACTOR_ERROR("invalid query: " << s);
                quit(exit_reason::user_defined);
              }
            });
      }
//    on(atom("query"), atom("failure"), arg_match) >> [=](std::string const& e)
//    {
//      VAST_LOG_ERROR(e);
//    },
//      on(atom("statistics")) >> [=]
//      {
//        VAST_LOG_DEBUG("query client @" << id() <<
//                       " asks for statistics of query @" << query_->id());
//
//        forward_to(query_);
//      },
//      on(atom("statistics"), arg_match)
//        >> [=](uint64_t processed, uint64_t matched)
//      {
//        auto selectvity =
//          static_cast<double>(processed) / static_cast<double>(matched);
//
//        VAST_LOG_VERBOSE(
//            "query @" << query_->id() <<
//            " processed " << processed << " events," <<
//            " matched " << matched << " events" <<
//            " (selectivity " << std::setprecision(3) << selectvity << "%)");
//      },
//    on(atom("results")) >> [=]
//    {
//      size_t i = 0;
//      while (! results_.empty() && i < batch_size)
//      {
//        std::cout << *results_.front() << std::endl;
//        results_.pop_front();
//        ++i;
//      }
//
//      VAST_LOG_DEBUG(
//        "query client @" << id() <<
//        " printed " << i << " results" <<
//        " (buffered: " << results_.size() << '/' << buffer_size_ << ')');
//
//      if (! running_ && results_.size() < buffer_size_)
//      {
//        send(query_, atom("resume"));
//        running_ = true;
//        VAST_LOG_DEBUG(
//            "query client @" << id() <<
//            " underflowed local result buffer (" << results_.size() << ")," <<
//            " resuming query @" << query_->id());
//      }
//    },
//    on_arg_match >> [=](event const& /* e */)
//    {
//      auto opt = tuple_cast<event>(last_dequeued());
//      assert(opt);
//      results_.push_back(*opt);
//      if (running_ && results_.size() >= buffer_size_)
//      {
//        send(query_, atom("pause"));
//        running_ = false;
//        VAST_LOG_DEBUG(
//            "query client @" << id() <<
//            " overflowed local result buffer (" << buffer_size_ << ")," <<
//            " pausing query @" << query_->id());
//      }
    );
}

char const* console::description() const
{
  return "console";
}

} // namespace vast
