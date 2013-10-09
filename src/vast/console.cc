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
      [=](std::string q)
      {
        if (q.empty())
          return true;
        sync_send(search_, atom("query"), atom("create"), q).then(
            on_arg_match >> [=](actor_ptr qry)
            {
              if (! qry)
              {
                VAST_LOG_ACTOR_ERROR("invalid query: " << q);
                return true;
              }
              VAST_LOG_ACTOR_VERBOSE("got query @" << qry->id());
              cmdline_.append_to_history(q);
              query_ = qry;
              link_to(qry);
              return false;
            },
            others() >> [=]
            {
              VAST_LOG_ACTOR_ERROR("got unexpected message: " <<
                                   to_string(last_dequeued()));
            });
        return true;
      });
}

void console::act()
{
  become(
      on(atom("DOWN"), arg_match) >> [=](uint32_t)
      {
        VAST_LOG_ACTOR_ERROR("got DOWN from query @" << last_sender()->id());
        query_ = nullptr;
        delayed_prompt_display();
      },
      on(atom("run")) >> [=]
      {
        delayed_prompt_display();
      },
      on(atom("prompt")) >> [=]
      {
        bool callback_result;
        if (! cmdline_.process(callback_result) || callback_result)
          self << last_dequeued();
      },
      on_arg_match >> [=](event const& /* e */)
      {
        auto opt = tuple_cast<event>(last_dequeued());
        assert(opt);
        results_.push_back(*opt);
        std::cout << *results_.back() << std::endl;
      },
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
      others() >> [=]
      {
        VAST_LOG_ACTOR_ERROR("got unexpected message from @" <<
                             last_sender()->id() << ": " <<
                             to_string(last_dequeued()));
      });
}

char const* console::description() const
{
  return "console";
}

void console::delayed_prompt_display(size_t ms)
{
  // The delay allows for logging messages to trickle through first
  // before we print the prompt.
  delayed_send(self, std::chrono::milliseconds(ms), atom("prompt"));
}

} // namespace vast
