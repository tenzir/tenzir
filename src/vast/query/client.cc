#include "vast/query/client.h"

#include <ze/event.h>
#include "vast/util/console.h"
#include "vast/util/logger.h"
#include "vast/query/exception.h"

namespace vast {
namespace query {

client::client()
  , printed_(0u)
  , asking_(true)
{
  using namespace cppa;
  init_state = (
      on(atom("initialize"), arg_match) >> [](std::string const& host,
                                              unsigned port)
      {
        // Connect to server.
        remote_ = remote_actor(host, port);
      },
      on(atom("query"), atom("create"), arg_match) >> [](std::string const&
                                                         expression)
      {
        remote_ << self->last_dequeued();
      }
      on(atom("query", atom("created"), arg_match)) >> [](std::string const& id)
      {
        query_ = id;

        // FIXME: get real endpoint details from self.
        std::string host = "localhost";
        auto port = 4242;

        std::vector<std::string> vals{host, std::to_string(port)};
        send(remote_, atom("query"), atom("set"), id, "sink", vals);

        vals = {batch_size}
        send(remote_, atom("query"), atom("set"), id, "batch size", vals);
      },
      on<atom("query", atom("set"), arg_match> >> [](std::string const& id,
                                                      std::string const& opt,
                                                      std::string const& val)
      {
        remote_ << self->last_dequeued();
      },
      on<atom("query", atom("get"), arg_match> >> [](std::string const& id,
                                                      std::string const& opt)
      {
        remote_ << self->last_dequeued();
      },
      on(atom("get user input")) >> []()
      {
        wait_for_input();
      }
      on(atom("shutdown")) >> []()
      {
        LOG(verbose, query) << "telling server to stop query " << query_;
        send(remote_, atom("query"), atom("stop"), query_);
      },
      on_arg_match >> [](ze::event const& e)
      {
          // TODO: use become() to transition into a new state.
          if (asking_)
          {
              asking_ = false;
              std::cout << e << std::endl;
              ++printed_;
          }
          else if (printed_ % batch_size_ != 0)
          {
              std::cout << e << std::endl;
              ++printed_;
          }
          else
          {
              // FIXME: avoid copy.
              buffer_.push(e);
          }
      });
}

void client::wait_for_input()
{
    util::unbuffer();

    char c;
    while (std::cin.get(c))
    {
      switch (c)
      {
        case ' ':
          {
            bool printed = try_print();
            if (! printed)
            {
              LOG(debug, query) << "asking for next batch in query " << query_;
              send(remote_, atom("query"), atom("control"), query_, atom("next batch"));

              // TODO: become(asking)
              asking_ = true;
            }
          }
          break;
        case 's':
          {
            LOG(debug, query) << "asking statistics about query " << query_;
            send(remote_, atom("query"), atom("get"), query_, "statistics");
          }
          break;
        case 'q':
          send(self, atom("shutdown");
          break;
        default:
          continue;
      }
    }

    util::buffer();
};

bool client::try_print()
{
  ze::event e;
  bool popped;

  do
  {
    popped = buffer_.try_pop(e);
    if (popped)
    {
      std::cout << e << std::endl;
      ++printed_;
    }
  }
  while (popped && printed_ % batch_size_ != 0);

  return popped && printed_ % batch_size_ == 0;
}

} // namespace query
} // namespace vast
