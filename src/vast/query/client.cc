#include "vast/query/client.h"

#include <ze/event.h>
#include "vast/util/console.h"
#include "vast/util/logger.h"
#include "vast/query/exception.h"

namespace vast {
namespace query {

client::client(cppa::actor_ptr search, unsigned batch_size)
  : batch_size_(batch_size)
  , search_(search)
{
  using namespace cppa;
  auto shutdown = on(atom("shutdown")) >> [=]
    {
      LOG(verbose, query) << "shutting down query client " << id();
      self->quit();
    };

  init_state = (
      on(atom("query"), atom("create"), arg_match)
        >> [=](std::string const& expression)
      {
        search_ << self->last_dequeued();
        become(operating_);
      },
      shutdown
    );

  operating_ = (
      on(atom("query"), atom("create"), atom("ack"), arg_match)
        >> [=](actor_ptr query)
      {
        query_ = query;
        send(query_, atom("set"), atom("batch size"), batch_size_);
      },
      on(atom("set"), atom("batch size"), atom("ack")) >> [=]
      {
        wait_for_input();
      },
      on(atom("statistics"), arg_match)
        >> [=](uint64_t processed, uint64_t matched)
      {
        std::cout
            << "statistics for query " << query_->id() << ": "
            << processed << " events processed, "
            << matched << " events matched ("
            << (matched / processed * 100) << " selectivity)"
            << std::endl;
      },
      on_arg_match >> [=](ze::event const& e)
      {
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
          auto t = tuple_cast<ze::event>(self->last_dequeued());
          assert(t.valid());
          buffer_.push(*t);
        }
      },
      shutdown
    );
}

void client::wait_for_input()
{
  using namespace cppa;
  util::unbuffer();

  char c;
  while (std::cin.get(c))
  {
    switch (c)
    {
      case ' ':
        {
          if (! try_print())
          {
            LOG(debug, query) << "asking for next batch in query " << query_->id();
            send(query_, atom("next batch"));
            asking_ = true;
          }
        }
        break;
      case 's':
        {
          LOG(debug, query) << "asking statistics about query " << query_->id();
          send(query_, atom("get"), atom("statistics"));
        }
        break;
      case 'q':
        send(self, atom("shutdown"));
        break;
      default:
        continue;
    }
  }

  util::buffer();
};

bool client::try_print()
{
  cppa::cow_tuple<ze::event> e;
  bool popped;
  do
  {
    popped = buffer_.try_pop(e);
    if (popped)
    {
      std::cout << cppa::get<0>(e) << std::endl;
      ++printed_;
    }
  }
  while (popped && printed_ % batch_size_ != 0);
  return popped && printed_ % batch_size_ == 0;
}

} // namespace query
} // namespace vast
