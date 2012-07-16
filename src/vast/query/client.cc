#include <vast/query/client.h>

#include <ze/event.h>
#include <vast/util/console.h>
#include <vast/util/logger.h>
#include <vast/query/exception.h>

namespace vast {
namespace query {

client::client(cppa::actor_ptr search, unsigned batch_size)
  : batch_size_(batch_size)
  , search_(search)
{
  LOG(verbose, query) << "spawning query client @" << id();
  using namespace cppa;
  auto shutdown = on(atom("shutdown")) >> [=]
    {
      self->quit();
      LOG(verbose, query) << "query client @" << id() << " terminated";
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
      on(atom("query"), atom("created"), arg_match) >> [=](actor_ptr query)
      {
        query_ = query;
        send(query_, atom("set"), atom("batch size"), batch_size_);
      },
      on(atom("set"), atom("batch size"), atom("ack")) >> [=]
      {
        LOG(info, query) << "successfully created query @" << query_->id();
        send(query_, atom("next chunk"));
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
      on(atom("query"), atom("finished")) >> [=]
      {
        LOG(verbose, query) << "query @" << query_->id() << " has finished";
        send(self, atom("shutdown"));
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
          wait_for_user_input();
        }
      },
      shutdown
    );
}

void client::wait_for_user_input()
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
          LOG(debug, query) 
            << "asking for next chunk in query @" << query_->id();
          send(query_, atom("next chunk"));
          asking_ = true;
        }
        break;
      case 's':
        {
          LOG(debug, query) << "asking statistics about query @" << query_->id();
          send(query_, atom("get"), atom("statistics"));
        }
        break;
      case 'q':
        {
          LOG(debug, query) << "shutting down";
          send(self, atom("shutdown"));
        }
        break;
      default:
        {
          LOG(debug, query) << "invalid command, use <space>, q(uit), or s(top)";
        }
        continue;
    }
  }

  util::buffer();
};

} // namespace query
} // namespace vast
