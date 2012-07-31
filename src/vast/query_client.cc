#include "vast/query_client.h"

#include <ze/event.h>
#include "vast/exception.h"
#include "vast/logger.h"
#include "vast/util/console.h"

namespace vast {

query_client::query_client(cppa::actor_ptr search,
                           std::string const& expression,
                           unsigned batch_size)
  : expression_(expression)
  , batch_size_(batch_size)
  , search_(search)
{
  LOG(verbose, query) << "spawning query client @" << id();

  using namespace cppa;
  init_state = (
      on(atom("start")) >> [=]
      {
        // FIXME: We use 'become' until the sync_send issues have been fixed.
        //auto future = sync_send(search_, atom("query"), atom("create"));
        //handle_response(future)(
        send(search_, atom("query"), atom("create"));
        become(
            keep_behavior,
            on(atom("query"), arg_match) >> [=](actor_ptr query)
            {
              query_ = query;
              LOG(info, query) << "query client @" << id()
                << " successfully created query @" << query_->id();

              unbecome(); // TODO: remove after sync_send fix.
              set_batch_size();
            },
            after(std::chrono::seconds(2)) >> [=]
            {
              LOG(error, query)
                << "query client @" << id()
                << " timed out trying to create query";

              unbecome(); // TODO: remove after sync_send fix.
              send(self, atom("shutdown"));
            });
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
        std::cout << e << std::endl;
        if (++printed_ % batch_size_ == 0)
          wait_for_user_input();
      },
      on(atom("shutdown")) >> [=]
      {
        quit();
        LOG(verbose, query) << "query client @" << id() << " terminated";
      });
}

void query_client::set_batch_size()
{
  using namespace cppa;
  // FIXME: We use 'become' until the sync_send issues have been fixed.
  //auto future = sync_send(query_, atom("set"), atom("batch size"), batch_size_);
  //handle_response(future)(
  send(query_, atom("set"), atom("batch size"), batch_size_);
  become(
      keep_behavior,
      on(atom("set"), atom("batch size"), atom("failure")) >> [=]
      {
        unbecome(); // TODO: remove after sync_send fix.
        send(self, atom("shutdown"));
      },
      on(atom("set"), atom("batch size"), atom("success")) >> [=]
      {
        LOG(info, query) << "query client @" << id()
          << " successfully set batch size to " << batch_size_;

        unbecome(); // TODO: remove after sync_send fix.
        set_expression();
      },
      after(std::chrono::seconds(1)) >> [=]
      {
        LOG(info, query) << "query client @" << id()
          << " timed out trying to set batch size to " << batch_size_;

        unbecome(); // TODO: remove after sync_send fix.
        send(self, atom("shutdown"));
      });
}

void query_client::set_expression()
{
  using namespace cppa;
  // FIXME: We use 'become' until the sync_send issues have been fixed.
  //auto f = sync_send(query_, atom("set"), atom("expression"), expression_);
  //handle_response(f)(
  send(query_, atom("set"), atom("expression"), expression_);
  become(
      keep_behavior,
      on(atom("set"), atom("expression"), atom("failure")) >> [=]
      {
        unbecome(); // TODO: remove after sync_send fix.
        send(self, atom("shutdown"));
      },
      on(atom("set"), atom("expression"), atom("success")) >> [=]
      {
        LOG(info, query) << "query client @" << id()
          << " successfully set expression '" << expression_ << "'";

        unbecome(); // TODO: remove after sync_send fix.
        send(query_, atom("start"));
      },
      after(std::chrono::seconds(1)) >> [=]
      {
        LOG(info, query) << "query client @" << id()
          << " timed out trying to set expression: " << expression_;

        unbecome(); // TODO: remove after sync_send fix.
        send(self, atom("shutdown"));
      });
}

void query_client::wait_for_user_input()
{
  using namespace cppa;
  util::unbuffer();

  DBG(query) << "query client @" << id() << " waits for user input";
  char c;
  while (std::cin.get(c))
  {
    switch (c)
    {
      case ' ':
        {
          DBG(query)
            << "query client @" << id()
            << " asks for more results in query @" << query_->id();

          send(query_, atom("get"), atom("results"));
        }
        return;
      case 's':
        {
          DBG(query)
            << "query client @" << id()
            << " asks for statistics of query @" << query_->id();

          send(query_, atom("get"), atom("statistics"));
        }
        return;
      case 'q':
        {
          DBG(query) << "query client @" << id() << " shuts down";
          send(self, atom("shutdown"));
        }
        return;
      default:
        {
          LOG(debug, query) << "invalid command, use <space>, q(uit), or s(top)";
        }
        continue;
    }
  }

  util::buffer();
};

} // namespace vast
