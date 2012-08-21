#include "vast/query_client.h"

#include <iomanip>
#include <ze.h>
#include "vast/exception.h"
#include "vast/logger.h"

namespace vast {

using namespace cppa;

query_client::query_client(actor_ptr search,
                           std::string const& expression,
                           unsigned batch_size)
  : expression_(expression)
  , batch_size_(batch_size)
  , search_(search)
{
  LOG(verbose, query) << "spawning query client @" << id();

  init_state = (
      on(atom("start")) >> [=]
      {
        sync_send(search_, atom("query"), atom("create")).then(
            on(atom("query"), arg_match) >> [=](actor_ptr query)
            {
              query_ = query;
              LOG(verbose, query) << "query client @" << id()
                << " successfully created query @" << query_->id();

              set_batch_size();
            },
            after(std::chrono::seconds(2)) >> [=]
            {
              LOG(error, query)
                << "query client @" << id()
                << " timed out trying to create query";

              send(self, atom("shutdown"));
            });
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
      on(atom("query"), atom("finished")) >> [=]
      {
        LOG(verbose, query) << "query @" << query_->id() << " has finished";
        send(self, atom("shutdown"));
      },
      on(atom("client"), atom("results")) >> [=]
      {
        DBG(query)
          << "query client @" << id()
          << " asks for more results in query @" << query_->id();

        send(query_, atom("get"), atom("results"));
      },
      on(atom("client"), atom("statistics")) >> [=]
      {
        DBG(query)
          << "query client @" << id()
          << " asks for statistics of query @" << query_->id();

        send(query_, atom("get"), atom("statistics"));
      },
      on_arg_match >> [=](ze::event const& e)
      {
        std::cout << e << std::endl;
      },
      on(atom("shutdown")) >> [=]
      {
        quit();
        LOG(verbose, query) << "query client @" << id() << " terminated";
      });
}

void query_client::set_batch_size()
{
  sync_send(query_, atom("set"), atom("batch size"), batch_size_).then(
      on(atom("set"), atom("batch size"), atom("failure")) >> [=]
      {
        LOG(error, query) << "query client @" << id()
          << " failed setting batch size to " << batch_size_;

        send(self, atom("shutdown"));
      },
      on(atom("set"), atom("batch size"), atom("success")) >> [=]
      {
        LOG(verbose, query) << "query client @" << id()
          << " successfully set batch size to " << batch_size_;

        set_expression();
      },
      after(std::chrono::seconds(1)) >> [=]
      {
        LOG(error, query) << "query client @" << id()
          << " timed out trying to set batch size to " << batch_size_;

        send(self, atom("shutdown"));
      });
}

void query_client::set_expression()
{
  sync_send(query_, atom("set"), atom("expression"), expression_).then(
      on(atom("set"), atom("expression"), atom("failure"), arg_match)
        >> [=](std::string const& msg)
      {
        LOG(error, query) << msg;

        send(self, atom("shutdown"));
      },
      on(atom("set"), atom("expression"), atom("success")) >> [=]
      {
        LOG(verbose, query) << "query client @" << id()
          << " successfully set expression '" << expression_ << "'";

        send(query_, atom("start"));
      },
      after(std::chrono::seconds(1)) >> [=]
      {
        LOG(error, query) << "query client @" << id()
          << " timed out trying to set expression: " << expression_;

        send(self, atom("shutdown"));
      });
}

} // namespace vast
