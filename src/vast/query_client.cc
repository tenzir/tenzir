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
        send(search_, atom("query"), atom("create"), expression);
      },
      on(atom("query"), atom("failure"), arg_match)
        >> [=](std::string const& msg)
      {
        LOG(error, query) << msg;
        send(self, atom("shutdown"));
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
        LOG(verbose, query) << "query @" << query_->id() << " has finished";
        send(self, atom("shutdown"));
      },
      on(atom("client"), atom("results")) >> [=]
      {
        DBG(query)
          << "query client @" << id()
          << " asks for more results in query @" << query_->id();

        send(query_, atom("get"), atom("results"), batch_size);
      },
      on(atom("client"), atom("statistics")) >> [=]
      {
        DBG(query)
          << "query client @" << id()
          << " asks for statistics of query @" << query_->id();

        send(query_, atom("get"), atom("statistics"));
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

} // namespace vast
