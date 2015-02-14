#ifndef VAST_ACTOR_SEARCH_H
#define VAST_ACTOR_SEARCH_H

#include "vast/actor/actor.h"
#include "vast/util/flat_set.h"

namespace vast {

class expression;

struct search : public default_actor
{
  search();

  void at(caf::exit_msg const& msg) override;
  void at(caf::down_msg const& msg) override;
  caf::message_handler make_handler() override;
  std::string name() const override;

  caf::actor archive_;
  caf::actor index_;
  std::multimap<caf::actor_addr, caf::actor> queries_;
};

} // namespace vast

#endif
