#ifndef VAST_ACTOR_SOURCE_BGPDUMP_H
#define VAST_ACTOR_SOURCE_BGPDUMP_H

#include "vast/schema.h"
#include "vast/actor/source/line_based.h"

namespace vast {
namespace source {

struct bgpdump_state : line_based_state {
  bgpdump_state(local_actor* self);

  vast::schema schema() final;
  void schema(vast::schema const& sch) final;
  result<event> extract() final;

  type announce_type_;
  type route_type_;
  type withdraw_type_;
  type state_change_type_;
};

/// A source reading ASCII output from the BGPDump utility.
/// @param self The actor handle.
/// @param in The input stream to read from.
behavior bgpdump(stateful_actor<bgpdump_state>* self,
                 std::unique_ptr<std::istream> in);

} // namespace source
} // namespace vast

#endif
