#ifndef VAST_ACTOR_SOURCE_BRO_H
#define VAST_ACTOR_SOURCE_BRO_H

#include <memory>
#include <string>
#include <unordered_map>

#include "vast/schema.h"
#include "vast/actor/source/line_based.h"
#include "vast/concept/parseable/core/rule.h"

namespace vast {
namespace source {

struct bro_state : line_based_state {
  bro_state(local_actor* self);

  vast::schema schema() final;
  void schema(vast::schema const& sch) final;
  result<event> extract() final;

  trial<void> parse_header();

  std::string separator_ = " ";
  std::string set_separator_;
  std::string empty_field_;
  std::string unset_field_;
  int timestamp_field_ = -1;
  vast::schema schema_;
  type type_;
  std::vector<rule<std::string::const_iterator, data>> parsers_;
};

// A source parsing Bro logs files.
// @param self The actor handle.
// @param in The input stream to read log lines from.
behavior bro(stateful_actor<bro_state>* self,
             std::unique_ptr<std::istream> in);

} // namespace source
} // namespace vast

#endif
