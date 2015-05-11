#ifndef VAST_ACTOR_SOURCE_BGPDUMP_H
#define VAST_ACTOR_SOURCE_BGPDUMP_H

#include "vast/schema.h"
#include "vast/actor/source/file.h"

namespace vast {
namespace source {

/// A source reading ASCII output from the bgpdump utility.
class bgpdump : public file<bgpdump>
{
public:
  /// Spawns a BGPDump source.
  /// @param stream The input stream to read the BGPDump file from.
  bgpdump(io::file_input_stream&& stream);

  schema sniff();

  void set(schema const& sch);

  result<event> extract();

private:
  std::string separator_ = "|";
  std::string set_separator_;
  std::string empty_field_;
  std::string unset_field_;

  type announce_type_;
  type route_type_;
  type withdraw_type_;
  type state_change_type_;
};

} // namespace source
} // namespace vast

#endif
