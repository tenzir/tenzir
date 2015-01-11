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
  /// @param sch The schema to prefer over the auto-deduced type.
  /// @param filename The name of the BGPDump file.
  /// @param sniff If `true`, sniff and print the schema, then exit. If
  ///              `false`, parse events.
  bgpdump(schema sch, std::string const& filename, bool sniff = false);

  result<event> extract_impl();

  std::string name() const;

private:
  // Updates a type with a congruent one from the provided schema.
  trial<void> update(type& t);

  schema schema_;
  bool sniff_;
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
