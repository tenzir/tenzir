#ifndef VAST_SOURCE_BRO_H
#define VAST_SOURCE_BRO_H

#include "vast/schema.h"
#include "vast/source/file.h"

namespace vast {
namespace source {

/// A Bro log file source.
class bro : public file<bro>
{
public:
  /// Spawns a Bro source.
  /// @param sch The schema to prefer over the auto-deduced type.
  /// @param filename The name of the Bro log file.
  /// @param sniff If `true`, sniff and print the schema, then exit. If
  ///              `false`, parse events..
  bro(schema sch, std::string const& filename, bool sniff = false);

  result<event> extract_impl();

  std::string describe() const final;

private:
  trial<std::string> parse_header_line(std::string const& line,
                                       std::string const& prefix);

  trial<void> parse_header();

  schema schema_;
  bool sniff_;
  int timestamp_field_ = -1;
  std::string separator_ = " ";
  std::string set_separator_;
  std::string empty_field_;
  std::string unset_field_;
  type type_;
};

} // namespace source
} // namespace vast

#endif
