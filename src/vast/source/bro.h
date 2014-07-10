#ifndef VAST_SOURCE_BRO_H
#define VAST_SOURCE_BRO_H

#include "vast/schema.h"
#include "vast/string.h"
#include "vast/source/file.h"

namespace vast {
namespace source {

/// A Bro log file source.
class bro : public file<bro>
{
public:
  bro(cppa::actor sink, std::string const& filename, int32_t timestamp_field);

  result<event> extract_impl();

  std::string describe() const final;

private:
  trial<void> parse_header();

  int32_t timestamp_field_ = -1;
  string separator_ = " ";
  string set_separator_;
  string empty_field_;
  string unset_field_;
  schema schema_;
  type_ptr type_;
  type_ptr flat_type_;
};

} // namespace source
} // namespace vast

#endif
