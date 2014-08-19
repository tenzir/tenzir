#ifndef VAST_SOURCE_BRO_H
#define VAST_SOURCE_BRO_H

#include "vast/source/file.h"

namespace vast {
namespace source {

/// A Bro log file source.
class bro : public file<bro>
{
public:
  bro(caf::actor sink, std::string const& filename, int32_t timestamp_field);

  result<event> extract_impl();

  std::string describe() const final;

private:
  trial<std::string> parse_header_line(std::string const& line,
                                       std::string const& prefix);

  trial<void> parse_header();

  int32_t timestamp_field_ = -1;
  std::string separator_ = " ";
  std::string set_separator_;
  std::string empty_field_;
  std::string unset_field_;
  type type_;
};

} // namespace source
} // namespace vast

#endif
