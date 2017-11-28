#ifndef VAST_FORMAT_ASCII_HPP
#define VAST_FORMAT_ASCII_HPP

#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/vast/event.hpp"

#include "vast/format/stream_writer.hpp"

namespace vast {
namespace format {
namespace ascii {

struct ascii_printer : printer<ascii_printer> {
  using attribute = event;

  template <class Iterator>
  bool print(Iterator&& out, event const& e) const {
    return event_printer{}.print(out, e);
  }
};

class writer : public format::stream_writer<ascii_printer>{
public:
  using stream_writer<ascii_printer>::stream_writer;

  char const* name() const {
    return "ascii-writer";
  }
};

} // namespace ascii
} // namespace format
} // namespace vast

#endif


