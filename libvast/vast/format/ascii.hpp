#ifndef VAST_FORMAT_ASCII_HPP
#define VAST_FORMAT_ASCII_HPP

#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/vast/event.hpp"

#include "vast/format/writer.hpp"

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

class writer : public format::writer<ascii_printer>{
public:
  using format::writer<ascii_printer>::writer;

  char const* name() const {
    return "ascii-writer";
  }
};

} // namespace ascii
} // namespace format
} // namespace vast

#endif


