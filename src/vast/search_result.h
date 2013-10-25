#ifndef VAST_SEARCH_RESULT_H
#define VAST_SEARCH_RESULT_H

#include "vast/bitstream.h"
#include "vast/util/operators.h"

namespace vast {

/// A result of a search represented as a bit sequence in the ID space.
class search_result : util::equality_comparable<search_result>,
                      util::andable<search_result>,
                      util::orable<search_result>
{
public:
  search_result() = default;
  search_result(bitstream result, bitstream coverage);

  explicit operator bool() const;
  search_result& operator&=(search_result const& other);
  search_result& operator|=(search_result const& other);

  bitstream const& hits() const;
  bitstream const& coverage() const;

private:
  bitstream hits_;
  bitstream coverage_;

private:
  friend access;
  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  friend bool operator==(search_result const& x, search_result const& y);
};

} // namespace vast

#endif
