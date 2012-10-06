#ifndef VAST_BITMAP_INDEX_H
#define VAST_BITMAP_INDEX_H

#include "vast/operator.h"
#include "vast/option.h"

namespace vast {

/// The abstract base class for bitmap indexes.
template <typename Bitstream>
class bitmap_index
{
public:
  virtual ~bitmap_index() = default;

  /// Appends a single value.
  /// @param value The value to add to the index.
  /// @return `true` if appending succeeded.
  virtual bool push_back(ze::value const& value) = 0;

  /// Looks up a value with under given relational operator.
  /// @param op The relation operator.
  /// @param value The value to lookup.
  virtual option<Bitstream> lookup(relational_operator op,
                                   ze::value const& value) const = 0;

  /// Creates a string representation of the bitmap index.
  /// @return An `std::string` of the bitmap index.
  virtual std::string to_string() const = 0;

protected:
  uint64_t base_id_;
};

} // namespace vast

#endif
