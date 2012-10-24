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
  /// Destroys a bitmap index.
  virtual ~bitmap_index() = default;

  /// Appends a single value.
  /// @param value The value to add to the index.
  /// @return `true` if appending succeeded.
  bool push_back(ze::value const& value)
  {
    if (value == ze::nil)
      return patch(1);
    else
      return push_back_impl(value);
  }

  /// Appends fill material (i.e., invalid bits).
  /// @param n The number of elements to append.
  /// @return `true` on success.
  virtual bool patch(size_t n) = 0;

  /// Looks up a value with a given relational operator.
  /// @param op The relation operator.
  /// @param value The value to lookup.
  virtual option<Bitstream>
  lookup(relational_operator op, ze::value const& value) const = 0;

  /// Creates a string representation of the bitmap index.
  /// @return An `std::string` of the bitmap index.
  virtual std::string to_string() const = 0;

private:
  virtual bool push_back_impl(ze::value const& value) = 0;

  bool optional_;
  Bitstream nil_;
};

} // namespace vast

#endif
