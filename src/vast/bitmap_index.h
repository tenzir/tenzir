#ifndef VAST_BITMAP_INDEX_H
#define VAST_BITMAP_INDEX_H

#include "vast/operator.h"
#include "vast/option.h"

namespace vast {

class bitstream;

/// The abstract base class for bitmap indexes.
class bitmap_index
{
public:
  /// Factory function to construct a bitmap index for a given value type.
  /// @param t The value type to create an index for.
  /// @return A bitmap index suited for type *t*.
  static std::unique_ptr<bitmap_index> create(value_type t);

  /// Destroys a bitmap index.
  virtual ~bitmap_index() = default;

  /// Appends a single value.
  /// @param val The value to add to the index.
  /// @return `true` if appending succeeded.
  bool push_back(value const& val);

  /// Appends fill material (i.e., invalid bits).
  /// @param n The number of elements to append.
  /// @param bit The value of the bits to append.
  /// @return `true` on success.
  virtual bool append(size_t n, bool bit) = 0;

  /// Looks up a value with a given relational operator.
  /// @param op The relation operator.
  /// @param val The value to lookup.
  virtual option<bitstream>
  lookup(relational_operator op, value const& val) const = 0;

  /// Retrieves the number of elements in the bitmap index.
  /// @return The number of rows, i.e., values in the bitmap.
  virtual uint64_t size() const = 0;

  /// Checks whether the bitmap is empty.
  /// @return `true` if `size() == 0`.
  bool empty() const;

private:
  virtual bool push_back_impl(value const& val) = 0;

private:
  friend access;

  virtual void serialize(serializer& sink) const = 0;
  virtual void deserialize(deserializer& source) = 0;
  virtual bool convert(std::string& str) const = 0;
};

} // namespace vast

#endif
