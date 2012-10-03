#ifndef VAST_BITMAP_INDEX_H
#define VAST_BITMAP_INDEX_H

#include "vast/bitmap.h"
#include "vast/operator.h"

namespace vast {

/// The abstract base class for bitmap indexes.
template <typename Bitstream>
class bitmap_index
{
public:
  virtual ~bitmap_index() = default;
  virtual void append(size_t n, bool bit) = 0;
  virtual bool push_back(ze::value const& value) = 0;
  virtual Bitstream lookup(ze::value const& value, relational_operator op) = 0;
  virtual std::string to_string() const = 0;

protected:
  uint64_t base_id_;
};

} // namespace vast

#endif
