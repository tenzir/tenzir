#ifndef VAST_DETAIL_BITMAP_INDEX_TIME_H
#define VAST_DETAIL_BITMAP_INDEX_TIME_H

#include <ze/value.h>
#include "vast/bitmap.h"
#include "vast/bitmap_index.h"
#include "vast/exception.h"
#include "vast/to_string.h"

namespace vast {
namespace detail {

/// A bitmap index for time range and time point types.
template <typename Bitstream>
class time_bitmap_index : public bitmap_index<Bitstream>
{
  typedef ze::time_range::rep value_type;

public:
  template <typename... Args>
  time_bitmap_index(Args&&... args)
    : bitmap_({}, std::forward<Args>(args)...)
  {
  }

  virtual bool patch(size_t n) override
  {
    return bitmap_.patch(n);
  }

  virtual option<Bitstream>
  lookup(relational_operator op, ze::value const& value) const override
  {
    if (op == in || op == not_in)
      throw error::operation("unsupported relational operator", op);
    if (bitmap_.empty())
      return {};
    return bitmap_.lookup(op, extract(value));
  };

  virtual std::string to_string() const override
  {
    return vast::to_string(bitmap_);
  }

private:
  virtual bool push_back_impl(ze::value const& value) override
  {
    bitmap_.push_back(extract(value));
    return true;
  }

  static value_type extract(ze::value const& x)
  {
    switch (x.which())
    {
      default:
        throw error::index("not a time type");
      case ze::time_range_type:
        return x.get<ze::time_range>().count();
      case ze::time_point_type:
        return x.get<ze::time_point>().since_epoch().count();
    }
  }

  bitmap<value_type, Bitstream, range_encoder, precision_binner> bitmap_;
};

} // namespace detail
} // namespace vast

#endif

