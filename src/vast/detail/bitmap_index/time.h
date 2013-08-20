#ifndef VAST_DETAIL_BITMAP_INDEX_TIME_H
#define VAST_DETAIL_BITMAP_INDEX_TIME_H

#include "vast/bitmap.h"
#include "vast/bitmap_index.h"
#include "vast/exception.h"
#include "vast/to_string.h"
#include "vast/value.h"

namespace vast {
namespace detail {

/// A bitmap index for time range and time point types.
class time_bitmap_index : public bitmap_index
{
  using bitstream_type = null_bitstream; // TODO: Use compressed bitstream.
  typedef time_range::rep value_type;

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

  virtual option<bitstream>
  lookup(relational_operator op, value const& val) const override
  {
    if (op == in || op == not_in)
      throw error::operation("unsupported relational operator", op);
    if (bitmap_.empty())
      return {};
    auto result = bitmap_.lookup(op, extract(val));
    if (! result)
      return {};
    return {std::move(*result)};
  };

  virtual std::string to_string() const override
  {
    using vast::to_string;
    return to_string(bitmap_);
  }

private:
  virtual bool push_back_impl(value const& val) override
  {
    bitmap_.push_back(extract(val));
    return true;
  }

  static value_type extract(value const& val)
  {
    switch (val.which())
    {
      default:
        throw error::index("not a time type");
      case time_range_type:
        return val.get<time_range>().count();
      case time_point_type:
        return val.get<time_point>().since_epoch().count();
    }
  }

  bitmap<value_type, bitstream_type, range_encoder, precision_binner> bitmap_;
};

} // namespace detail
} // namespace vast

#endif

