#ifndef VAST_DETAIL_BITMAP_INDEX_ARITHMETIC_H
#define VAST_DETAIL_BITMAP_INDEX_ARITHMETIC_H

#include "vast/bitmap.h"
#include "vast/bitmap_index.h"
#include "vast/exception.h"
#include "vast/value.h"

namespace vast {
namespace detail {

/// A bitmap index for arithmetic types.
template <value_type T, typename Bitstream>
class arithmetic_bitmap_index : public bitmap_index<Bitstream>
{
  typedef underlying_value_type<T> underlying_value_type;
  typedef typename std::conditional<
    std::is_same<underlying_value_type, bool>::value,
    bitmap<bool, Bitstream>,
    typename std::conditional<
      std::is_same<underlying_value_type, double>::value,
      bitmap<double, Bitstream, range_encoder, precision_binner>,
      typename std::conditional<
        std::is_integral<underlying_value_type>::value,
        bitmap<underlying_value_type, Bitstream, range_encoder>,
        std::false_type
      >::type
    >::type
  >::type bitmap_type;

public:
  arithmetic_bitmap_index() = default;

  // The template parameter U is just a hack to allow enable_if in the
  // constructor.
  template <
    typename... Args,
    typename U = typename bitmap_type::value_type,
    typename std::enable_if<std::is_same<U, double>::value, int>::type = 0
  >
  arithmetic_bitmap_index(Args&&... args)
    : bitmap_({}, std::forward<Args>(args)...)
  {
  }

  virtual bool patch(size_t n) override
  {
    return bitmap_.patch(n);
  }

  virtual option<Bitstream>
  lookup(relational_operator op, value const& val) const override
  {
    if (op == in || op == not_in)
      throw error::operation("unsupported relational operator", op);
    if (bitmap_.empty())
      return {};
    return bitmap_.lookup(op, val.get<underlying_value_type>());
  };

  virtual std::string to_string() const override
  {
    using vast::to_string;
    return to_string(bitmap_);
  }

private:
  virtual bool push_back_impl(value const& val) override
  {
    return bitmap_.push_back(val.get<underlying_value_type>());
  }

  bitmap_type bitmap_;
};

} // namespace detail
} // namespace vast

#endif
