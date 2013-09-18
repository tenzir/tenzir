#ifndef VAST_BITMAP_INDEX_ARITHMETIC_H
#define VAST_BITMAP_INDEX_ARITHMETIC_H

#include "vast/bitmap.h"
#include "vast/bitmap_index.h"
#include "vast/exception.h"
#include "vast/value.h"

namespace vast {

/// A bitmap index for arithmetic types.
template <value_type T>
class arithmetic_bitmap_index : public bitmap_index
{
  using bitstream_type = null_bitstream; // TODO: Use compressed bitstream.
  using underlying_value_type = underlying_value_type<T>;
  using bitmap_type =
    typename std::conditional<
      std::is_same<underlying_value_type, bool>::value,
      bitmap<bool, bitstream_type>,
      typename std::conditional<
        std::is_same<underlying_value_type, double>::value,
        bitmap<double, bitstream_type, range_coder, precision_binner>,
        typename std::conditional<
          std::is_integral<underlying_value_type>::value,
          bitmap<underlying_value_type, bitstream_type, range_coder>,
          std::false_type
        >::type
      >::type
    >::type;

public:
  arithmetic_bitmap_index() = default;

  template <
    typename U = underlying_value_type,
    typename = EnableIf<std::is_same<U, double>>
  >
  explicit arithmetic_bitmap_index(int precision)
    : bitmap_({precision}, {})
  {
  }

  virtual bool append(size_t n, bool bit) override
  {
    return bitmap_.append(n, bit);
  }

  virtual option<bitstream>
  lookup(relational_operator op, value const& val) const override
  {
    if (op == in || op == not_in)
        throw std::runtime_error(
            "unsupported relational operator: " + to<std::string>(op));
    if (bitmap_.empty())
      return {};
    auto result = bitmap_.lookup(op, val.get<underlying_value_type>());
    if (! result)
      return {};
    return {std::move(*result)};
  };

  virtual uint64_t size() const override
  {
    return bitmap_.size();
  }

private:
  virtual bool push_back_impl(value const& val) override
  {
    return bitmap_.push_back(val.get<underlying_value_type>());
  }

  virtual bool equals(bitmap_index const& other) const override
  {
    if (typeid(*this) != typeid(other))
      return false;
    auto& o = static_cast<arithmetic_bitmap_index<T> const&>(other);
    return bitmap_ == o.bitmap_;
  }

  bitmap_type bitmap_;

private:
  friend access;

  virtual void serialize(serializer& sink) const override
  {
    sink << bitmap_;
  }

  virtual void deserialize(deserializer& source) override
  {
    source >> bitmap_;
  }

  virtual bool convert(std::string& str) const override
  {
    using vast::convert;
    return convert(bitmap_, str);
  }
};

} // namespace vast

#endif
