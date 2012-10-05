#ifndef VAST_DETAIL_BITMAP_INDEX_ADDRESS_H
#define VAST_DETAIL_BITMAP_INDEX_ADDRESS_H

#include <ze/value.h>
#include "vast/bitmap.h"
#include "vast/bitmap_index.h"
#include "vast/exception.h"
#include "vast/to_string.h"

namespace vast {
namespace detail {

/// A bitmap index for IP addresses.
template <typename Bitstream>
class address_bitmap_index : public bitmap_index<Bitstream>
{
  typedef bitmap_index<Bitstream> super;

public:
  virtual bool push_back(ze::value const& value)
  {
    auto& addr = value.get<ze::address>();
    auto& bytes = addr.data();
    size_t const start = addr.is_v4() ? 12 : 0;
    v4_.push_back(start == 12);
    for (size_t i = 0; i < 16; ++i)
      bitmaps_[i].push_back(i < start ? 0x00 : bytes[i]);
    return true;
  }

  virtual option<Bitstream> lookup(ze::value const& value,
                                   relational_operator op) const
  {
    if (! (op == equal || op == not_equal || op == in || op == not_in))
      throw error::operation("unsupported relational operator", op);

    if (v4_.empty())
      return {};

    switch (value.which())
    {
      default:
        throw error::index("invalid value type");
      case ze::address_type:
        return lookup(value.get<ze::address>(), op);
      case ze::prefix_type:
        return lookup(value.get<ze::prefix>(), op);
    }
  };

  virtual std::string to_string() const
  {
    throw error::index("to_string() not yet implemented");
  }

private:
  option<Bitstream> lookup(ze::address const& addr, relational_operator op) const
  {
    auto& bytes = addr.data();
    auto is_v4 = addr.is_v4();
    option<Bitstream> result = is_v4 ? v4_ : Bitstream(v4_.size(), true);
    for (size_t i = is_v4 ? 12 : 0; i < 16; ++ i)
      if (auto bs = bitmaps_[i][bytes[i]])
        *result &= *bs;
      else if (op == not_equal)
        return std::move(Bitstream(v4_.size(), true));
      else
        return {};

    if (op == not_equal)
      (*result).flip();
    return result;
  }

  option<Bitstream> lookup(ze::prefix const& pfx, relational_operator op) const
  {
    if (! (op == in || op == not_in))
      throw error::operation("unsupported relational operator", op);

    auto topk = pfx.length();
    if (topk == 0)
      throw error::index("invalid IP prefix length");

    auto net = pfx.network();
    auto is_v4 = net.is_v4();
    if ((is_v4 ? topk + 96 : topk) == 128)
      return lookup(pfx.network(), op == in ? equal : not_equal);

    option<Bitstream> result = is_v4 ? v4_ : Bitstream(v4_.size(), true);
    auto bit = topk;
    auto& bytes = net.data();
    for (size_t i = is_v4 ? 12 : 0; i < 16; ++ i)
      for (size_t j = 8; j --> 0; )
      {
        if (auto bs = bitmaps_[i].storage().find(j))
          *result &= ((bytes[i] >> j) & 1) ? *bs : ~*bs;
        else
          throw error::index("corrupt index: bit must exist");

        if (! --bit)
        {
          if (op == not_in)
            (*result).flip();
          return result;
        }
      }

    return {};
  }

  std::array<bitmap<uint8_t, Bitstream, binary_encoder>, 16> bitmaps_;
  Bitstream v4_;
};

} // namespace detail
} // namespace vast

#endif
