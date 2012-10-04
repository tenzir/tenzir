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
    is_v4_.push_back(start == 12);
    for (size_t i = 0; i < 16; ++i)
      bitmaps_[i].push_back(i < start ? 0x00 : bytes[i]);
    return true;
  }

  virtual option<Bitstream> lookup(ze::value const& value,
                                   relational_operator op)
  {
    if (! (op == equal || op == not_equal || op == in || op == not_in))
      throw error::index("unsupported relational operator");

    if (bitmaps_[0].empty())
      return {};

    if (value.which() == ze::address_type)
    {
      auto& addr = value.get<ze::address>();
      auto& bytes = addr.data();
      size_t const start = addr.is_v4() ? 12 : 0;
      auto first = bitmaps_[start][bytes[start]];
      if (! first)
      {
        if (op == not_equal)
          return std::move(bitmaps_[0].all(false));
        return {};
      }

      *first &= is_v4_;

      for (size_t i = start + 1; i < 16; ++i)
      {
        auto bs = bitmaps_[i][bytes[i]];
        if (! bs)
        {
          if (op == not_equal)
            return std::move(bitmaps_[0].all(false));
          return {};
        }
        *first &= *bs;
      }

      if (op == not_equal)
        (*first).flip();

      return first;
    }
    else if (value.which() == ze::prefix_type)
    {
      throw error::index("prefix matching not yet implemented");
    }
    else
    {
      throw error::index("invalid value type");
    }

    return {};
  };

  virtual std::string to_string() const
  {
    throw error::index("to_string() not yet implemented");
  }

private:
  std::array<bitmap<uint8_t, Bitstream>, 16> bitmaps_;
  Bitstream is_v4_;
};

} // namespace detail
} // namespace vast

#endif
