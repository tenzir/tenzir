#ifndef VAST_DETAIL_BITMAP_INDEX_ADDRESS_H
#define VAST_DETAIL_BITMAP_INDEX_ADDRESS_H

#include "vast/bitmap.h"
#include "vast/bitmap_index.h"
#include "vast/exception.h"
#include "vast/to_string.h"
#include "vastue.h"

namespace vast {
namespace detail {

/// A bitmap index for IP addresses.
template <typename Bitstream>
class address_bitmap_index : public bitmap_index<Bitstream>
{
  typedef bitmap_index<Bitstream> super;

public:
  virtual bool patch(size_t n) override
  {
    bool success = true;
    for (size_t i = 0; i < 16; ++i)
      if (! bitmaps_[i].patch(1))
        success = false;
    return v4_.append(1, false) && success;
  }

  virtual option<Bitstream>
  lookup(relational_operator op, value const& val) const override
  {
    if (! (op == equal || op == not_equal || op == in || op == not_in))
      throw error::operation("unsupported relational operator", op);

    if (v4_.empty())
      return {};

    switch (val.which())
    {
      default:
        throw error::index("invalid value type");
      case address_type:
        return lookup(val.get<address>(), op);
      case prefix_type:
        return lookup(val.get<prefix>(), op);
    }
  };

  virtual std::string to_string() const override
  {
    std::vector<Bitstream> v;
    v.reserve(128);
    for (size_t i = 0; i < 128; ++i)
      v.push_back(*bitmaps_[i / 8].storage().find(7 - i % 8));
    std::string str;
    for (auto& row : transpose(v))
      str += vast::to_string(row) + '\n';
    str.pop_back();
    return str;
  }

private:
  virtual bool push_back_impl(value const& val) override
  {
    auto& addr = val.get<address>();
    auto& bytes = addr.data();
    size_t const start = addr.is_v4() ? 12 : 0;
    auto success = v4_.push_back(start == 12);
    for (size_t i = 0; i < 16; ++i)
      if (! bitmaps_[i].push_back(i < start ? 0x00 : bytes[i]))
        success = false;
    return success;
  }

  option<Bitstream> lookup(address const& addr, relational_operator op) const
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

  option<Bitstream> lookup(prefix const& pfx, relational_operator op) const
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
