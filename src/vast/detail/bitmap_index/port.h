#ifndef VAST_DETAIL_BITMAP_INDEX_PORT_H
#define VAST_DETAIL_BITMAP_INDEX_PORT_H

#include "vast/bitmap.h"
#include "vast/bitmap_index.h"
#include "vast/exception.h"
#include "vast/to_string.h"
#include "vast/value.h"

namespace vast {
namespace detail {

/// A bitmap index for transport-layer ports.
class port_bitmap_index : public bitmap_index
{
  using bitstream_type = null_bitstream; // TODO: Use compressed bitstream.
  typedef std::underlying_type<port::port_type>::type proto_type;

public:
  virtual bool patch(size_t n) override
  {
    auto success = num_.patch(n);
    return proto_.patch(n) && success;
  }

  virtual option<bitstream>
  lookup(relational_operator op, value const& val) const override
  {
    if (op == in || op == not_in)
      throw error::operation("unsupported relational operator", op);
    if (num_.empty())
      return {};
    auto& p = val.get<port>();
    auto nbs = num_.lookup(op, p.number());
    if (! nbs)
      return {};
    if (p.type() != port::unknown)
      if (auto tbs = num_[p.type()])
          *nbs &= *tbs;
    return {std::move(*nbs)};
  };

  virtual std::string to_string() const override
  {
    using vast::to_string;
    return to_string(num_);
  }

private:
  virtual bool push_back_impl(value const& val) override
  {
    auto& p = val.get<port>();
    num_.push_back(p.number());
    proto_.push_back(static_cast<proto_type>(p.type()));
    return true;
  }

  bitmap<uint16_t, bitstream_type, range_encoder> num_;
  bitmap<proto_type, bitstream_type> proto_;
};

} // namespace detail
} // namespace vast

#endif

