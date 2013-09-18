#ifndef VAST_BITMAP_INDEX_ADDRESS_H
#define VAST_BITMAP_INDEX_ADDRESS_H

#include <array>
#include "vast/bitmap.h"
#include "vast/bitmap_index.h"

namespace vast {

/// A bitmap index for IP addresses.
class address_bitmap_index : public bitmap_index
{
  using bitstream_type = null_bitstream; // TODO: Use compressed bitstream.

public:
  virtual bool append(size_t n, bool bit) override;

  virtual option<bitstream>
  lookup(relational_operator op, value const& val) const override;

  virtual uint64_t size() const override;

private:
  virtual bool push_back_impl(value const& val) override;
  virtual bool equals(bitmap_index const& other) const override;

  option<bitstream> lookup(address const& addr, relational_operator op) const;
  option<bitstream> lookup(prefix const& pfx, relational_operator op) const;

  std::array<bitmap<uint8_t, bitstream_type, binary_coder>, 16> bitmaps_;
  bitstream_type v4_;

private:
  friend access;
  virtual void serialize(serializer& sink) const override;
  virtual void deserialize(deserializer& source) override;
  virtual bool convert(std::string& str) const override;
};

} // namespace vast

#endif
