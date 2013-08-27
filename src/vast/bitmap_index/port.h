#ifndef VAST_BITMAP_INDEX_PORT_H
#define VAST_BITMAP_INDEX_PORT_H

#include "vast/bitmap.h"
#include "vast/bitmap_index.h"
#include "vast/port.h"

namespace vast {

/// A bitmap index for transport-layer ports.
class port_bitmap_index : public bitmap_index
{
  using bitstream_type = null_bitstream; // TODO: Use compressed bitstream.
  using proto_type = std::underlying_type<port::port_type>::type;

public:
  virtual bool append(size_t n, bool bit) override;

  virtual option<bitstream>
  lookup(relational_operator op, value const& val) const override;

  virtual uint64_t size() const override;
  virtual std::string to_string() const override;

private:
  virtual bool push_back_impl(value const& val) override;

  friend access;
  virtual void serialize(serializer& sink) const override;
  virtual void deserialize(deserializer& source) override;

  bitmap<uint16_t, bitstream_type, range_coder> num_;
  bitmap<proto_type, bitstream_type> proto_;
};

} // namespace vast

#endif

