#ifndef VAST_BITMAP_INDEX_TIME_H
#define VAST_BITMAP_INDEX_TIME_H

#include "vast/bitmap.h"
#include "vast/bitmap_index.h"
#include "vast/time.h"
#include "vast/traits.h"

namespace vast {

/// A bitmap index for time range and time point types.
class time_bitmap_index : public bitmap_index
{
  using bitstream_type = null_bitstream; // TODO: Use compressed bitstream.

public:
  /// Constructs a time bitmap index.
  /// @param precision The granularity of the index. Defaults to seconds.
  time_bitmap_index(int precision = 7);

  virtual bool append(size_t n, bool bit) override;

  virtual option<bitstream>
  lookup(relational_operator op, value const& val) const override;

  virtual uint64_t size() const override;
  virtual std::string to_string() const override;

private:
  static time_range::rep extract(value const& val);

  virtual bool push_back_impl(value const& val) override;

  friend access;
  virtual void serialize(serializer& sink) const override;
  virtual void deserialize(deserializer& source) override;

  bitmap<time_range::rep, bitstream_type, range_coder, precision_binner>
    bitmap_;
};

} // namespace vast

#endif

