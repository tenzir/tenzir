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
public:
  /// Constructs a time bitmap index.
  /// @param precision The granularity of the index. Defaults to seconds.
  explicit time_bitmap_index(int precision = 7);

  virtual bool append(size_t n, bool bit) override;

  virtual optional<bitstream>
  lookup(relational_operator op, value const& val) const override;

  virtual uint64_t size() const override;

private:
  static time_range::rep extract(value const& val);

  virtual bool push_back_impl(value const& val) override;
  virtual bool equals(bitmap_index const& other) const override;

  bitmap<time_range::rep, bitstream_type, range_coder, precision_binner>
    bitmap_;

private:
  friend access;
  virtual void serialize(serializer& sink) const override;
  virtual void deserialize(deserializer& source) override;
  virtual bool convert(std::string& str) const override;
};

} // namespace vast

#endif

