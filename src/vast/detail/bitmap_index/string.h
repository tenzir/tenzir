#ifndef VAST_DETAIL_BITMAP_INDEX_STRING_H
#define VAST_DETAIL_BITMAP_INDEX_STRING_H

#include "vast/bitmap.h"
#include "vast/bitmap_index.h"
#include "vast/exception.h"
#include "vast/to_string.h"
#include "vast/value.h"
#include "vast/util/dictionary.h"

namespace vast {
namespace detail {

/// A bitmap index for strings. It uses a @link dictionary
/// vast::util::dictionary@endlink to map each string to a unique numeric value
/// to be used by the bitmap.
class string_bitmap_index : public bitmap_index
{
  using bitstream_type = null_bitstream; // TODO: Use compressed bitstream.
  using dictionary_codomain = uint64_t;

public:
  virtual bool patch(size_t n) override
  {
    return bitmap_.patch(n);
  }

  virtual option<bitstream>
  lookup(relational_operator op, value const& val) const override
  {
    if (! (op == equal || op == not_equal))
      throw error::operation("unsupported relational operator", op);

    auto str = vast::to_string(val.get<string>());
    auto i = dictionary_[str];
    if (! i)
      return {};

    auto bs = bitmap_[*i];
    if (! bs)
      return {};

    return {std::move(op == equal ? *bs : bs->flip())};
  };

  virtual std::string to_string() const override
  {
    using vast::to_string;
    return to_string(bitmap_);
  }

private:
  virtual bool push_back_impl(value const& val) override
  {
    using vast::to_string;
    auto str = to_string(val.get<string>());
    auto i = dictionary_[str];
    if (!i)
      i = dictionary_.insert(str);
    if (!i)
      return false;

    return bitmap_.push_back(*i);
  }

  bitmap<dictionary_codomain, bitstream_type> bitmap_;
  util::map_dictionary<dictionary_codomain> dictionary_;
};

} // namespace detail
} // namespace vast

#endif
