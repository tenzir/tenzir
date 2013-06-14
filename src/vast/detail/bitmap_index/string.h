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
template <typename Bitstream>
class string_bitmap_index : public bitmap_index<Bitstream>
{
  typedef bitmap_index<Bitstream> super;
  typedef uint64_t dictionary_codomain;

public:
  virtual bool patch(size_t n) override
  {
    return bitmap_.patch(n);
  }

  virtual option<Bitstream>
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

    return op == equal ? bs : std::move((*bs).flip());
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

  bitmap<dictionary_codomain, Bitstream> bitmap_;
  util::map_dictionary<dictionary_codomain> dictionary_;
};

} // namespace detail
} // namespace vast

#endif
