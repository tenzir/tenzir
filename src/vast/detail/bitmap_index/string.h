#ifndef VAST_DETAIL_BITMAP_INDEX_STRING_H
#define VAST_DETAIL_BITMAP_INDEX_STRING_H

#include <ze/value.h>
#include <ze/to_string.h>
#include "vast/bitmap_index.h"
#include "vast/to_string.h"
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
  virtual void append(size_t n, bool bit)
  {
    bitmap_.append(n, bit);
  };

  virtual bool push_back(ze::value const& value)
  {
    auto str = ze::to_string(value.get<ze::string>());
    auto i = dictionary_[str];
    if (!i)
      i = dictionary_.insert(str);
    if (!i)
      return false;

    bitmap_.push_back(*i);
    return true;
  }

  virtual Bitstream lookup(ze::value const& value, relational_operator op)
  {
    auto str = ze::to_string(value.get<ze::string>());
    auto i = dictionary_[str];
    if (!i)
      return {};
    return bitmap_[*i];
  };

  virtual std::string to_string() const
  {
    return vast::to_string(bitmap_);
  }

private:
  bitmap<dictionary_codomain, Bitstream> bitmap_;
  util::map_dictionary<dictionary_codomain> dictionary_;
};

} // namespace detail
} // namespace vast

#endif
