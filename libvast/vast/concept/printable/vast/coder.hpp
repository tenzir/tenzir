#ifndef VAST_CONCEPT_PRINTABLE_VAST_CODER_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_CODER_HPP

#include "vast/bitmap_base.hpp"
#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/core/ignore.hpp"
#include "vast/concept/printable/numeric/integral.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"

namespace vast {

template <class Bitmap, class Policy = policy::expanded>
struct vector_coder_printer : printer<vector_coder_printer<Bitmap, Policy>> {
  using attribute = vector_coder<Bitmap>;

  template <class Iterator, class Coder>
  bool print(Iterator& out, const Coder& coder) const {
    if (coder.storage().empty())
      return true;
    auto i = size_t{0};
    auto key = printers::integral<size_t> ->* [&] { return i++; };
    auto row = ignore(key) << '\t' << printers::bitmap<Bitmap, Policy>;
    auto bmi = row % '\n';
    return bmi.print(out, coder.storage());
  }
};

template <class Coder>
struct printer_registry<
  Coder,
  std::enable_if_t<
    std::is_base_of<vector_coder<typename Coder::bitmap_type>, Coder>::value
  >
> {
  using type = vector_coder_printer<
    typename Coder::bitmap_type,
    policy::expanded
  >;
};

namespace printers {

template <class Bitmap, class Policy>
auto const vector_coder = vector_coder_printer<Bitmap, Policy>{};

} // namespace printers
} // namespace vast

#endif

