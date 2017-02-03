#ifndef VAST_CONCEPT_PRINTABLE_VAST_BITVECTOR_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_BITVECTOR_HPP

#include "vast/bitvector.hpp"
#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/string/any.hpp"
#include "vast/concept/printable/string/char.hpp"

namespace vast {

namespace policy {

struct lsb_to_msb {};
struct msb_to_lsb {};

} // namespace policy

template <class Bitvector, class Order>
struct bitvector_printer : printer<bitvector_printer<Bitvector, Order>> {
  using attribute = Bitvector;

  static constexpr bool msb_to_lsb =
    std::is_same<Order, policy::msb_to_lsb>::value;

  template <class Iterator>
  bool print(Iterator& out, Bitvector const& bv) const {
    auto render = [&](auto f, auto l) {
      for (; f != l; ++f)
        if (!printers::any.print(out, *f ? '1' : '0'))
          return false;
      return true;
    };
    if (msb_to_lsb)
      return render(bv.rbegin(), bv.rend());
    else
      return render(bv.begin(), bv.end());
  }
};

template <class Block, class Allocator>
struct printer_registry<bitvector<Block, Allocator>> {
  using type = bitvector_printer<bitvector<Block, Allocator>, policy::lsb_to_msb>;
};

} // namespace vast

#endif
