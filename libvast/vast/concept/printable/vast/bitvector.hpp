#ifndef VAST_CONCEPT_PRINTABLE_VAST_BITVECTOR_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_BITVECTOR_HPP

#include "vast/bitvector.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/any.hpp"
#include "vast/concept/printable/string/char.hpp"

namespace vast {

namespace policy {

struct lsb_to_msb {};
struct msb_to_lsb {};

} // namespace policy

template <
  typename BlockOrder,
  char UnusedBit = '\0',
  char BlockDivider = '\0'
>
struct bitvector_printer
  : printer<bitvector_printer<BlockOrder, UnusedBit, BlockDivider>> {
  using attribute = bitvector;

  static constexpr bool msb_to_lsb
    = std::is_same<BlockOrder, policy::msb_to_lsb>{};

  static constexpr char unused_bit = UnusedBit;

  template <typename Iterator>
  static bool print_block_divider(Iterator& out) {
    if (BlockDivider == '\0')
      return true;
    return printers::any.print(out, BlockDivider);
  }

  template <typename Iterator, typename Block>
  static bool print_block(Iterator& out, Block block, size_t from = 0,
                          size_t to = bitvector::block_width) {
    while (from < to) {
      char c;
      if (msb_to_lsb)
        c = (block & (Block{1} << (to - from++ - 1))) ? '1' : '0';
      else
        c = (block & (Block{1} << from++)) ? '1' : '0';
      if (!printers::any.print(out, c))
        return false;
    }
    return true;
  }

  template <typename Iterator>
  static bool print_block_range(Iterator& out, bitvector const& b, size_t from,
                                size_t to) {
    if (from == to)
      return true;
    if (!print_block(out, b.block(from++)))
      return false;
    while (from != to)
      if (!(print_block_divider(out) && print_block(out, b.block(from++))))
        return false;
    return true;
  }

  template <typename Iterator>
  static bool print_unused_bits(Iterator& out, bitvector const& b) {
    if (unused_bit != '\0' && b.extra_bits() > 0)
      for (auto i = 0u; i < bitvector::block_width - b.extra_bits(); ++i)
        if (!printers::any.print(out, unused_bit))
          return false;
    return true;
  }

  template <typename Iterator>
  static bool print_last_block(Iterator& out, bitvector const& b) {
    auto bits = b.extra_bits() == 0 ? bitvector::block_width : b.extra_bits();
    if (msb_to_lsb)
      return print_unused_bits(out, b)
             && print_block(out, b.last_block(), 0, bits);
    else
      return print_block(out, b.last_block(), 0, bits)
             && print_unused_bits(out, b);
  }

  template <typename Iterator>
  bool print(Iterator& out, bitvector const& b) const {
    if (b.empty())
      return true;
    if (!print_block_range(out, b, 0, b.blocks() - 1))
      return false;
    if (b.blocks() > 1 && !print_block_divider(out))
      return false;
    return print_last_block(out, b);
  }
};

template <>
struct printer_registry<bitvector> {
  using type = bitvector_printer<policy::msb_to_lsb>;
};

} // namespace vast

#endif
