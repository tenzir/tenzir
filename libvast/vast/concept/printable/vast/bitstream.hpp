#ifndef VAST_CONCEPT_PRINTABLE_VAST_BITSTREAM_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_BITSTREAM_HPP

#include "vast/bitstream.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/vast/bitvector.hpp"

namespace vast {

struct null_bitstream_printer : printer<null_bitstream_printer> {
  using attribute = null_bitstream;

  template <typename Iterator>
  bool print(Iterator& out, null_bitstream const& b) const {
    static auto p = bitvector_printer<policy::lsb_to_msb>{};
    return p.print(out, b.bits());
  }
};

struct ewah_bitstream_printer : printer<ewah_bitstream_printer> {
  using attribute = ewah_bitstream;

  template <typename Iterator>
  bool print(Iterator& out, ewah_bitstream const& b) const {
    static auto p = bitvector_printer<policy::msb_to_lsb, ' ', '\n'>{};
    return p.print(out, b.bits());
  }
};

template <>
struct printer_registry<null_bitstream> {
  using type = null_bitstream_printer;
};

template <>
struct printer_registry<ewah_bitstream> {
  using type = ewah_bitstream_printer;
};

/// Transposes a vector of bitstreams into a character matrix of 0s and 1s.
/// @param out The output iterator.
/// @param v A vector of bitstreams.
template <
  typename Iterator,
  typename Bitstream,
  typename = std::enable_if_t<is_bitstream<Bitstream>::value>
>
bool print(Iterator& out, std::vector<Bitstream> const& v) {
  if (v.empty())
    return true;
  using const_iterator = typename Bitstream::const_iterator;
  using ipair = std::pair<const_iterator, const_iterator>;
  std::vector<ipair> is(v.size());
  for (size_t i = 0; i < v.size(); ++i)
    is[i] = {v[i].begin(), v[i].end()};
  auto const zero_row = std::string(v.size(), '0') + '\n';
  typename Bitstream::size_type last = 0;
  bool done = false;
  while (!done) {
    // Compute the minimum.
    typename Bitstream::size_type min = Bitstream::npos;
    for (auto& p : is)
      if (p.first != p.second && *p.first < min)
        min = *p.first;
    if (min == Bitstream::npos)
      break;
    // Fill up the distance to the last row with 0 rows.
    auto distance = min - last;
    for (decltype(min) i = 0; i < distance; ++i)
      std::copy(zero_row.begin(), zero_row.end(), out);
    last = min + 1;
    // Print the current transposed row.
    done = true;
    for (auto& p : is) {
      if (p.first != p.second && *p.first == min) {
        *out++ = '1';
        done = false;
        ++p.first;
      } else {
        *out++ = '0';
      }
    }
    *out++ = '\n';
  }
  return true;
}

} // namespace vast

#endif
