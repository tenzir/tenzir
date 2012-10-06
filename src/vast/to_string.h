#ifndef VAST_TO_STRING_H
#define VAST_TO_STRING_H

#include <string>
#include "vast/exception.h"
#include "vast/operator.h"
#include "vast/schema.h"

namespace vast {

// Forward declarations.
template <
  typename,
  typename,
  template <typename> class,
  template <typename> class
>
class bitmap;
template <typename> class bitstream;
class bitvector;
class expression;

/// Converts a bitvector to an `std::string`.
///
/// @param b The bitvector to convert.
///
/// @param msb_to_lsb The order of display. If `true`, display bits from MSB to
/// LSB and in the reverse order otherwise.
///
/// @param all Indicates whether to include also the unused bits of the last
/// block if the number of `b.size()` is not a multiple of
/// `bitvector::bits_per_block`.
///
/// @param cut_off Specifies a maximum size on the output. If 0, no cutting
/// occurs.
///
/// @return An `std::string` representation of *b*.
std::string to_string(bitvector const& b,
                      bool msb_to_lsb = true,
                      bool all = false,
                      size_t cut_off = 0);

/// Converts a bitstream to an `std::string`. Unlike a plain bitvector, we
/// print bitstreams from LSB to MSB.
///
/// @param bs The bitstream to convert.
///
/// @return An `std::string` representation of *bs*.
template <typename Derived>
std::string to_string(bitstream<Derived> const& bs)
{
  return to_string(bs.bits(), false, false, 0);
}

/// Converts a bitmap to an `std::string`.
///
/// @param bm The bitmap to convert.
///
/// @param with_header If `true`, include a header with bitmap values as first
/// row in the output.
///
/// @param delim The delimiting character separating header values.
///
/// @return An `std::string` representation of *bm*.
template <
  typename T,
  typename Bitstream,
  template <typename> class Encoder,
  template <typename> class Binner
>
std::string to_string(
    bitmap<T, Bitstream, Encoder, Binner> const& bm,
    bool with_header = true,
    char delim = '\t')
{
  if (bm.empty())
    return {};
  std::string str;
  auto& store = bm.storage();
  if (with_header)
  {
    using std::to_string;
    //using ze::to_string;
    store.each(
        [&](T const& x, Bitstream const&) { str += to_string(x) + delim; });
    str.pop_back();
    str += '\n';
  }
  std::vector<Bitstream> cols;
  cols.reserve(store.rows);
  store.each([&](T const&, Bitstream const& bs) { cols.push_back(bs); });
  for (auto& row : transpose(cols))
    str += to_string(row) + '\n';
  str.pop_back();
  return str;
}

template <
  typename Bitstream,
  template <typename> class Encoder,
  template <typename> class Binner
>
std::string to_string(
    bitmap<bool, Bitstream, Encoder, Binner> const& bm,
    bool with_header = false,
    char delim = 0x00)
{
  std::string str;
  auto& bs = bm.storage();
  auto i = bs.find_first();
  if (i == Bitstream::npos)
    throw exception("bitstream too large to convert to string");
  str.reserve(bs.size() * 2);
  if (i > 0)
    for (size_t j = 0; j < i; ++j)
      str += "0\n";
  str += "1\n";
  auto j = i;
  while ((j = bs.find_next(i)) != Bitstream::npos)
  {
    auto delta = j - i;
    for (i = 1; i < delta; ++i)
      str += "0\n";
    str += "1\n";
    i = j;
  }
  assert(j == Bitstream::npos);
  for (j = 1; j < bs.size() - i; ++i)
    str += "0\n";
  if (str.back() == '\n')
    str.pop_back();
  return str;
}

std::string to_string(boolean_operator op);
std::string to_string(arithmetic_operator op);
std::string to_string(relational_operator op);
std::string to_string(schema::type const& t);
std::string to_string(schema::type_info const& ti);
std::string to_string(schema::event const& e);
std::string to_string(schema::argument const& a);
std::string to_string(schema const& s);
std::string to_string(expression const& e);

} // namespace vast

#endif
