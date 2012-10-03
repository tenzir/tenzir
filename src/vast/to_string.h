#ifndef VAST_TO_STRING_H
#define VAST_TO_STRING_H

#include <string>
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
/// @param all Indicates whether to include also the unused bits of the last
/// block if the number of `b.size()` is not a multiple of
/// `bitvector::bits_per_block`.
///
/// @param cut_off Specifies a maximum size on the output.
///
/// @return An `std::string` representation of *b*.
std::string to_string(bitvector const& b,
                      bool msb_to_lsb = true,
                      bool all = false,
                      size_t cut_off = 64);

/// Converts a bitstream to an `std::string`.
///
/// @param bs The bitstream to convert.
///
/// @return An `std::string` representation of *bs*.
template <typename Derived>
std::string to_string(bitstream<Derived> const& bs)
{
  return to_string(bs.bits());
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
  std::string str;
  std::vector<T> header;
  auto t = bm.transpose(with_header ? &header : nullptr);
  if (with_header)
  {
    auto first = header.begin();
    auto last = header.end();
    while (first != last)
    {
      using std::to_string;
      str += to_string(*first);
      if (++first != last)
        str += delim;
    }
    str += '\n';
  }

  auto first = t.begin();
  auto last = t.end();
  while (first != last)
  {
    str += to_string(first->bits(), false);
    if (++first != last)
      str += '\n';
  }

  return str;
}

std::string to_string(schema::type const& t);
std::string to_string(schema::type_info const& ti);
std::string to_string(schema::event const& e);
std::string to_string(schema::argument const& a);
std::string to_string(schema const& s);
std::string to_string(expression const& e);

} // namespace vast

#endif
