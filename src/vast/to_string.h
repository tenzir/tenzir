#ifndef VAST_TO_STRING_H
#define VAST_TO_STRING_H

#include <string>
#include "vast/schema.h"

namespace vast {

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

template <typename Bitstream>
std::string to_string(Bitstream const& b)
{
  return to_string(b.bits());
}

std::string to_string(schema::type const& t);
std::string to_string(schema::type_info const& ti);
std::string to_string(schema::event const& e);
std::string to_string(schema::argument const& a);
std::string to_string(schema const& s);
std::string to_string(expression const& e);

} // namespace vast

#endif
