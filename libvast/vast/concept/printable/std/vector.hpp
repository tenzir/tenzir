#ifndef VAST_CONCEPT_PRINTABLE_STD_VECTOR_HPP
#define VAST_CONCEPT_PRINTABLE_STD_VECTOR_HPP

#include <vector>

#include "vast/key.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/detail/print_delimited.hpp"

namespace vast {

template <typename T>
struct std_vector_printer : printer<std_vector_printer<T>> {
  using attribute = std::vector<T>;

  std_vector_printer(std::string const& delim = ", ") : delim_{delim} {}

  template <typename Iterator>
  bool print(Iterator& out, attribute const& a) const {
    return detail::print_delimited(a.begin(), a.end(), out, delim_);
  }

  std::string delim_;
};

template <typename T>
struct printer_registry<std::vector<T>> {
  using type = std_vector_printer<T>;
};

} // namespace vast

#endif
