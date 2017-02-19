#ifndef VAST_CONCEPT_PRINTABLE_TO_HPP
#define VAST_CONCEPT_PRINTABLE_TO_HPP

#include <string>
#include <type_traits>

#include "vast/error.hpp"
#include "vast/expected.hpp"
#include "vast/concept/printable/print.hpp"

namespace vast {

template <class To, class From, class... Opts>
auto to(From&& from, Opts&&... opts)
-> std::enable_if_t<
     std::is_same<std::string, To>{} && has_printer<std::decay_t<From>>{},
     expected<std::string>
   > {
  std::string str;
  if (!print(std::back_inserter(str), from, std::forward<Opts>(opts)...))
    return make_error(ec::print_error);
  return str;
}

} // namespace vast

#endif
