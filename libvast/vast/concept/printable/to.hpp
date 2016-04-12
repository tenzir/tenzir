#ifndef VAST_CONCEPT_PRINTABLE_TO_HPP
#define VAST_CONCEPT_PRINTABLE_TO_HPP

#include <string>
#include <type_traits>

#include "vast/error.hpp"
#include "vast/maybe.hpp"
#include "vast/concept/printable/print.hpp"

namespace vast {

template <typename To, typename From, typename... Opts>
auto to(From&& from, Opts&&... opts)
-> std::enable_if_t<
     std::is_same<std::string, To>{} && has_printer<std::decay_t<From>>{},
     maybe<std::string>
   > {
  std::string str;
  if (! print(std::back_inserter(str), from, std::forward<Opts>(opts)...))
    return fail<ec::print_error>();
  return maybe<std::string>{std::move(str)};
}

} // namespace vast

#endif
