#ifndef VAST_CONCEPT_PRINTABLE_TO_H
#define VAST_CONCEPT_PRINTABLE_TO_H

#include <string>
#include <type_traits>

#include "vast/optional.h"
#include "vast/concept/printable/print.h"

namespace vast {

template <typename To, typename From, typename... Opts>
auto to(From&& from, Opts&&... opts)
  -> std::enable_if_t<
       std::is_same<std::string, To>{} && has_printer<std::decay_t<From>>{},
       optional<std::string>
     >
{
  std::string str;
  if (! print(std::back_inserter(str), from, std::forward<Opts>(opts)...))
    return nil;
  return optional<std::string>{std::move(str)};
}

} // namespace vast

#endif
