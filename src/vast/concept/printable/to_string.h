#ifndef VAST_CONCEPT_PRINTABLE_TO_STRING_H
#define VAST_CONCEPT_PRINTABLE_TO_STRING_H

#include <string>
#include <type_traits>

#include "vast/concept/printable/print.h"

namespace vast {

template <typename From, typename... Opts>
auto to_string(From&& from, Opts&&... opts)
  -> std::enable_if_t<
       is_printable<
          std::back_insert_iterator<std::string>, std::decay_t<From>
        >{},
       std::string
     > {
  std::string str;
  print(std::back_inserter(str), from, std::forward<Opts>(opts)...);
  return str;
}

} // namespace vast

#endif
