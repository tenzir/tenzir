#ifndef VAST_CONCEPT_CONVERTIBLE_TO_H
#define VAST_CONCEPT_CONVERTIBLE_TO_H

#include <type_traits>

#include "vast/maybe.h"
#include "vast/concept/convertible/is_convertible.h"

namespace vast {

/// Converts one type to another.
/// @tparam To The type to convert `From` to.
/// @tparam From The type to convert to `To`.
/// @param from The instance to convert.
/// @returns *from* converted to `T`.
template <typename To, typename From, typename... Opts>
auto to(From&& from, Opts&&... opts)
  -> std::enable_if_t<
       is_convertible<std::decay_t<From>, To>{},
       maybe<To>
     > {
  maybe<To> x{To()};
  if (convert(from, *x, std::forward<Opts>(opts)...))
    return x;
  return nil;
}

template <typename To, typename From, typename... Opts>
auto to_string(From&& from, Opts&&... opts)
  -> std::enable_if_t<
       std::is_same<To, std::string>{}
        && is_convertible<std::decay_t<From>, To>{}, To
     > {
  std::string str;
  if (convert(from, str, std::forward<Opts>(opts)...))
    return str;
  return {};
}

} // namespace vast

#endif
