#ifndef VAST_LOAD_HPP
#define VAST_LOAD_HPP

#include <fstream>
#include <streambuf>
#include <type_traits>

#include <caf/stream_serializer.hpp>
#include <caf/stream_deserializer.hpp>

#include "vast/compression.hpp"
#include "vast/detail/compressedbuf.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/detail/variadic_serialization.hpp"
#include "vast/maybe.hpp"
#include "vast/filesystem.hpp"

namespace vast {

/// Deserializes a sequence of objects from a streambuffer.
/// @see save
template <
  compression Method = compression::null,
  class Streambuf,
  class T,
  class... Ts
>
auto load(Streambuf& streambuf, T&& x, Ts&&... xs)
-> std::enable_if_t<detail::is_streambuf<Streambuf>::value, maybe<void>> {
  if (Method == compression::null) {
    caf::stream_deserializer<Streambuf&> s{streambuf};
    detail::read(s, std::forward<T>(x), std::forward<Ts>(xs)...);
  } else {
    detail::compressedbuf compressed{streambuf, Method};
    caf::stream_deserializer<detail::compressedbuf&> s{compressed};
    detail::read(s, std::forward<T>(x), std::forward<Ts>(xs)...);
  }
  return {};
}

/// Deserializes a sequence of objects from a container of bytes.
/// @see save
template <
  compression Method = compression::null,
  class Container,
  class T,
  class... Ts
>
auto load(Container const& container, T&& x, Ts&&... xs)
-> std::enable_if_t<
  detail::is_contiguous_byte_container<Container>::value,
  maybe<void>
> {
  auto c = const_cast<Container*>(&container); // load() won't mess with it.
  caf::containerbuf<Container> sink{*c};
  return load<Method>(sink, std::forward<T>(x), std::forward<Ts>(xs)...);
}

/// Deserializes a sequence of objects from a file.
/// @see save
template <
  compression Method = compression::null,
  class T,
  class... Ts
>
maybe<void> load(path const& p, T&& x, Ts&&... xs) {
  std::ifstream fs{p.str()};
  return load<Method>(*fs.rdbuf(), std::forward<T>(x), std::forward<Ts>(xs)...);
}

} // namespace vast

#endif
