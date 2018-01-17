/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#ifndef VAST_LOAD_HPP
#define VAST_LOAD_HPP

#include <fstream>
#include <stdexcept>
#include <streambuf>
#include <type_traits>

#include <caf/stream_deserializer.hpp>
#include <caf/streambuf.hpp>

#include "vast/compression.hpp"
#include "vast/detail/compressedbuf.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/detail/variadic_serialization.hpp"
#include "vast/error.hpp"
#include "vast/expected.hpp"
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
-> std::enable_if_t<detail::is_streambuf<Streambuf>::value, expected<void>> {
  try {
    if (Method == compression::null) {
      caf::stream_deserializer<Streambuf&> s{streambuf};
      detail::read(s, std::forward<T>(x), std::forward<Ts>(xs)...);
    } else {
      detail::compressedbuf compressed{streambuf, Method};
      caf::stream_deserializer<detail::compressedbuf&> s{compressed};
      detail::read(s, std::forward<T>(x), std::forward<Ts>(xs)...);
    }
  } catch (std::exception const& e) {
    return make_error(ec::unspecified, e.what());
  }
  return {};
}

template <
  compression Method = compression::null,
  class T,
  class... Ts
>
expected<void> load(std::istream& is, T&& x, Ts&&... xs) {
  auto sb = is.rdbuf();
  return load<Method>(*sb, std::forward<T>(x), std::forward<Ts>(xs)...);
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
  expected<void>
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
expected<void> load(path const& p, T&& x, Ts&&... xs) {
  std::ifstream fs{p.str()};
  if (!fs)
    return make_error(ec::filesystem_error, "failed to create filestream", p);
  return load<Method>(*fs.rdbuf(), std::forward<T>(x), std::forward<Ts>(xs)...);
}

} // namespace vast

#endif
