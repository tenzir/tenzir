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

#ifndef VAST_SAVE_HPP
#define VAST_SAVE_HPP

#include <fstream>
#include <stdexcept>
#include <streambuf>
#include <type_traits>

#include <caf/stream_serializer.hpp>

#include "vast/compression.hpp"
#include "vast/detail/compressedbuf.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/detail/variadic_serialization.hpp"
#include "vast/error.hpp"
#include "vast/expected.hpp"
#include "vast/filesystem.hpp"

namespace vast {

/// Serializes a sequence of objects into a streambuffer.
/// @see load
template <
  compression Method = compression::null,
  class Streambuf,
  class T,
  class... Ts
>
auto save(Streambuf& streambuf, T&& x, Ts&&... xs)
-> std::enable_if_t<detail::is_streambuf<Streambuf>::value, expected<void>> {
  try {
    if (Method == compression::null) {
      caf::stream_serializer<Streambuf&> s{streambuf};
      detail::write(s, std::forward<T>(x), std::forward<Ts>(xs)...);
    } else {
      detail::compressedbuf compressed{streambuf, Method};
      caf::stream_serializer<detail::compressedbuf&> s{compressed};
      detail::write(s, std::forward<T>(x), std::forward<Ts>(xs)...);
      compressed.pubsync();
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
expected<void> save(std::ostream& os, T&& x, Ts&&... xs) {
  auto sb = os.rdbuf();
  return save<Method>(*sb, std::forward<T>(x), std::forward<Ts>(xs)...);
}

/// Serializes a sequence of objects into a container of bytes.
/// @see load
template <
  compression Method = compression::null,
  class Container,
  class T,
  class... Ts
>
auto save(Container& container, T&& x, Ts&&... xs)
-> std::enable_if_t<
  detail::is_contiguous_byte_container<Container>::value,
  expected<void>
> {
  caf::containerbuf<Container> sink{container};
  return save<Method>(sink, std::forward<T>(x), std::forward<Ts>(xs)...);
}

/// Serializes a sequence of objects into a file.
/// @see load
template <
  compression Method = compression::null,
  class T,
  class... Ts
>
expected<void> save(path const& p, T&& x, Ts&&... xs) {
  std::ofstream fs{p.str()};
  if (!fs)
    return make_error(ec::filesystem_error, "failed to create filestream", p);
  return save<Method>(*fs.rdbuf(), std::forward<T>(x), std::forward<Ts>(xs)...);
}

} // namespace vast

#endif
