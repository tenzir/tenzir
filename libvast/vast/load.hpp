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

#pragma once

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

/// Deserializes a sequence of objects.
/// @see save
template <compression Method = compression::null, class Source, class... Ts>
expected<void> load(Source&& in, Ts&&... xs) {
  static_assert(sizeof...(Ts) > 0);
  using source_type = std::decay_t<Source>;
  if constexpr (detail::is_streambuf_v<source_type>) {
#ifndef VAST_NO_EXCEPTIONS
    try {
#endif // VAST_NO_EXCEPTIONS
      if (Method == compression::null) {
        caf::stream_deserializer<source_type&> s{in};
        detail::read(s, std::forward<Ts>(xs)...);
      } else {
        detail::compressedbuf compressed{in, Method};
        caf::stream_deserializer<detail::compressedbuf&> s{compressed};
        detail::read(s, std::forward<Ts>(xs)...);
      }
#ifndef VAST_NO_EXCEPTIONS
    } catch (const std::exception& e) {
      return make_error(ec::unspecified, e.what());
    }
#endif // VAST_NO_EXCEPTIONS
    return {};
  } else if constexpr (std::is_base_of_v<std::istream, source_type>) {
    auto sb = in.rdbuf();
    return load<Method>(*sb, std::forward<Ts>(xs)...);
  } else if constexpr (detail::is_contiguous_byte_container_v<source_type>) {
    // load() won't mess with the given reference.
    caf::containerbuf<source_type> sink{const_cast<source_type&>(in)};
    return load<Method>(sink, std::forward<Ts>(xs)...);
  } else if constexpr (std::is_same_v<source_type, path>) {
    std::ifstream fs{in.str()};
    if (!fs)
      return make_error(ec::filesystem_error, "failed to create filestream",
                        in);
    return load<Method>(*fs.rdbuf(), std::forward<Ts>(xs)...);
  } else {
    static_assert(!std::is_same_v<Source, Source>, "unexpected Source type");
  }
}

} // namespace vast

