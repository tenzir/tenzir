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

#include <caf/stream_serializer.hpp>

#include "vast/compression.hpp"
#include "vast/detail/compressedbuf.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/error.hpp"
#include "vast/expected.hpp"
#include "vast/filesystem.hpp"

namespace vast {

/// Serializes a sequence of objects into a sink.
/// @see load
template <compression Method = compression::null, class Sink, class... Ts>
expected<void> save(Sink&& out, const Ts&... xs) {
  static_assert(sizeof...(Ts) > 0);
  using sink_type = std::decay_t<Sink>;
  if constexpr (detail::is_streambuf_v<sink_type>) {
    if (Method == compression::null) {
      caf::stream_serializer<sink_type&> s{out};
      if (auto err = s(xs...))
        return err;
    } else {
      detail::compressedbuf compressed{out, Method};
      caf::stream_serializer<detail::compressedbuf&> s{compressed};
      if (auto err = s(xs...))
        return err;
      compressed.pubsync();
    }
    return {};
  } else if constexpr (std::is_base_of_v<std::ostream, sink_type>) {
    auto sb = out.rdbuf();
    return save<Method>(*sb, xs...);
  } else if constexpr (detail::is_contiguous_byte_container_v<sink_type>) {
    caf::containerbuf<sink_type> sink{out};
    return save<Method>(sink, xs...);
  } else if constexpr (std::is_same_v<sink_type, path>) {
    std::ofstream fs{out.str()};
    if (!fs)
      return make_error(ec::filesystem_error, "failed to create filestream",
                        out);
    return save<Method>(*fs.rdbuf(), xs...);
  } else {
    static_assert(!std::is_same_v<Sink, Sink>, "unexpected Sink type");
  }
}

} // namespace vast

