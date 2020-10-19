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

#include "vast/table_slice_encoding.hpp"

#include <array>
#include <type_traits>

namespace vast {

namespace v1 {

const char* to_string(table_slice_encoding encoding) {
  static constexpr std::array descriptions = {
    "invalid",
    "msgpack",
    "arrow",
  };
  using underlying = std::underlying_type_t<table_slice_encoding>;
  static_assert(descriptions.size()
                == static_cast<underlying>(table_slice_encoding::COUNT));
  return descriptions[static_cast<underlying>(encoding)];
}

} // namespace v1

} // namespace vast
