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

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/data.hpp"
#include "vast/detail/narrow.hpp"

namespace vast {

struct yaml_parser : parser<yaml_parser> {
  using attribute = data;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    auto str = std::string_view{f, detail::narrow_cast<size_t>(l - f)};
    if (auto yaml = from_yaml(str)) {
      a = std::move(*yaml);
      f = l;
      return true;
    }
    return false;
  }
};

namespace parsers {

static auto const yaml = yaml_parser{};

} // namespace parsers

} // namespace vast
