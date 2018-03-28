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

#include "vast/subnet.hpp"
#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/numeric/integral.hpp"
#include "vast/concept/printable/string/char.hpp"
#include "vast/concept/printable/vast/address.hpp"

namespace vast {

struct subnet_printer : printer<subnet_printer> {
  using attribute = subnet;

  template <class Iterator>
  bool print(Iterator& out, const subnet& sn) const {
    using namespace printers;
    return (addr << chr<'/'> << u8)(out, sn.network(), sn.length());
  }
};

template <>
struct printer_registry<subnet> {
  using type = subnet_printer;
};

} // namespace vast

