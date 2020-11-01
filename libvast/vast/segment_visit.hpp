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

#include "vast/die.hpp"
#include "vast/fbs/segment.hpp"
#include "vast/fwd.hpp"
#include "vast/segment.hpp"

#include <functional>
#include <type_traits>

namespace vast {

template <class Visitor>
auto visit(Visitor&& visitor, const segment& x) noexcept(
  std::conjunction_v<
    std::is_nothrow_invocable<Visitor>,
    std::is_nothrow_invocable<Visitor, const fbs::segment::v0*>>) {
  if (!x)
    return std::invoke(std::forward<Visitor>(visitor));
  switch (x->segment_type()) {
    case fbs::segment::Segment::NONE:
      return std::invoke(std::forward<Visitor>(visitor));
    case fbs::segment::Segment::v0:
      return std::invoke(std::forward<Visitor>(visitor), x->segment_as_v0());
  }
  die("unhandled segment type");
}

} // namespace vast
