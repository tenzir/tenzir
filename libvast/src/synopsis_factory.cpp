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

#include "vast/synopsis_factory.hpp"

#include "vast/address_synopsis.hpp"
#include "vast/bool_synopsis.hpp"
#include "vast/concept/hashable/xxhash.hpp"
#include "vast/string_synopsis.hpp"
#include "vast/time_synopsis.hpp"

namespace vast {

void factory_traits<synopsis>::initialize() {
  factory<synopsis>::add(address_type{}, make_address_synopsis<xxhash64>);
  factory<synopsis>::add<bool_type, bool_synopsis>();
  factory<synopsis>::add(string_type{}, make_string_synopsis<xxhash64>);
  factory<synopsis>::add<time_type, time_synopsis>();
}

} // namespace vast
