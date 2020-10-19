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

#include "vast/table_slice_builder_factory.hpp"

#include "vast/caf_table_slice.hpp"
#include "vast/caf_table_slice_builder.hpp"
#include "vast/config.hpp"
#include "vast/msgpack_table_slice.hpp"
#include "vast/msgpack_table_slice_builder.hpp"

#if VAST_HAVE_ARROW
#  include "vast/arrow_table_slice.hpp"
#  include "vast/arrow_table_slice_builder.hpp"
#endif

namespace vast {

void factory_traits<v1::table_slice_builder>::initialize() {
  using f = factory<v1::table_slice_builder>;
  f::add<v1::table_slice_builder>(v1::table_slice_builder::implementation_id);
  f::add<v1::msgpack_table_slice_builder>(
    v1::msgpack_table_slice_builder::implementation_id);
  f::add<v1::arrow_table_slice_builder>(
    v1::arrow_table_slice_builder::implementation_id);
}

void factory_traits<v0::table_slice_builder>::initialize() {
  using f = factory<v0::table_slice_builder>;
  f::add<v0::caf_table_slice_builder>(v0::caf_table_slice::class_id);
  f::add<v0::msgpack_table_slice_builder>(v0::msgpack_table_slice::class_id);
#if VAST_HAVE_ARROW
  f::add<v0::arrow_table_slice_builder>(v0::arrow_table_slice::class_id);
#endif
}

} // namespace vast
