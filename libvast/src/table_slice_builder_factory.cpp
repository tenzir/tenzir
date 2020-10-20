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

#include "vast/arrow_table_slice.hpp"
#include "vast/arrow_table_slice_builder.hpp"
#include "vast/config.hpp"
#include "vast/msgpack_table_slice.hpp"
#include "vast/msgpack_table_slice_builder.hpp"

namespace vast {

void factory_traits<table_slice_builder>::initialize() {
  using f = factory<table_slice_builder>;
  f::add<table_slice_builder>(table_slice_builder::implementation_id);
  f::add<msgpack_table_slice_builder>(
    msgpack_table_slice_builder::implementation_id);
  f::add<arrow_table_slice_builder>(
    arrow_table_slice_builder::implementation_id);
}

} // namespace vast
