//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/table_slice_builder_factory.hpp"

#include "vast/arrow_table_slice_builder.hpp"
#include "vast/config.hpp"
#include "vast/experimental_table_slice_builder.hpp"

namespace vast {

void factory_traits<table_slice_builder>::initialize() {
  using f = factory<table_slice_builder>;
  f::add<experimental_table_slice_builder>(table_slice_encoding::arrow);
  // The MsgPack table slice is deprecated, so instead we simply use the Arrow
  // table slice builder instead here.
  f::add<arrow_table_slice_builder>(table_slice_encoding::msgpack);
}

} // namespace vast
