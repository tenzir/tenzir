//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/synopsis_factory.hpp"

#include "vast/address_synopsis.hpp"
#include "vast/bool_synopsis.hpp"
#include "vast/concept/hashable/default_hash.hpp"
#include "vast/string_synopsis.hpp"
#include "vast/time_synopsis.hpp"

namespace vast {

void factory_traits<synopsis>::initialize() {
  factory<synopsis>::add(legacy_address_type{},
                         make_address_synopsis<default_hash>);
  factory<synopsis>::add<legacy_bool_type, bool_synopsis>();
  factory<synopsis>::add(legacy_string_type{},
                         make_string_synopsis<default_hash>);
  factory<synopsis>::add<legacy_time_type, time_synopsis>();
}

} // namespace vast
