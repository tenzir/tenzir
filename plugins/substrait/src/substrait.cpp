//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "substrait/substrait.hpp"

#include "substrait/plan.pb.h"

#include <vast/error.hpp>

namespace vast::plugins::substrait {

[[nodiscard]] caf::expected<vast::expression>
parse_substrait(const ::substrait::Plan& plan) {
  if (plan.relations_size() == 0)
    return caf::make_error(ec::unimplemented, "no relations");
  auto& relation = plan.relations(0);
  // [...]
  return caf::make_error(ec::unimplemented, "TODO!");
}

} // namespace vast::plugins::substrait
