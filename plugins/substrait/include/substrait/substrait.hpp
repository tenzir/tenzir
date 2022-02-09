//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/expression.hpp>

#include <caf/expected.hpp>

namespace substrait {
struct Plan;
} // namespace substrait

namespace vast::plugins::substrait {

[[nodiscard]] caf::expected<vast::expression>
parse_substrait(const ::substrait::Plan&);

} // namespace vast::plugins::substrait
