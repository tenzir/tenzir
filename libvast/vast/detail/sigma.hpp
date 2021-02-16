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

#include "vast/fwd.hpp"

#include "vast/expression.hpp"

#include <caf/expected.hpp>

/// Utilities to work with [Sigma](https://github.com/Neo23x0/sigma).
namespace vast::detail::sigma {

/// Parses a *rule* as VAST expression.
/// @param yaml The rule contents.
/// @returns The VAST expression corresponding to *yaml*.
caf::expected<expression> parse_rule(const data& yaml);

/// Parses a *search identifier* as VAST expression.
/// @param yaml The contents converted from YAML.
/// @returns The VAST expression corresponding to *yaml*.
caf::expected<expression> parse_search_id(const data& yaml);

} // namespace vast::detail::sigma
