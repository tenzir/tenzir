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

#include "vast/expression.hpp"
#include "vast/fwd.hpp"
#include "vast/offset.hpp"

#include <caf/actor.hpp>

#include <tuple>
#include <vector>

namespace vast {

/// Bundles an offset into an expression under evaluation to the curried
/// representation of the ::predicate at that position in the expression and
/// the INDEXER actor responsible for answering the (curried) predicate.
using evaluation_triple = std::tuple<offset, curried_predicate, caf::actor>;

using evaluation_triples = std::vector<evaluation_triple>;

} // namespace vast
