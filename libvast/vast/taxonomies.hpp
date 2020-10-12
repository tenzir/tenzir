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

#include <caf/meta/type_name.hpp>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace vast {

using concepts_type = std::unordered_map<std::string, std::vector<std::string>>;
using models_type = std::unordered_map<std::string, std::vector<std::string>>;

struct taxonomies {
  concepts_type concepts;
  models_type models;

  friend bool operator==(const taxonomies& lhs, const taxonomies& rhs);

  template <class Inspector>
  friend auto inspect(Inspector& f, taxonomies& t) {
    return f(caf::meta::type_name("taxonomies"), t.concepts, t.models);
  }
};

using taxonomies_ptr = std::shared_ptr<const taxonomies>;

caf::error inspect(caf::serializer& sink, const taxonomies_ptr& x);

caf::error inspect(caf::deserializer& source, taxonomies_ptr& x);

expression resolve(const taxonomies& t, const expression& orig);

} // namespace vast
