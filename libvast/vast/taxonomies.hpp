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
#include "vast/type_set.hpp"

#include <caf/meta/type_name.hpp>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace vast {

/// Maps concept names to the fields or concepts that implement them.
struct concepts_type {
  struct definition {
    std::vector<std::string> fields;
    std::vector<std::string> concepts;

    friend bool operator==(const concepts_type::definition& lhs,
                           const concepts_type::definition& rhs);

    template <class Inspector>
    friend auto inspect(Inspector& f, concepts_type::definition& cd) {
      return f(caf::meta::type_name("concepts_type::definition"), cd.fields,
               cd.concepts);
    }
  };
  std::unordered_map<std::string, definition> data;

  friend bool operator==(const concepts_type& lhs, const concepts_type& rhs);

  template <class Inspector>
  friend auto inspect(Inspector& f, concepts_type& c) {
    return f(caf::meta::type_name("concepts_type"), c.data);
  }
};

/// Converts a data record to a concept.
caf::error convert(const data& d, concepts_type& out);

/// Extracts a concept definition from a data object.
caf::error extract_concepts(const data& d, concepts_type& out);

/// Extracts a concept definition from a data object.
caf::expected<concepts_type> extract_concepts(const data& d);

/// Maps model names to the concepts or models that implement them.
using models_type = std::unordered_map<std::string, std::vector<std::string>>;

/// A taxonomy is a combination of concepts and models. VAST stores all
/// configured taxonomies in memory together, hence the plural naming.
struct taxonomies {
  concepts_type concepts;
  models_type models;

  friend bool operator==(const taxonomies& lhs, const taxonomies& rhs);

  template <class Inspector>
  friend auto inspect(Inspector& f, taxonomies& t) {
    return f(caf::meta::type_name("taxonomies"), t.concepts, t.models);
  }
};

/// Substitutes concept and model identifiers in field extractors with
/// replacement expressions containing only concrete field names.
/// @param t The set of taxonomies to apply.
/// @param e The original expression.
/// @returns The sustitute expression.
expression resolve(const taxonomies& t, const expression& e);

/// Substitutes concept and model identifiers in field extractors with
/// replacement expressions containing only concrete field names.
/// @param t The set of taxonomies to apply.
/// @param e The original expression.
/// @param seen The set of all types in the database.
/// @returns The sustitute expression.
expression resolve(const taxonomies& t, const expression& e,
                   const std::map<std::string, type_set>& seen);

} // namespace vast
