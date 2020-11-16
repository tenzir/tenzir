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

/// The definition of a concept.
struct concept_ {
  /// The description of the concept.
  std::string description;

  /// The fields that the concept maps to.
  std::vector<std::string> fields;

  /// Other concepts that are referenced. Their fields are also considered
  /// during substitution.
  std::vector<std::string> concepts;

  friend bool operator==(const concept_& lhs, const concept_& rhs);

  template <class Inspector>
  friend auto inspect(Inspector& f, concept_& c) {
    return f(caf::meta::type_name("concept"), c.fields, c.concepts);
  }
};

/// Maps concept names to their definitions.
using concepts_map = std::unordered_map<std::string, concept_>;

/// Converts a data record to a concept.
caf::error convert(const data& d, concepts_map& out);

/// Extracts a concept definition from a data object.
caf::error extract_concepts(const data& d, concepts_map& out);

/// Extracts a concept definition from a data object.
caf::expected<concepts_map> extract_concepts(const data& d);

/// The definition of a model.
struct model {
  /// The description of the model.
  std::string description;

  /// The ordered concepts and models that the model is composed of.
  /// If an entry is another model, its concepts must also be represented  for
  /// a layout to be considered.
  std::vector<std::string> definition;

  friend bool operator==(const model& lhs, const model& rhs);

  template <class Inspector>
  friend auto inspect(Inspector& f, model& m) {
    return f(caf::meta::type_name("model"), m.description, m.definition);
  }
};

/// Maps model names to their definitions.
using models_map = std::unordered_map<std::string, model>;

/// Converts a data record to a model.
caf::error convert(const data& d, models_map& out);

/// Extracts a model definition from a data object.
caf::error extract_models(const data& d, models_map& out);

/// Extracts a model definition from a data object.
caf::expected<models_map> extract_models(const data& d);

/// A taxonomy is a combination of concepts and models. VAST stores all
/// configured taxonomies in memory together, hence the plural naming.
struct taxonomies {
  concepts_map concepts;
  models_map models;

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
