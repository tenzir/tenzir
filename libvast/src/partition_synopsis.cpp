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

#include "vast/partition_synopsis.hpp"

#include "vast/synopsis_factory.hpp"

namespace vast {

void partition_synopsis::shrink() {
  for (auto& [field, synopsis] : field_synopses_) {
    if (!synopsis)
      continue;
    auto shrinked_synopsis = synopsis->shrink();
    if (!shrinked_synopsis)
      continue;
    synopsis.swap(shrinked_synopsis);
  }
  // TODO: Make a utility function instead of copy/pasting
  for (auto& [field, synopsis] : type_synopses_) {
    if (!synopsis)
      continue;
    auto shrinked_synopsis = synopsis->shrink();
    if (!shrinked_synopsis)
      continue;
    synopsis.swap(shrinked_synopsis);
  }
}

void partition_synopsis::add(const table_slice& slice,
                             const caf::settings& synopsis_options) {
  auto make_synopsis = [&](const type& t) -> synopsis_ptr {
    return has_skip_attribute(t) ? nullptr
                                 : factory<synopsis>::make(t, synopsis_options);
  };
  auto& layout = slice.layout();
  auto each = record_type::each(layout);
  auto field_it = each.begin();
  for (size_t col = 0; col < slice.columns(); ++col, ++field_it) {
    auto& type = field_it->type();
    auto add_column = [&](const synopsis_ptr& syn) {
      for (size_t row = 0; row < slice.rows(); ++row) {
        auto view = slice.at(row, col, type);
        if (!caf::holds_alternative<caf::none_t>(view))
          syn->add(std::move(view));
      }
    };
    auto key = qualified_record_field{layout.name(), *field_it};
    if (!caf::holds_alternative<string_type>(type)) {
      // Locate the relevant synopsis.
      auto it = field_synopses_.find(key);
      if (it == field_synopses_.end()) {
        // Attempt to create a synopsis if we have never seen this key before.
        it = field_synopses_.emplace(std::move(key), make_synopsis(type)).first;
      }
      // If there exists a synopsis for a field, add the entire column.
      if (auto& syn = it->second)
        add_column(syn);
    } else { // type == string
      field_synopses_[key] = nullptr;
      auto cleaned_type = vast::type{field_it->type()}.attributes({});
      auto tt = type_synopses_.find(cleaned_type);
      if (tt == type_synopses_.end())
        tt = type_synopses_.emplace(cleaned_type, make_synopsis(type)).first;
      if (auto& syn = tt->second)
        add_column(syn);
    }
  }
}

size_t partition_synopsis::memusage() const {
  size_t result = 0;
  for (auto& [field, synopsis] : field_synopses_)
    result += synopsis ? synopsis->memusage() : 0ull;
  return result;
}

} // namespace vast
