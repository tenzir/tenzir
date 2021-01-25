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

#include "vast/error.hpp"
#include "vast/fbs/utils.hpp"
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

caf::expected<flatbuffers::Offset<fbs::partition_synopsis::v0>>
pack(flatbuffers::FlatBufferBuilder& builder, const partition_synopsis& x) {
  std::vector<flatbuffers::Offset<fbs::synopsis::v0>> synopses;
  for (auto& [fqf, synopsis] : x.field_synopses_) {
    auto maybe_synopsis = pack(builder, synopsis, fqf);
    if (!maybe_synopsis)
      return maybe_synopsis.error();
    synopses.push_back(*maybe_synopsis);
  }
  for (auto& [type, synopsis] : x.type_synopses_) {
    qualified_record_field fqf;
    fqf.type = type;
    auto maybe_synopsis = pack(builder, synopsis, fqf);
    if (!maybe_synopsis)
      return maybe_synopsis.error();
    synopses.push_back(*maybe_synopsis);
  }
  auto synopses_vector = builder.CreateVector(synopses);
  fbs::partition_synopsis::v0Builder ps_builder(builder);
  ps_builder.add_synopses(synopses_vector);
  return ps_builder.Finish();
}

caf::error
unpack(const fbs::partition_synopsis::v0& x, partition_synopsis& ps) {
  if (!x.synopses())
    return caf::make_error(ec::format_error, "missing synopses");
  for (auto synopsis : *x.synopses()) {
    if (!synopsis)
      return caf::make_error(ec::format_error, "synopsis is null");
    qualified_record_field qf;
    if (auto error
        = fbs::deserialize_bytes(synopsis->qualified_record_field(), qf))
      return error;
    synopsis_ptr ptr;
    if (auto error = unpack(*synopsis, ptr))
      return error;
    if (!qf.field_name.empty())
      ps.field_synopses_[qf] = std::move(ptr);
    else
      ps.type_synopses_[qf.type] = std::move(ptr);
  }
  return caf::none;
}

} // namespace vast
