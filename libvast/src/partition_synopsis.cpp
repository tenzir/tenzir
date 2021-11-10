//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

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
    if (t.tag("skip"))
      return nullptr;
    return factory<synopsis>::make(t, synopsis_options);
  };
  const auto& layout = slice.layout();
  auto each = caf::get<record_type>(layout).leaves();
  auto field_it = each.begin();
  for (size_t col = 0; col < slice.columns(); ++col, ++field_it) {
    auto add_column = [&](const synopsis_ptr& syn) {
      for (size_t row = 0; row < slice.rows(); ++row) {
        auto view = slice.at(row, col, field_it->first.type);
        // TODO: It would probably make sense to allow `nil` in the
        // synopsis API, so we can treat queries like `x == nil` just
        // like normal queries.
        if (!caf::holds_alternative<caf::none_t>(view))
          syn->add(std::move(view));
      }
    };
    auto key = qualified_record_field{layout, field_it->second};
    if (!caf::holds_alternative<string_type>(field_it->first.type)) {
      // Locate the relevant synopsis.
      auto it = field_synopses_.find(key);
      if (it == field_synopses_.end()) {
        // Attempt to create a synopsis if we have never seen this key before.
        it = field_synopses_
               .emplace(std::move(key), make_synopsis(field_it->first.type))
               .first;
      }
      // If there exists a synopsis for a field, add the entire column.
      if (auto& syn = it->second)
        add_column(syn);
    } else { // type == string
      // All strings share a partition-wide synopsis.
      // NOTE: if this is made configurable or removed, the pruning step from
      // the meta index lookup must be adjusted acordingly.
      field_synopses_[key] = nullptr;
      auto cleaned_type = field_it->first.type;
      cleaned_type.prune_metadata();
      auto tt = type_synopses_.find(cleaned_type);
      if (tt == type_synopses_.end())
        tt = type_synopses_
               .emplace(cleaned_type, make_synopsis(field_it->first.type))
               .first;
      if (auto& syn = tt->second)
        add_column(syn);
    }
  }
}

size_t partition_synopsis::memusage() const {
  size_t result = 0;
  for (const auto& [field, synopsis] : field_synopses_)
    result += synopsis ? synopsis->memusage() : 0ull;
  return result;
}

caf::expected<flatbuffers::Offset<fbs::partition_synopsis::v0>>
pack(flatbuffers::FlatBufferBuilder& builder, const partition_synopsis& x) {
  std::vector<flatbuffers::Offset<fbs::synopsis::v0>> synopses;
  for (const auto& [fqf, synopsis] : x.field_synopses_) {
    auto maybe_synopsis = pack(builder, synopsis, fqf);
    if (!maybe_synopsis)
      return maybe_synopsis.error();
    synopses.push_back(*maybe_synopsis);
  }
  for (const auto& [type, synopsis] : x.type_synopses_) {
    auto fqf = qualified_record_field{"", "", type};
    auto maybe_synopsis = pack(builder, synopsis, fqf);
    if (!maybe_synopsis)
      return maybe_synopsis.error();
    synopses.push_back(*maybe_synopsis);
  }
  auto synopses_vector = builder.CreateVector(synopses);
  fbs::partition_synopsis::v0Builder ps_builder(builder);
  ps_builder.add_synopses(synopses_vector);
  vast::fbs::uinterval id_range{x.offset, x.offset + x.events};
  ps_builder.add_id_range(&id_range);
  return ps_builder.Finish();
}

namespace {

// Not publicly exposed because it doesn't fully initialize `ps`.
caf::error unpack_(
  const flatbuffers::Vector<flatbuffers::Offset<fbs::synopsis::v0>>& synopses,
  partition_synopsis& ps) {
  for (const auto* synopsis : synopses) {
    if (!synopsis)
      return caf::make_error(ec::format_error, "synopsis is null");
    qualified_record_field qf;
    if (auto error
        = fbs::deserialize_bytes(synopsis->qualified_record_field(), qf))
      return error;
    synopsis_ptr ptr;
    if (auto error = unpack(*synopsis, ptr))
      return error;
    if (qf.is_standalone_type())
      ps.type_synopses_[qf.type()] = std::move(ptr);
    else
      ps.field_synopses_[qf] = std::move(ptr);
  }
  return caf::none;
}

} // namespace

caf::error
unpack(const fbs::partition_synopsis::v0& x, partition_synopsis& ps) {
  if (!x.id_range())
    return caf::make_error(ec::format_error, "missing id range");
  ps.offset = x.id_range()->begin();
  ps.events = x.id_range()->end() - x.id_range()->begin();
  if (!x.synopses())
    return caf::make_error(ec::format_error, "missing synopses");
  return unpack_(*x.synopses(), ps);
}

// This overload is for supporting the transition from VAST 2021.07.29
// to 2021.08.26, where the `id_range` field was added to partition
// synopsis. It can be removed once these versions are unsupported.
caf::error unpack(const fbs::partition_synopsis::v0& x, partition_synopsis& ps,
                  uint64_t offset, uint64_t events) {
  // We should not end up in this overload when an id range already exists.
  VAST_ASSERT(!x.id_range());
  ps.offset = offset;
  ps.events = events;
  if (!x.synopses())
    return caf::make_error(ec::format_error, "missing synopses");
  return unpack_(*x.synopses(), ps);
}

} // namespace vast
