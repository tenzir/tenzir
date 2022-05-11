//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/partition_synopsis.hpp"

#include "vast/detail/collect.hpp"
#include "vast/error.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/index_config.hpp"
#include "vast/partition_sketch_builder.hpp"
#include "vast/sketch/sketch.hpp"
#include "vast/synopsis_factory.hpp"

namespace vast {

partition_synopsis::partition_synopsis(partition_synopsis&& that) noexcept
  : use_sketches(that.use_sketches) {
  offset = std::exchange(that.offset, {}), events
                                           = std::exchange(that.events, {});
  min_import_time = std::exchange(that.min_import_time, time::max());
  max_import_time = std::exchange(that.max_import_time, time::min());
  version = std::exchange(that.version, version::partition_version);
  schema = std::exchange(that.schema, {});
  type_synopses_ = std::exchange(that.type_synopses_, {});
  field_synopses_ = std::exchange(that.field_synopses_, {});
  type_sketches_ = std::exchange(that.type_sketches_, {});
  field_sketches_ = std::exchange(that.field_sketches_, {});
  memusage_.store(that.memusage_.exchange(0));
}

partition_synopsis&
partition_synopsis::operator=(partition_synopsis&& that) noexcept {
  if (this != &that) {
    offset = std::exchange(that.offset, {});
    events = std::exchange(that.events, {});
    use_sketches = that.use_sketches;
    min_import_time = std::exchange(that.min_import_time, time::max());
    max_import_time = std::exchange(that.max_import_time, time::min());
    version = std::exchange(that.version, version::partition_version);
    schema = std::exchange(that.schema, {});
    type_synopses_ = std::exchange(that.type_synopses_, {});
    field_synopses_ = std::exchange(that.field_synopses_, {});
    type_sketches_ = std::exchange(that.type_sketches_, {});
    field_sketches_ = std::exchange(that.field_sketches_, {});
    memusage_.store(that.memusage_.exchange(0));
  }
  return *this;
}

void partition_synopsis::shrink() {
  // Note that we don't need to shrink sketches here,
  // because they are constructed differently and only
  // get entered into the synopsis with their shrinked
  // size.
  memusage_ = 0; // Invalidate cached size.
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

// TODO: Use a more efficient data structure for rule lookup.
std::optional<double> get_field_fprate(const index_config& config,
                                       const qualified_record_field& field) {
  for (const auto& [targets, fprate] : config.rules)
    for (const auto& name : targets)
      if (name.size()
            == field.field_name().size() + field.layout_name().size() + 1
          && name.starts_with(field.layout_name())
          && name.ends_with(field.field_name()))
        return fprate;
  return std::nullopt;
}

double get_type_fprate(const index_config& config, const type& type) {
  for (const auto& [targets, fprate] : config.rules) {
    for (const auto& name : targets) {
      if (name == ":string" && type == string_type{})
        return fprate;
      else if (name == ":addr" && type == address_type{})
        return fprate;
    }
  }
  return config.default_fp_rate;
}

void partition_synopsis::add(const table_slice& slice,
                             size_t partition_capacity,
                             const index_config& fp_rates) {
  memusage_ = 0; // Invalidate cached size.
  const auto& layout = slice.layout();
  if (!schema)
    schema = layout;
  VAST_ASSERT(schema == layout);
  // Update generic partition synopsis information.
  min_import_time = std::min(min_import_time, slice.import_time());
  max_import_time = std::max(max_import_time, slice.import_time());
  events += slice.rows();
  // Synopses are created live by the partition synopsis, but
  // sketches are built externally by the partition_sketch_builder
  // and only inserted into a partition synopsis after the fact.
  if (use_sketches) {
    VAST_ASSERT(type_synopses_.empty() && field_synopses_.empty(),
                "cannot change between synopses and sketches on the fly");
    return;
  }
  // Update field and/or type synopses if necessary.
  auto make_synopsis
    = [](const type& t, const caf::settings& synopsis_options) -> synopsis_ptr {
    if (t.attribute("skip"))
      return nullptr;
    return factory<synopsis>::make(t, synopsis_options);
  };
  auto each = caf::get<record_type>(layout).leaves();
  auto leaf_it = each.begin();
  caf::settings synopsis_opts;
  // These options must be kept in sync with vast/address_synopsis.hpp and
  // vast/string_synopsis.hpp respectively.
  synopsis_opts["buffer-input-data"] = true;
  synopsis_opts["max-partition-size"] = partition_capacity;
  synopsis_opts["string-synopsis-fp-rate"]
    = get_type_fprate(fp_rates, vast::type{string_type{}});
  synopsis_opts["address-synopsis-fp-rate"]
    = get_type_fprate(fp_rates, vast::type{address_type{}});
  for (size_t col = 0; col < slice.columns(); ++col, ++leaf_it) {
    auto&& leaf = *leaf_it;
    auto add_column = [&](const synopsis_ptr& syn) {
      for (size_t row = 0; row < slice.rows(); ++row) {
        auto view = slice.at(row, col, leaf.field.type);
        // TODO: It would probably make sense to allow `nil` in the
        // synopsis API, so we can treat queries like `x == nil` just
        // like normal queries.
        if (!caf::holds_alternative<caf::none_t>(view))
          syn->add(std::move(view));
      }
    };
    // Make a field synopsis if it was configured.
    if (auto key = qualified_record_field{layout, leaf.index};
        auto fprate = get_field_fprate(fp_rates, key)) {
      // Locate the relevant synopsis.
      auto it = field_synopses_.find(key);
      if (it == field_synopses_.end()) {
        // Attempt to create a synopsis if we have never seen this key before.
        auto opts = synopsis_opts;
        opts["string-synopsis-fp-rate"] = *fprate;
        opts["address-synopsis-fp-rate"] = *fprate;
        auto syn = make_synopsis(leaf.field.type, opts);
        it = field_synopses_.emplace(std::move(key), std::move(syn)).first;
      }
      // If there exists a synopsis for a field, add the entire column.
      if (auto& syn = it->second)
        add_column(syn);
    } else {
      // We still rely on having `field -> nullptr` mappings for all fields
      // without a dedicated synopsis during lookup and .
      field_synopses_.emplace(std::move(key), nullptr);
    }
    // We need to prune the type's metadata here by converting it to a
    // concrete type and back, because the type synopses are looked up
    // independent from names and attributes.
    auto prune = [&]<concrete_type T>(const T& x) {
      return type{x};
    };
    auto cleaned_type = caf::visit(prune, leaf.field.type);
    // Create the type synopsis
    auto tt = type_synopses_.find(cleaned_type);
    if (tt == type_synopses_.end())
      tt = type_synopses_
             .emplace(cleaned_type,
                      make_synopsis(leaf.field.type, synopsis_opts))
             .first;
    if (auto& syn = tt->second)
      add_column(syn);
  }
}

size_t partition_synopsis::memusage() const {
  size_t result = memusage_;
  if (result == size_t{0}) {
    for (const auto& [field, synopsis] : field_synopses_)
      result += synopsis ? synopsis->memusage() : 0ull;
    for (const auto& [type, synopsis] : type_synopses_)
      result += synopsis ? synopsis->memusage() : 0ull;
    for (const auto& [field, sketch] : field_sketches_)
      result += sketch ? mem_usage(*sketch) : 0ull;
    for (const auto& [type, sketch] : type_sketches_)
      result += mem_usage(sketch);
    memusage_ = result;
  }
  return result;
}

partition_synopsis* partition_synopsis::copy() const {
  auto result = std::make_unique<partition_synopsis>();
  result->offset = offset;
  result->events = events;
  result->min_import_time = min_import_time;
  result->max_import_time = max_import_time;
  result->version = version;
  result->schema = schema;
  result->memusage_ = memusage_.load();
  result->use_sketches = use_sketches;
  result->type_synopses_.reserve(type_synopses_.size());
  result->field_synopses_.reserve(field_synopses_.size());
  result->type_sketches_.reserve(type_sketches_.size());
  result->field_sketches_.reserve(field_sketches_.size());
  for (auto const& [type, synopsis] : type_synopses_) {
    if (synopsis)
      result->type_synopses_[type] = synopsis->clone();
    else
      result->type_synopses_[type] = nullptr;
  }
  for (auto const& [field, synopsis] : field_synopses_) {
    if (synopsis)
      result->field_synopses_[field] = synopsis->clone();
    else
      result->field_synopses_[field] = nullptr;
  }
  for (auto const& [type, sketch] : type_sketches_) {
    result->type_sketches_.insert(std::make_pair(type, sketch));
  }
  for (auto const& [field, sketch] : field_sketches_) {
    if (sketch)
      result->field_sketches_[field] = sketch;
    else
      result->field_sketches_[field] = std::nullopt;
  }
  return result.release();
}

caf::expected<
  flatbuffers::Offset<fbs::partition_synopsis::LegacyPartitionSynopsis>>
pack_legacy(flatbuffers::FlatBufferBuilder& builder,
            const partition_synopsis& x) {
  if (x.use_sketches)
    return caf::make_error(ec::logic_error, "cannot create legacy synopsis "
                                            "from sketches");
  std::vector<flatbuffers::Offset<fbs::synopsis::LegacySynopsis>> synopses;
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
  auto schema_bytes = as_bytes(x.schema);
  auto schema_vector = builder.CreateVector(
    reinterpret_cast<const uint8_t*>(schema_bytes.data()), schema_bytes.size());
  fbs::partition_synopsis::LegacyPartitionSynopsisBuilder ps_builder(builder);
  ps_builder.add_synopses(synopses_vector);
  vast::fbs::uinterval id_range{x.offset, x.offset + x.events};
  ps_builder.add_id_range(&id_range);
  vast::fbs::interval import_time_range{
    x.min_import_time.time_since_epoch().count(),
    x.max_import_time.time_since_epoch().count()};
  ps_builder.add_import_time_range(&import_time_range);
  ps_builder.add_version(x.version);
  ps_builder.add_schema(schema_vector);
  return ps_builder.Finish();
}

// FIXME: Move to type.hpp
flatbuffers::Offset<flatbuffers::Vector<uint8_t>>
pack_nested(flatbuffers::FlatBufferBuilder& builder, const type& type) {
  auto type_bytes = as_bytes(type);
  return builder.CreateVector(
    reinterpret_cast<const uint8_t*>(type_bytes.data()), type_bytes.size());
}

caf::expected<flatbuffers::Offset<fbs::partition_synopsis::PartitionSynopsisV1>>
pack(flatbuffers::FlatBufferBuilder& builder, const partition_synopsis& x) {
  if (!x.use_sketches)
    return caf::make_error(ec::logic_error, "cannot create sketches from "
                                            "legacy synopses");
  std::vector<flatbuffers::Offset<fbs::partition_synopsis::FieldSketch>>
    field_sketches;
  std::vector<flatbuffers::Offset<fbs::partition_synopsis::TypeSketch>>
    type_sketches;
  for (auto const& [field, sketch] : x.field_sketches_) {
    if (!sketch)
      continue; // FIXME - is this correct or should we leave the field as null?
    auto name
      = builder.CreateString(field.name()); // FIXME - field.field_name() ?
    auto type_offset = pack_nested(builder, field.type());
    auto field_offset
      = fbs::type::detail::CreateRecordField(builder, name, type_offset);
    auto sketch_offset = pack_nested(builder, *sketch);
    auto field_sketch_offset = fbs::partition_synopsis::CreateFieldSketch(
      builder, field_offset, sketch_offset);
    field_sketches.push_back(field_sketch_offset);
  }
  for (auto const& [type, sketch] : x.type_sketches_) {
    auto type_offset = pack_nested(builder, type);
    auto sketch_offset = pack_nested(builder, sketch);
    auto type_sketch_offset = fbs::partition_synopsis::CreateTypeSketch(
      builder, type_offset, sketch_offset);
    type_sketches.push_back(type_sketch_offset);
  }
  auto field_sketches_vector = builder.CreateVector(field_sketches);
  auto type_sketches_vector = builder.CreateVector(type_sketches);
  fbs::partition_synopsis::PartitionSynopsisV1Builder ps_builder(builder);
  vast::fbs::uinterval id_range{x.offset, x.offset + x.events};
  ps_builder.add_id_range(&id_range);
  ps_builder.add_field_sketches(field_sketches_vector);
  ps_builder.add_type_sketches(type_sketches_vector);
  vast::fbs::interval import_time_range{
    x.min_import_time.time_since_epoch().count(),
    x.max_import_time.time_since_epoch().count()};
  ps_builder.add_import_time_range(&import_time_range);
  return ps_builder.Finish();
}

namespace {

// Not publicly exposed because it doesn't fully initialize `ps`.
caf::error unpack_(
  const flatbuffers::Vector<flatbuffers::Offset<fbs::synopsis::LegacySynopsis>>&
    synopses,
  partition_synopsis& ps) {
  ps.use_sketches = false;
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
    // We mark type-level synopses by using an empty string as name.
    if (qf.is_standalone_type())
      ps.type_synopses_[qf.type()] = std::move(ptr);
    else
      ps.field_synopses_[qf] = std::move(ptr);
  }
  return caf::none;
}

} // namespace

caf::error unpack(const fbs::partition_synopsis::PartitionSynopsisV1& x,
                  partition_synopsis& ps) {
  if (!x.id_range())
    return caf::make_error(ec::format_error, "missing id range");
  if (!x.import_time_range())
    return caf::make_error(ec::format_error, "missing import time range");
  if (!x.field_sketches())
    return caf::make_error(ec::format_error, "missing field sketches");
  if (!x.type_sketches())
    return caf::make_error(ec::format_error, "missing type sketches");
  ps.use_sketches = true;
  ps.offset = x.id_range()->begin();
  ps.events = x.id_range()->end() - x.id_range()->begin();
  ps.min_import_time = time{} + duration{x.import_time_range()->begin()};
  ps.max_import_time = time{} + duration{x.import_time_range()->end()};
  for (auto fs : *x.field_sketches()) {
    if (!fs)
      return caf::make_error(ec::format_error, "missing id range");
    auto const* field = fs->field();
    auto field_name = field->name();
    auto type_bytes = field->type();
    if (!type_bytes)
      return caf::make_error(ec::format_error, "missing type");
    auto type_chunk
      = chunk::copy(as_bytes(type_bytes->data(), type_bytes->size()));
    auto type = vast::type{std::move(type_chunk)};
    auto const* sketch_bytes = fs->sketch();
    auto chunk
      = chunk::copy(as_bytes(sketch_bytes->data(), sketch_bytes->size()));
    auto fb = flatbuffer<fbs::Sketch>::make(std::move(chunk));
    if (!fb)
      return caf::make_error(ec::format_error, "invalid sketch");
    auto sketch = sketch::sketch{*fb};
    auto const* layout_name = x.layout_name();
    auto qf = vast::qualified_record_field{
      layout_name->string_view(), field_name->string_view(), std::move(type)};
    ps.field_sketches_.insert(std::make_pair(std::move(qf), std::move(sketch)));
  }
  for (auto ts : *x.type_sketches()) {
    auto const* type_bytes = ts->type();
    auto type_chunk
      = chunk::copy(as_bytes(type_bytes->data(), type_bytes->size()));
    auto type = vast::type{std::move(type_chunk)};
    auto const* sketch_bytes = ts->sketch();
    auto sketch_chunk
      = chunk::copy(as_bytes(sketch_bytes->data(), sketch_bytes->size()));
    auto fb = flatbuffer<fbs::Sketch>::make(std::move(sketch_chunk));
    if (!fb)
      return caf::make_error(ec::format_error, "invalid sketch");
    auto sketch = sketch::sketch{*fb};
    ps.type_sketches_.insert(
      std::make_pair(std::move(type), std::move(sketch)));
  }
  return caf::error{};
}

caf::error unpack(const fbs::partition_synopsis::LegacyPartitionSynopsis& x,
                  partition_synopsis& ps) {
  if (!x.id_range())
    return caf::make_error(ec::format_error, "missing id range");
  ps.offset = x.id_range()->begin();
  ps.events = x.id_range()->end() - x.id_range()->begin();
  if (x.import_time_range()) {
    ps.min_import_time = time{} + duration{x.import_time_range()->begin()};
    ps.max_import_time = time{} + duration{x.import_time_range()->end()};
  } else {
    ps.min_import_time = time{};
    ps.max_import_time = time{};
  }
  ps.version = x.version();
  if (const auto* schema = x.schema())
    ps.schema = type{chunk::copy(as_bytes(*schema))};
  if (!x.synopses())
    return caf::make_error(ec::format_error, "missing synopses");
  return unpack_(*x.synopses(), ps);
}

// This overload is for supporting the transition from VAST 2021.07.29
// to 2021.08.26, where the `id_range` field was added to partition
// synopsis. It can be removed once these versions are unsupported.
caf::error unpack(const fbs::partition_synopsis::LegacyPartitionSynopsis& x,
                  partition_synopsis& ps, uint64_t offset, uint64_t events) {
  // We should not end up in this overload when an id range already exists.
  VAST_ASSERT(!x.id_range());
  ps.offset = offset;
  ps.events = events;
  if (x.import_time_range()) {
    ps.min_import_time = time{} + duration{x.import_time_range()->begin()};
    ps.max_import_time = time{} + duration{x.import_time_range()->end()};
  } else {
    ps.min_import_time = time{};
    ps.max_import_time = time{};
  }
  ps.version = x.version();
  if (const auto* schema = x.schema())
    ps.schema = type{chunk::copy(as_bytes(*schema))};
  if (!x.synopses())
    return caf::make_error(ec::format_error, "missing synopses");
  return unpack_(*x.synopses(), ps);
}

} // namespace vast
