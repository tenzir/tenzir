//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/partition_synopsis.hpp"

#include "tenzir/collect.hpp"
#include "tenzir/error.hpp"
#include "tenzir/fbs/utils.hpp"
#include "tenzir/index_config.hpp"
#include "tenzir/synopsis_factory.hpp"

namespace tenzir {

partition_synopsis::partition_synopsis(partition_synopsis&& that) noexcept {
  events = std::exchange(that.events, {});
  min_import_time = std::exchange(that.min_import_time, time::max());
  max_import_time = std::exchange(that.max_import_time, time::min());
  version = std::exchange(that.version, version::current_partition_version);
  schema = std::exchange(that.schema, {});
  type_synopses_ = std::exchange(that.type_synopses_, {});
  field_synopses_ = std::exchange(that.field_synopses_, {});
  memusage_.store(that.memusage_.exchange(0));
}

partition_synopsis&
partition_synopsis::operator=(partition_synopsis&& that) noexcept {
  if (this != &that) {
    events = std::exchange(that.events, {});
    min_import_time = std::exchange(that.min_import_time, time::max());
    max_import_time = std::exchange(that.max_import_time, time::min());
    version = std::exchange(that.version, version::current_partition_version);
    schema = std::exchange(that.schema, {});
    type_synopses_ = std::exchange(that.type_synopses_, {});
    field_synopses_ = std::exchange(that.field_synopses_, {});
    memusage_.store(that.memusage_.exchange(0));
  }
  return *this;
}

void partition_synopsis::shrink() {
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
  for (const auto& [targets, fprate, _] : config.rules) {
    for (const auto& name : targets) {
      if (name.size()
            == field.field_name().size() + field.schema_name().size() + 1
          && name.starts_with(field.schema_name())
          && name.ends_with(field.field_name()))
        return fprate;
    }
  }
  auto use_default_fprate = []<concrete_type T>(const T&) {
    return detail::is_any_v<T, bool_type, int64_type, uint64_type, double_type,
                            duration_type, time_type>;
  };
  if (match(field.type(), use_default_fprate)) {
    return config.default_fp_rate;
  }
  return std::nullopt;
}

double get_type_fprate(const index_config& config, const type& type) {
  for (const auto& [targets, fprate, _] : config.rules) {
    for (const auto& name : targets) {
      if (name == ":string" && type == string_type{})
        return fprate;
      else if (name == ":ip" && type == ip_type{})
        return fprate;
    }
  }
  return config.default_fp_rate;
}

void partition_synopsis::add(const table_slice& slice,
                             size_t partition_capacity,
                             const index_config& fp_rates) {
  memusage_ = 0; // Invalidate cached size.
  auto make_synopsis
    = [](const type& t, const caf::settings& synopsis_options) -> synopsis_ptr {
    if (t.attribute("skip"))
      return nullptr;
    return factory<synopsis>::make(t, synopsis_options);
  };
  if (!schema)
    schema = slice.schema();
  TENZIR_ASSERT_EXPENSIVE(schema == slice.schema());
  auto each = as<record_type>(schema).leaves();
  auto leaf_it = each.begin();
  caf::settings synopsis_opts;
  // These options must be kept in sync with tenzir/ip_synopsis.hpp and
  // tenzir/string_synopsis.hpp respectively.
  synopsis_opts["buffer-input-data"] = true;
  synopsis_opts["max-partition-size"] = partition_capacity;
  synopsis_opts["string-synopsis-fp-rate"]
    = get_type_fprate(fp_rates, tenzir::type{string_type{}});
  synopsis_opts["address-synopsis-fp-rate"]
    = get_type_fprate(fp_rates, tenzir::type{ip_type{}});
  for (size_t col = 0; col < slice.columns(); ++col, ++leaf_it) {
    auto&& leaf = *leaf_it;
    auto add_column = [&](const synopsis_ptr& syn) {
      for (size_t row = 0; row < slice.rows(); ++row) {
        auto view = slice.at(row, col, leaf.field.type);
        // TODO: It would probably make sense to allow `null` in the
        // synopsis API, so we can treat queries like `x == null` just
        // like normal queries.
        if (!is<caf::none_t>(view)) {
          syn->add(std::move(view));
        }
      }
    };
    // Make a field synopsis if it was configured.
    if (auto key = qualified_record_field{schema, leaf.index};
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
    auto cleaned_type = match(leaf.field.type, prune);
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
    memusage_ = result;
  }
  return result;
}

partition_synopsis* partition_synopsis::copy() const {
  auto result = std::make_unique<partition_synopsis>();
  result->events = events;
  result->min_import_time = min_import_time;
  result->max_import_time = max_import_time;
  result->version = version;
  result->schema = schema;
  result->memusage_ = memusage_.load();
  result->type_synopses_.reserve(type_synopses_.size());
  result->field_synopses_.reserve(field_synopses_.size());
  result->memusage_ = memusage_.load();
  for (const auto& [type, synopsis] : type_synopses_) {
    if (synopsis)
      result->type_synopses_[type] = synopsis->clone();
    else
      result->type_synopses_[type] = nullptr;
  }
  for (const auto& [field, synopsis] : field_synopses_) {
    if (synopsis)
      result->field_synopses_[field] = synopsis->clone();
    else
      result->field_synopses_[field] = nullptr;
  }
  return result.release();
}

caf::expected<
  flatbuffers::Offset<fbs::partition_synopsis::LegacyPartitionSynopsis>>
pack(flatbuffers::FlatBufferBuilder& builder, const partition_synopsis& x) {
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
  tenzir::fbs::uinterval id_range{0, x.events};
  ps_builder.add_id_range(&id_range);
  tenzir::fbs::interval import_time_range{
    x.min_import_time.time_since_epoch().count(),
    x.max_import_time.time_since_epoch().count()};
  ps_builder.add_import_time_range(&import_time_range);
  ps_builder.add_version(x.version);
  ps_builder.add_schema(schema_vector);
  return ps_builder.Finish();
}

namespace {

// Not publicly exposed because it doesn't fully initialize `ps`.
caf::error unpack_(
  const flatbuffers::Vector<flatbuffers::Offset<fbs::synopsis::LegacySynopsis>>&
    synopses,
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
    // We mark type-level synopses by using an empty string as name.
    if (qf.is_standalone_type())
      ps.type_synopses_[qf.type()] = std::move(ptr);
    else
      ps.field_synopses_[qf] = std::move(ptr);
  }
  return caf::none;
}

} // namespace

caf::error unpack(const fbs::partition_synopsis::LegacyPartitionSynopsis& x,
                  partition_synopsis& ps) {
  if (!x.id_range())
    return caf::make_error(ec::format_error, "missing id range");
  if (x.id_range()->begin() != 0)
    return caf::make_error(ec::format_error,
                           "partitions with an ID range not starting at zero "
                           "are no longer supported");
  ps.events = x.id_range()->end();
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

} // namespace tenzir
