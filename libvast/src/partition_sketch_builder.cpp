//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/partition_sketch_builder.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/detail/overload.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/partition_synopsis.hpp"
#include "vast/sketch/accumulator_builder.hpp"
#include "vast/sketch/bloom_filter_builder.hpp"
#include "vast/sketch/min_max_accumulator.hpp"
#include "vast/table_slice.hpp"

namespace vast {

namespace {

using builder_factory = partition_sketch_builder::builder_factory;
using sketch_builder_ptr = std::unique_ptr<sketch::builder>;
template <typename T>
using minmax_builder
  = sketch::accumulator_builder<sketch::min_max_accumulator<T>>;

sketch_builder_ptr
make_sketch_builder(const type& t, const index_config::rule* rule) {
  VAST_ASSERT(rule != nullptr);
  auto f = detail::overload{
    [](const bool_type&) -> sketch_builder_ptr {
      // We (ab)use the min-max sketch to represent min = false and max = true.
      return std::make_unique<minmax_builder<bool_type>>();
    },
    [](const integer_type&) -> sketch_builder_ptr {
      return std::make_unique<minmax_builder<integer_type>>();
    },
    [](const count_type&) -> sketch_builder_ptr {
      return std::make_unique<minmax_builder<count_type>>();
    },
    [](const real_type&) -> sketch_builder_ptr {
      return std::make_unique<minmax_builder<real_type>>();
    },
    [](const duration_type&) -> sketch_builder_ptr {
      return std::make_unique<minmax_builder<duration_type>>();
    },
    [](const time_type&) -> sketch_builder_ptr {
      return std::make_unique<minmax_builder<time_type>>();
    },
    [=](const string_type&) -> sketch_builder_ptr {
      return std::make_unique<sketch::bloom_filter_builder>(rule->fp_rate);
    },
    [=](const pattern_type&) -> sketch_builder_ptr {
      return std::make_unique<sketch::bloom_filter_builder>(rule->fp_rate);
    },
    [=](const address_type&) -> sketch_builder_ptr {
      return std::make_unique<sketch::bloom_filter_builder>(rule->fp_rate);
    },
    [=](const subnet_type&) -> sketch_builder_ptr {
      return std::make_unique<sketch::bloom_filter_builder>(rule->fp_rate);
    },
    [=](const enumeration_type&) -> sketch_builder_ptr {
      return std::make_unique<sketch::bloom_filter_builder>(rule->fp_rate);
    },
    [=](const list_type& x) -> sketch_builder_ptr {
      // Lists are "transparent" for sketching, i.e., we consider them as
      // flattened in the context of the built sketch.
      return make_sketch_builder(x.value_type(), rule);
    },
    [=](const map_type& x) -> sketch_builder_ptr {
      // TODO: to support maps, we need some form of "compound sketch" that
      // internally consists of two sketches, one for the keys and one for the
      // values. To the user, this could be presented as a "union sketch" that
      // accepts multiple types. That said, an intermediate compromise is to
      // demote a map to a list, ignoring the keys and only considering the
      // values.
      return make_sketch_builder(x.value_type(), rule);
    },
    [](const record_type&) -> sketch_builder_ptr {
      // If the use case of creating sketches for records comes up, we can
      // solve it with the same "compound sketch" idea mentioned above.
      die("sketch builders can only operate on individual columns");
    },
  };
  return caf::visit(f, t);
}

/// Constructs a builder factory from a given rule.
builder_factory make_factory(const index_config::rule* rule) {
  VAST_ASSERT(rule != nullptr);
  return [rule](const type& x) {
    return make_sketch_builder(x, rule);
  };
}

} // namespace

caf::expected<partition_sketch_builder>
partition_sketch_builder::make(type layout, index_config config) {
  partition_sketch_builder builder{std::move(config)};
  for (const auto& rule : builder.config_.rules) {
    // Create one sketch per target.
    // FIXME: Verify that target fields exists in layout.
    for (const auto& target : rule.targets) {
      auto op = to<predicate::operand>(target);
      if (!op)
        return caf::error(ec::unspecified,
                          fmt::format("invalid rule target '{}'", target));
      // Register builder factories and factory stubs where typing is absent.
      auto f = detail::overload{
        [&](const auto&) -> caf::error {
          // TODO(MV): add formatter for predicate::operand
          return caf::make_error(ec::unspecified, "unsupported predicate "
                                                  "operand");
        },
        [&builder, &rule](const field_extractor& x) -> caf::error {
          if (builder.field_factory_.contains(x.field))
            return caf::make_error(ec::unspecified,
                                   fmt::format("duplicate field extractor {}",
                                               x.field));
          builder.field_factory_.emplace(x.field, make_factory(&rule));
          return caf::none;
        },
        [&builder, &rule](const type_extractor& x) -> caf::error {
          if (builder.type_factory_.contains(x.type))
            return caf::make_error(ec::unspecified,
                                   fmt::format("duplicate type extractor {}",
                                               x.type.name()));
          builder.type_factory_.emplace(x.type, make_factory(&rule));
          return caf::none;
        },
      };
      if (auto err = caf::visit(f, *op))
        return err;
    }
  }
  // VAST creates type-level sketches for all types per default. Therefore, we
  // top up the type factory to include all basic types.
  static const auto default_rule = index_config::rule{};
  auto default_factory = make_factory(&default_rule);
  auto const* partition_layout = caf::get_if<record_type>(&layout);
  VAST_ASSERT_CHEAP(partition_layout);
  for (auto const& leaf : partition_layout->leaves())
    builder.type_factory_.emplace(leaf.field.type, default_factory);
  return builder;
}

caf::error partition_sketch_builder::add(const table_slice& x) {
  const auto& layout = caf::get<record_type>(x.layout());
  events_ += x.rows();
  offset_ = std::min(offset_, x.offset());
  min_import_time_ = std::min(min_import_time_, x.import_time());
  max_import_time_ = std::max(max_import_time_, x.import_time());
  schema_ = x.layout();
  auto record_batch = to_record_batch(x);
  // Handle all field sketches.
  for (auto const& [key, factory] : field_factory_) {
    auto offsets = layout.resolve_key_suffix(key, x.layout().name());
    auto begin = offsets.begin();
    auto end = offsets.end();
    if (begin == end)
      continue;
    // The first type locks in the builder for all matching fields. If
    // subsequent fields have a different type, we emit a warning.
    // Alternatively, we could bifurcate the processing.
    auto first_offset = *begin;
    auto field = vast::qualified_record_field{x.layout(), first_offset};
    auto& builder = field_builders_[field];
    auto first_field = layout.field(first_offset);
    if (!builder)
      builder = factory(first_field.type);
    if (!builder)
      return caf::make_error(
        ec::unspecified,
        fmt::format("failed to construct sketch builder for key '{}'", key));
    auto xs = record_batch->column(layout.flat_index(first_offset));
    if (auto err = builder->add(xs))
      return err;
    while (++begin != end) {
      auto offset = *begin;
      auto field = layout.field(offset);
      if (field.type != first_field.type) {
        VAST_WARN("ignoring field '{}' with different type than first field",
                  offset);
        continue;
      }
      auto ys = record_batch->column(layout.flat_index(first_offset));
      if (auto err = builder->add(ys))
        return err;
    }
  }
  // Handle all type sketches.
  for (auto&& leaf : layout.leaves()) {
    auto add = [&, this](const type& t) -> caf::error {
      // TODO: Strip attributes from `t`
      auto i = type_factory_.find(t);
      if (i == type_factory_.end())
        return caf::none;
      auto& factory = i->second;
      auto& builder = type_builders_[t];
      if (!builder) {
        VAST_INFO("constructing new builder for type '{}'", t);
        builder = factory(t);
      }
      if (!builder) {
        VAST_INFO("not builder");
        return caf::make_error(
          ec::unspecified,
          fmt::format("failed to construct sketch builder for type '{}'", t));
      }
      auto xs = record_batch->column(layout.flat_index(leaf.index));
      if (auto err = builder->add(xs))
        return err;
      return caf::none;
    };
    if (auto err = add(leaf.field.type))
      return err;
    for (auto const& alias : leaf.field.type.aliases())
      if (auto err = add(alias))
        return err;
  }
  return caf::none;
}

caf::error partition_sketch_builder::finish_into(partition_synopsis& ps) && {
  VAST_ASSERT(ps.field_synopses_.empty() && ps.type_synopses_.empty(),
              "cannot mix & match sketches and synopses in the same partition "
              "synopsis");
  ps.events_ = events_;
  ps.offset_ = offset_;
  ps.min_import_time_ = min_import_time_;
  ps.max_import_time_ = max_import_time_;
  ps.schema_ = schema_;
  ps.version_ = version::partition_version;
  ps.use_sketches_ = true;
  ps.type_sketches_.reserve(type_builders_.size());
  for (auto&& [type, builder] : std::exchange(type_builders_, {})) {
    VAST_INFO("Finishing builder for type {}", type);
    auto sketch = builder->finish();
    if (!sketch) {
      // FIXME: temporary workaround until list<string> works correctly.
      continue;
      // return sketch.error();
    }
    ps.type_sketches_.insert(std::make_pair(type, std::move(*sketch)));
  }
  for (auto&& [field, builder] : std::exchange(field_builders_, {})) {
    auto sketch = builder->finish();
    if (!sketch) {
      // FIXME: temporary workaround until list<string> works correctly.
      continue;
      // return sketch.error();
    }
    ps.field_sketches_[field] = std::move(*sketch);
  }
  return {};
}

detail::generator<std::string_view> partition_sketch_builder::fields() const {
  for (const auto& [key, _] : field_builders_)
    co_yield std::string_view{key.field_name()};
  co_return;
}

detail::generator<type> partition_sketch_builder::types() const {
  for (const auto& [key, _] : type_builders_)
    co_yield key;
  co_return;
}

partition_sketch_builder::partition_sketch_builder(index_config config)
  : config_{std::move(config)} {
}

} // namespace vast
