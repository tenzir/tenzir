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
#include "vast/sketch/accumulator_builder.hpp"
#include "vast/sketch/bloom_filter_builder.hpp"
#include "vast/sketch/min_max_accumulator.hpp"
#include "vast/table_slice.hpp"

namespace vast {

namespace {

using builder_factory = partition_sketch_builder::builder_factory;
using sketch_builder_ptr = std::unique_ptr<sketch::builder>;

sketch_builder_ptr
make_sketch_builder(const type& t, const index_config::rule* rule) {
  VAST_ASSERT(rule != nullptr);
  using min_max_sketch_builder
    = sketch::accumulator_builder<sketch::min_max_accumulator>;
  auto f = detail::overload{
    [](const none_type&) -> sketch_builder_ptr {
      die("sketch builders require none-null types");
    },
    [](const bool_type&) -> sketch_builder_ptr {
      // We (ab)use the min-max sketch to represent min = false and max = true.
      return std::make_unique<min_max_sketch_builder>();
    },
    [](const integer_type&) -> sketch_builder_ptr {
      return std::make_unique<min_max_sketch_builder>();
    },
    [](const count_type&) -> sketch_builder_ptr {
      return std::make_unique<min_max_sketch_builder>();
    },
    [](const real_type&) -> sketch_builder_ptr {
      return std::make_unique<min_max_sketch_builder>();
    },
    [](const duration_type&) -> sketch_builder_ptr {
      return std::make_unique<min_max_sketch_builder>();
    },
    [](const time_type&) -> sketch_builder_ptr {
      return std::make_unique<min_max_sketch_builder>();
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
partition_sketch_builder::make(index_config config) {
  partition_sketch_builder builder{std::move(config)};
  for (const auto& rule : builder.config_.rules) {
    // Create one sketch per target.
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
          // TODO: remove the string wrapping once transparent keys work.
          if (builder.type_factory_.contains(x.type.name()))
            return caf::make_error(ec::unspecified,
                                   fmt::format("duplicate type extractor {}",
                                               x.type.name()));
          builder.type_factory_.emplace(x.type.name(), make_factory(&rule));
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
  auto top_up = [&](concrete_type auto concrete) {
    auto t = type{concrete};
    builder.type_factory_.emplace(t.name(), default_factory);
  };
  top_up(bool_type{});
  top_up(integer_type{});
  top_up(count_type{});
  top_up(real_type{});
  top_up(duration_type{});
  top_up(time_type{});
  top_up(string_type{});
  top_up(pattern_type{});
  top_up(address_type{});
  top_up(subnet_type{});
  // The inner types don't matter here, since we only register the factory for
  // the name of the outer type, i.e., "list", and "map".
  auto n = none_type{};
  top_up(list_type{n});
  top_up(map_type{n, n});
  return builder;
}

caf::error partition_sketch_builder::add(const table_slice& x) {
  const auto& layout = caf::get<record_type>(x.layout());
  auto record_batch = to_record_batch(x);
  // Handle all field sketches.
  for (auto& [key, factory] : field_factory_) {
    auto offsets = layout.resolve_key_suffix(key, x.layout().name());
    auto begin = offsets.begin();
    auto end = offsets.end();
    if (begin == end)
      continue;
    auto& builder = field_builders_[key];
    // The first type locks in the builder for all matching fields. If
    // subsequent fields have a different type, we emit a warning.
    // Alternatively, we could bifurcate the processing.
    auto first_offset = *begin;
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
  // FIXME: this doesn't work for nested types yet because we catch types under
  // a single top-level name. E.g., list<string>, list<count>, etc. will result
  // in creation of *one* builder for the list type (under the name "list").
  for (auto&& leaf : layout.leaves()) {
    // TODO: Do this more efficiently rather than going through a string
    // vector. We could offer another generator that goes over the names in
    // reverse order, or consider changing the order in the first place.
    std::vector<std::string> names;
    for (auto name : leaf.field.type.names())
      names.push_back(std::string{name});
    auto begin = names.rbegin();
    auto end = names.rend();
    // We only allow aliases of the types that exist in the factory, so unless
    // we have a valid starting point, we do not proceed.
    if (!type_factory_.contains(*begin))
      continue;
    builder_factory parent_factory;
    for (; begin != end; ++begin) {
      auto&& name = *begin;
      auto& factory = type_factory_[name];
      if (factory)
        parent_factory = factory;
      else
        factory = parent_factory;
      auto& builder = type_builders_[name];
      if (!builder)
        builder = factory(leaf.field.type);
      if (!builder)
        return caf::make_error(ec::unspecified,
                               fmt::format("failed to construct sketch "
                                           "builder for type '{}'",
                                           name));
      auto xs = record_batch->column(layout.flat_index(leaf.index));
      if (auto err = builder->add(xs))
        return err;
    }
  }
  return caf::none;
}

caf::expected<partition_sketch> partition_sketch_builder::finish() {
  // TODO: implement.
  return ec::unimplemented;
}

detail::generator<std::string_view> partition_sketch_builder::fields() const {
  for (const auto& [key, _] : field_builders_)
    co_yield std::string_view{key};
  co_return;
}

detail::generator<std::string_view> partition_sketch_builder::types() const {
  for (const auto& [name, _] : type_builders_)
    co_yield std::string_view{name};
  co_return;
}

partition_sketch_builder::partition_sketch_builder(index_config config)
  : config_{std::move(config)} {
}

} // namespace vast
