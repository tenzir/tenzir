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

/// Constructs a builder factory from a given rule.
builder_factory make_factory(const index_config::rule& rule) {
  using sketch_builder_ptr = std::unique_ptr<sketch::builder>;
  using min_max_sketch_builder
    = sketch::accumulator_builder<sketch::min_max_accumulator>;
  auto p = rule.fp_rate;
  auto f = detail::overload{
    [](const none_type&) -> sketch_builder_ptr {
      return nullptr;
    },
    [](const bool_type&) -> sketch_builder_ptr {
      // We abuse the min-max sketch to represent min = false and max = true.
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
    [p](const string_type&) -> sketch_builder_ptr {
      return std::make_unique<sketch::bloom_filter_builder>(p);
    },
    [p](const pattern_type&) -> sketch_builder_ptr {
      return std::make_unique<sketch::bloom_filter_builder>(p);
    },
    [p](const address_type&) -> sketch_builder_ptr {
      return std::make_unique<sketch::bloom_filter_builder>(p);
    },
    [p](const subnet_type&) -> sketch_builder_ptr {
      return std::make_unique<sketch::bloom_filter_builder>(p);
    },
    [p](const enumeration_type&) -> sketch_builder_ptr {
      return std::make_unique<sketch::bloom_filter_builder>(p);
    },
    [](const list_type& x) -> sketch_builder_ptr {
      // FIXME: recurse
      return nullptr; // no sketch available
    },
    [](const map_type& x) -> sketch_builder_ptr {
      // FIXME: recurse
      return nullptr; // no sketch available
    },
    [](const record_type&) -> sketch_builder_ptr {
      die("sketch builders can only operate on individual columns");
    },
  };
  return [f](const type& x) {
    return caf::visit(f, x);
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
          builder.field_factory_.emplace(x.field, make_factory(rule));
          return caf::none;
        },
        [&builder, &rule](const type_extractor& x) -> caf::error {
          // TODO: remove the string wrapping once transparent keys work.
          if (builder.type_factory_.contains(x.type.name()))
            return caf::make_error(ec::unspecified,
                                   fmt::format("duplicate type extractor {}",
                                               x.type.name()));
          builder.type_factory_.emplace(x.type.name(), make_factory(rule));
          return caf::none;
        },
      };
      if (auto err = caf::visit(f, *op))
        return err;
    }
  }
  // VAST creates type-level sketches for all types per default. Therefore, we
  // top up the type factory to include all basic types.
  // TODO: consider a generic version that uses tl_filter and the type list
  // concrete_types from type.hpp.
  auto default_factory = make_factory(index_config::rule{});
  auto top_up = [&](basic_type auto basic) {
    auto t = type{basic};
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
  // FIXME: top up list and map
  return builder;
}

partition_sketch_builder::partition_sketch_builder(index_config config)
  : config_{std::move(config)} {
}

caf::error partition_sketch_builder::add(const table_slice& x) {
  const auto& layout = caf::get<record_type>(x.layout());
  // Handle all field sketches.
  for (auto& [key, factory] : field_factory_) {
    auto offsets = layout.resolve_key_suffix(key);
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
    VAST_ASSERT(builder);
    if (auto err = builder->add(x, first_offset))
      return err;
    while (++begin != end) {
      auto offset = *begin;
      auto field = layout.field(offset);
      if (field.type != first_field.type) {
        VAST_WARN("ignoring field '{}' with different type than first field",
                  offset);
        continue;
      }
      if (auto err = builder->add(x, offset))
        return err;
    }
  }
  // Handle all type sketches.
  for (auto&& leaf : layout.leaves()) {
    auto type_name = leaf.field.type.name();
    VAST_WARN("-------> {}: {}", leaf.field.name, type_name);
    // Should we create a sketch for this type?
    auto i = type_factory_.find(type_name);
    if (i == type_factory_.end())
      continue;
    const auto& factory = i->second;
    for (auto name : leaf.field.type.names()) {
      auto& builder = type_builders_[std::string{name}];
      if (!builder)
        builder = factory(leaf.field.type);
      if (!builder)
        return caf::make_error(ec::unspecified,
                               fmt::format("failed to construct sketch "
                                           "builder for type '{}'",
                                           name));
      if (auto err = builder->add(x, leaf.index))
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

} // namespace vast
