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
#include "vast/sketch/bloom_filter_builder.hpp"
#include "vast/table_slice.hpp"

namespace vast {

namespace {

using builder_factory = partition_sketch_builder::builder_factory;

/// Constructs a builder factory for a given type and rule parameters.
caf::expected<builder_factory>
make_builder_factory(const type& t, const index_config::rule& rule) {
  auto make_bloom_filter_builder = [&](double p) -> builder_factory {
    return [p] {
      return std::make_unique<sketch::bloom_filter_builder>(p);
    };
  };
  auto f = detail::overload{
    // TODO: for numeric and time types, use a min-max sketch.
    [&](const auto&) -> caf::expected<builder_factory> {
      return caf::make_error(ec::type_clash,
                             fmt::format("no sketch available for type {}", t));
    },
    [&](const string_type&) -> caf::expected<builder_factory> {
      return make_bloom_filter_builder(rule.fp_rate);
    },
    [&](const address_type&) -> caf::expected<builder_factory> {
      return make_bloom_filter_builder(rule.fp_rate);
    },
  };
  return caf::visit(f, t);
}

} // namespace

partition_sketch_builder::partition_sketch_builder(index_config config)
  : config_{std::move(config)} {
  // Parse config and populate builder factories.
  for (const auto& rule : config_.rules) {
    // Create one sketch per target.
    for (const auto& target : rule.targets) {
      auto op = to<predicate::operand>(target);
      if (!op) {
        VAST_WARN("ignoring invalid rule target '{}'", target);
        continue;
      }
      // Register builder factories and factory stubs where typing is absent.
      auto f = detail::overload{
        [&](const auto&) {
          // TODO(MV): add formatter for predicate::operand
          // VAST_WARN("ignoring unsupported predicate operand '{}'", *op);
          VAST_WARN("ignoring unsupported predicate operand");
        },
        [this](const field_extractor& x) {
          // We cannot construct concrete factories at startup time because
          // the field type is not known until the first event arrives.
          // Therefore, we only register the name and construct the factory
          // on first sight.
          // TODO(MV): there is currently no mechanism in place to ensure
          // that a fully qualified field is actually present.
          field_factory_.emplace(x.field, builder_factory{});
        },
        [this, &rule](const type_extractor& x) {
          // FIXME: handle duplicates in key space or rethink data structure.
          if (auto factory = make_builder_factory(x.type, rule))
            type_factory_.emplace(x.type, *factory);
        },
      };
      caf::visit(f, *op);
    }
  }
}

caf::error partition_sketch_builder::add(const table_slice& x) {
  const auto& layout = caf::get<record_type>(x.layout());
  for (auto leaf : layout.leaves()) {
    // Add column to corresopnding field sketch.
    auto fqf = qualified_record_field{x.layout(), leaf.index};
    sketch::builder* builder = nullptr;
    if (auto i = field_builders_.find(fqf); i != field_builders_.end()) {
      builder = i->second.get();
    } else if (auto i = field_factory_.find(fqf.name());
               i != field_factory_.end()) {
      // 👆 On-the-fly construction of an FQF might be expensive. Measure.
      auto new_builder = i->second();
      builder = new_builder.get();
      field_builders_.emplace(std::move(fqf), std::move(new_builder));
    }
    if (builder)
      if (auto err = builder->add(x, leaf.index))
        return err;
    // Add column to corresopnding type sketch.
    // We currently offer only type sketches for basic types and therefore
    // prune the type meta data, i.e., names and attributes.
    // TODO(MV): it would make sense to allow differentiate sketches for type
    // aliases, so we should revisit this in the future and only strip
    // attributes.
    auto prune = [&]<concrete_type T>(const T& x) {
      return type{x};
    };
    auto pruned = caf::visit(prune, leaf.field.type);
    if (auto i = type_builders_.find(pruned); i != type_builders_.end()) {
      builder = i->second.get();
    } else if (auto i = type_factory_.find(pruned); i != type_factory_.end()) {
      auto new_builder = i->second();
      builder = new_builder.get();
      type_builders_.emplace(std::move(pruned), std::move(new_builder));
    }
    if (builder)
      if (auto err = builder->add(x, leaf.index))
        return err;
  }
  return caf::none;
}

caf::expected<partition_sketch> partition_sketch_builder::finish() {
  // TODO: implement.
  return ec::unimplemented;
}

} // namespace vast
