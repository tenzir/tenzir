//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

// This header provides the free template functions plugin_serialize() and
// plugin_inspect() which are used by pipeline.hpp for operator serialization.
//
// It is extracted from plugin.hpp to break the circular dependency:
//   - pipeline.hpp needs plugin_serialize/plugin_inspect
//   - plugin.hpp needs pipeline.hpp for operator_signature, operator_ptr, etc.
//
// The serialization_plugin and inspection_plugin class templates are defined in
// plugin/base.hpp, which includes this header.

#include "tenzir/fwd.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/debug_writer.hpp"
#include "tenzir/error.hpp"
#include "tenzir/logger.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/deserializer.hpp>
#include <caf/detail/pretty_type_name.hpp>
#include <caf/detail/stringification_inspector.hpp>
#include <caf/error.hpp>
#include <caf/serializer.hpp>

#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <variant>

namespace tenzir {

// Type aliases for serializer/deserializer variants (also defined in
// pipeline.hpp)
using serializer
  = std::variant<std::reference_wrapper<caf::serializer>,
                 std::reference_wrapper<caf::binary_serializer>,
                 std::reference_wrapper<caf::detail::stringification_inspector>>;

using deserializer
  = std::variant<std::reference_wrapper<caf::deserializer>,
                 std::reference_wrapper<caf::binary_deserializer>>;

// Forward declaration of serialization_plugin (fully defined in plugin/base.hpp)
template <class Base>
class serialization_plugin;

namespace plugins {

// Forward declaration of plugins::find - will be fully defined in plugin.hpp
template <class Plugin>
auto find(std::string_view name) noexcept -> const Plugin*;

} // namespace plugins

// -- Free functions for plugin serialization ---------------------------------
// These are used by pipeline.hpp for operator serialization

template <class Inspector, class Base>
auto plugin_serialize(Inspector& f, const Base& x) -> bool {
  static_assert(not Inspector::is_loading);
  auto name = x.name();
  auto const* p = plugins::find<serialization_plugin<Base>>(name);
  if (auto dbg = as_debug_writer(f)) {
    if (not dbg->prepend("{} ", name)) {
      return false;
    }
    // Workaround for debug formatting non-serializable plugins. In that case we
    // only print the name instead of throwing an exception.
    if (not p) {
      return dbg->fmt_value("<no serialization plugin>");
    }
  } else {
    if (not f.apply(name)) {
      return false;
    }
  }
  TENZIR_ASSERT(p, fmt::format("serialization plugin `{}` for `{}` not found",
                               name, detail::pretty_type_name(typeid(Base))));
  return p->serialize(std::ref(f), x);
}

/// Inspects a polymorphic object `x` by using the serialization plugin with the
/// name that matches `x->name()`.
template <class Inspector, class Base>
auto plugin_inspect(Inspector& f, std::unique_ptr<Base>& x) -> bool {
  if constexpr (Inspector::is_loading) {
    auto name = std::string{};
    if (not f.apply(name)) {
      return false;
    }
    auto const* p = plugins::find<serialization_plugin<Base>>(name);
    TENZIR_ASSERT(p, fmt::format("serialization plugin `{}` for `{}` not found",
                                 name, detail::pretty_type_name(typeid(Base))));
    p->deserialize(f, x);
    return x != nullptr;
  } else {
    if (auto dbg = as_debug_writer(f)) {
      if (not x) {
        return dbg->fmt_value("<invalid>");
      }
    }
    TENZIR_ASSERT(x);
    return plugin_serialize(f, *x);
  }
}

} // namespace tenzir
