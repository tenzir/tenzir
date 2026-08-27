//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

/// @file plugin_fwd.hpp
/// Forward declarations for all plugin types. Include this header when you only
/// need pointers or references to plugin types, avoiding the cost of pulling in
/// the full plugin definitions.

namespace tenzir {

// -- base classes (plugin/base.hpp) -------------------------------------------

class plugin;
class plugin_ptr;

template <class Base>
class serialization_plugin;

// -- component (plugin/component.hpp) -----------------------------------------

class component_plugin;

// -- command (plugin/command.hpp) ----------------------------------------------

class command_plugin;

// -- rest endpoint (plugin/rest_endpoint.hpp) ---------------------------------

class rest_endpoint_plugin;

// -- store (plugin/store.hpp) -------------------------------------------------

class storage_policy_plugin;
class store_actor_plugin;
class store_plugin;

// -- metrics (plugin/metrics.hpp) ---------------------------------------------

class metrics_plugin;

// -- tql2 (tql2/plugin.hpp) ---------------------------------------------------

class operator_factory_plugin;
class function_use;
class function_plugin;
class aggregation_instance;
class aggregation_plugin;
class operator_compiler_plugin;

} // namespace tenzir
