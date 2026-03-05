//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
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

// -- operator (plugin/operator.hpp) -------------------------------------------

class operator_parser_plugin;

// -- loader (plugin/loader.hpp) -----------------------------------------------

class plugin_loader;
class loader_parser_plugin;

// -- parser (plugin/parser.hpp) -----------------------------------------------

class plugin_parser;
class parser_parser_plugin;

// -- printer (plugin/printer.hpp) ---------------------------------------------

class printer_instance;
class plugin_printer;
class printer_parser_plugin;

// -- saver (plugin/saver.hpp) -------------------------------------------------

struct printer_info;
class plugin_saver;
class saver_parser_plugin;

// -- rest endpoint (plugin/rest_endpoint.hpp) ---------------------------------

class rest_endpoint_plugin;

// -- store (plugin/store.hpp) -------------------------------------------------

class store_actor_plugin;
class store_plugin;

// -- metrics (plugin/metrics.hpp) ---------------------------------------------

class metrics_plugin;

// -- aspect (plugin/aspect.hpp) -----------------------------------------------

class aspect_plugin;

// -- tql2 (tql2/plugin.hpp) ---------------------------------------------------

class operator_factory_plugin;
class function_use;
class function_plugin;
class aggregation_instance;
class aggregation_plugin;
class operator_compiler_plugin;

} // namespace tenzir
