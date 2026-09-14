//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/tql2/plugin.hpp>

#include <string>
#include <string_view>

namespace tenzir {

auto normalize_content_type(std::string_view content_type) -> std::string;

auto read_plugin_for_content_type(std::string_view content_type)
  -> const operator_factory_plugin*;

auto read_plugin_for_url_path(std::string_view path)
  -> const operator_factory_plugin*;

auto invocation_for_plugin(const plugin& plugin, location location
                                                 = location::unknown)
  -> ast::invocation;

} // namespace tenzir
