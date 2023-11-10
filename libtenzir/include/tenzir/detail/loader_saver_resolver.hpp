//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/parser_interface.hpp>
#include <tenzir/plugin.hpp>

namespace tenzir::detail {

template <typename Plugin>
struct resolve_loader_saver_result {
  std::unique_ptr<Plugin> plugin;
  located<std::string_view> name;
  located<std::string> path;
  bool matched_uri;
};

auto resolve_loader(parser_interface& parser, located<std::string_view> name)
  -> resolve_loader_saver_result<plugin_loader>;

auto resolve_saver(parser_interface& parser, located<std::string_view> name)
  -> resolve_loader_saver_result<plugin_saver>;

auto resolve_decompressor(located<std::string_view> path) -> operator_ptr;

auto resolve_compressor(located<std::string_view> path) -> operator_ptr;

auto resolve_parser(located<std::string_view> path,
                    std::string_view default_parser)
  -> std::pair<operator_ptr, std::unique_ptr<plugin_parser>>;

auto resolve_printer(located<std::string_view> path,
                     std::string_view default_printer)
  -> std::pair<operator_ptr, std::unique_ptr<plugin_printer>>;

} // namespace tenzir::detail
