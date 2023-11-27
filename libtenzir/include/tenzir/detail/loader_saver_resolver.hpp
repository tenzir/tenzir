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
  bool matched_uri;
};

auto resolve_loader(parser_interface& parser, const located<std::string>& name)
  -> resolve_loader_saver_result<plugin_loader>;

auto resolve_saver(parser_interface& parser, const located<std::string>& name)
  -> resolve_loader_saver_result<plugin_saver>;

} // namespace tenzir::detail
