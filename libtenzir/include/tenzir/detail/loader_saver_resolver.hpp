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

auto resolve_loader(parser_interface& parser, const located<std::string>& name)
  -> std::pair<std::unique_ptr<plugin_loader>, located<std::string_view>>;

auto resolve_saver(parser_interface& parser, const located<std::string>& name)
  -> std::pair<std::unique_ptr<plugin_saver>, located<std::string_view>>;

} // namespace tenzir::detail
