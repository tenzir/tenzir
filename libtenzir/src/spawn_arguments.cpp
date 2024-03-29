//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/spawn_arguments.hpp"

#include "tenzir/concept/parseable/tenzir/schema.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/detail/load_contents.hpp"
#include "tenzir/error.hpp"
#include "tenzir/module.hpp"

#include <caf/config_value.hpp>
#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/settings.hpp>
#include <caf/string_algorithms.hpp>

#include <filesystem>
#include <optional>

namespace tenzir {

caf::expected<std::optional<module>> read_module(const spawn_arguments& args) {
  auto module_file_ptr = caf::get_if<std::string>(&args.inv.options, "schema");
  if (!module_file_ptr)
    return std::optional<module>{std::nullopt};
  auto str = detail::load_contents(std::filesystem::path{*module_file_ptr});
  if (!str)
    return str.error();
  auto result = to<module>(*str);
  if (!result)
    return result.error();
  return std::optional<module>{std::move(*result)};
}

caf::error unexpected_arguments(const spawn_arguments& args) {
  return caf::make_error(ec::syntax_error, "unexpected argument(s)",
                         caf::join(args.inv.arguments.begin(),
                                   args.inv.arguments.end(), " "));
}

} // namespace tenzir
