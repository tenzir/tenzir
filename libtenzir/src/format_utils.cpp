//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/detail/string.hpp>
#include <tenzir/format_utils.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/exec.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/resolve.hpp>

#include <algorithm>

namespace tenzir {

namespace {

auto entity_for_plugin(const plugin& plugin,
                       location location = location::unknown) -> ast::entity {
  auto name = plugin.name();
  auto segments = detail::split(name, "::");
  auto identifiers = std::vector<ast::identifier>{};
  for (auto& segment : segments) {
    identifiers.emplace_back(
      segment, &segment == &segments.back() ? location : location::unknown);
  }
  return ast::entity{std::move(identifiers)};
}

} // namespace

auto normalize_content_type(std::string_view content_type) -> std::string {
  content_type = content_type.substr(0, content_type.find(';'));
  content_type = detail::trim(content_type);
  return detail::ascii_tolower(content_type);
}

auto read_plugin_for_content_type(std::string_view content_type)
  -> const operator_factory_plugin* {
  auto normalized = normalize_content_type(content_type);
  if (normalized.empty()) {
    return nullptr;
  }
  for (auto const& plugin : plugins::get<operator_factory_plugin>()) {
    auto props = plugin->read_properties();
    for (auto const& mime_type : props.mime_types) {
      if (detail::ascii_icase_equal(mime_type, normalized)) {
        return plugin;
      }
    }
  }
  return nullptr;
}

auto read_plugin_for_url_path(std::string_view path)
  -> const operator_factory_plugin* {
  auto slash = path.find_last_of('/');
  auto filename
    = slash == std::string_view::npos ? path : path.substr(slash + 1);
  auto const* result = static_cast<const operator_factory_plugin*>(nullptr);
  auto result_extension_size = size_t{0};
  for (auto const& plugin : plugins::get<operator_factory_plugin>()) {
    auto props = plugin->read_properties();
    for (auto const& extension : props.extensions) {
      TENZIR_ASSERT(not extension.empty());
      TENZIR_ASSERT(not extension.starts_with('.'));
      auto matches
        = (filename == extension and extension.find('.') != std::string::npos)
          or (filename.size() > extension.size()
              and filename[filename.size() - extension.size() - 1] == '.'
              and filename.ends_with(extension));
      if (matches and extension.size() > result_extension_size) {
        result = plugin;
        result_extension_size = extension.size();
      }
    }
  }
  return result;
}

auto invocation_for_plugin(const plugin& plugin, location location)
  -> ast::invocation {
  return ast::invocation{entity_for_plugin(plugin, location), {}};
}

} // namespace tenzir
