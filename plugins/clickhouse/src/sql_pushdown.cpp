//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/sql_pushdown.hpp"

#include "clickhouse/arguments.hpp"
#include "clickhouse/block_decoding.hpp"
#include "clickhouse/transformers.hpp"
#include "tenzir/detail/escapers.hpp"
#include "tenzir/detail/flat_map.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/tql2/ast.hpp"

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <array>

namespace tenzir::plugins::clickhouse {

namespace {

/// Strips `Nullable(...)` and `LowCardinality(...)` wrappers.
auto unwrap_column_type(std::string_view type) -> std::string_view {
  while (true) {
    if (auto inner = unwrap_clickhouse_type_call(type, "Nullable")) {
      type = *inner;
      continue;
    }
    if (auto inner = unwrap_clickhouse_type_call(type, "LowCardinality")) {
      type = *inner;
      continue;
    }
    return type;
  }
}

} // namespace

// -- SqlSchema ----------------------------------------------------------------

auto SqlSchema::make_node(std::string_view type) -> Node {
  auto node = Node{.type = std::string{type}, .children = {}};
  auto elements
    = unwrap_clickhouse_type_call(unwrap_column_type(type), "Tuple");
  if (not elements) {
    return node;
  }
  for (auto element : split_top_level_clickhouse_type_arguments(*elements)) {
    auto split = find_top_level_clickhouse_type_space(element);
    // Unnamed tuple elements are only addressable by index in SQL.
    if (split == std::string_view::npos) {
      continue;
    }
    auto name
      = unquote_identifier_component(detail::trim(element.substr(0, split)));
    if (node.children.contains(name)) {
      continue;
    }
    auto child = make_node(detail::trim(element.substr(split + 1)));
    node.children.emplace(std::move(name), Box<Node>{std::move(child)});
  }
  return node;
}

auto SqlSchema::add_column(std::string_view name, std::string_view type,
                           bool generated) -> void {
  columns_.emplace_back(name);
  if (generated) {
    generated_.emplace(std::string{name});
    return;
  }
  auto normalized = remove_non_significant_whitespace(type);
  if (not is_decodable_type(normalized) or nodes_.contains(name)) {
    return;
  }
  nodes_.emplace(std::string{name}, make_node(normalized));
}

auto SqlSchema::is_generated(std::string_view name) const -> bool {
  return generated_.contains(std::string{name});
}

auto SqlSchema::columns() const -> std::span<const std::string> {
  return columns_;
}

auto SqlSchema::find_node(std::span<const std::string> path) const
  -> Node const* {
  if (path.empty()) {
    return nullptr;
  }
  auto it = nodes_.find(path.front());
  if (it == nodes_.end()) {
    return nullptr;
  }
  auto const* node = &it->second;
  for (auto const& segment : path.subspan(1)) {
    auto child = node->children.find(segment);
    if (child == node->children.end()) {
      return nullptr;
    }
    node = &*child->second;
  }
  return node;
}

namespace {

/// The tuple elements that a projection requests below a node.
struct Requested {
  detail::flat_map<std::string, Box<Requested>> children;
  /// Whether the node itself is requested, which subsumes its elements.
  bool whole = false;
};

} // namespace

auto SqlSchema::render_projection(
  std::string_view name, std::span<const std::vector<std::string>> paths) const
  -> Option<std::string> {
  auto const* root = find_node(std::array{std::string{name}});
  if (not root or root->children.empty()) {
    return None{};
  }
  auto requested = Requested{};
  for (auto const& path : paths) {
    auto* node = root;
    auto* request = &requested;
    for (auto const& segment : path) {
      auto child = node->children.find(segment);
      // A path outside the schema selects the column whole; the local
      // `select` then yields `null` for it either way.
      if (child == node->children.end()) {
        return None{};
      }
      node = &*child->second;
      auto next = request->children.find(segment);
      if (next == request->children.end()) {
        next = request->children.emplace(segment, Box<Requested>{std::in_place})
                 .first;
      }
      request = &*next->second;
    }
    request->whole = true;
  }
  if (requested.whole) {
    return None{};
  }
  // Rebuilds the tuple below `node` with the requested elements only, in
  // declaration order, and casts it back to a named tuple so that it decodes
  // to a record.
  auto render
    = [](this auto const& self, Node const& node, Requested const& request,
         std::string const& sql) -> std::pair<std::string, std::string> {
    if (request.whole or node.children.empty()) {
      return {sql, node.type};
    }
    auto values = std::vector<std::string>{};
    auto elements = std::vector<std::string>{};
    for (auto const& [child_name, child] : node.children) {
      auto it = request.children.find(child_name);
      if (it == request.children.end()) {
        continue;
      }
      auto quoted = quote_sql_identifier(child_name);
      // `t`.`a` would read a column named `t.a` instead, if there is one.
      auto element = fmt::format("tupleElement({}, {})", sql,
                                 quote_sql_string(child_name));
      auto [value, type] = self(*child, *it->second, element);
      values.push_back(std::move(value));
      elements.push_back(fmt::format("{} {}", quoted, type));
    }
    auto type = fmt::format("Tuple({})", fmt::join(elements, ", "));
    return {fmt::format("CAST(tuple({}), {})", fmt::join(values, ", "),
                        quote_sql_string(type)),
            std::move(type)};
  };
  auto quoted = quote_sql_identifier(name);
  return fmt::format("{} AS {}", render(*root, requested, quoted).first,
                     quoted);
}

auto select_columns(SqlSchema const* schema,
                    Option<ir::OptimizeProjection> const& projection)
  -> std::string {
  auto result = std::string{"*"};
  if (schema and projection) {
    // The nested paths requested below each top-level column, relative to it.
    auto wanted
      = detail::flat_map<std::string, std::vector<std::vector<std::string>>>{};
    for (auto const& path : *projection) {
      auto segments = path.path();
      if (segments.empty()) {
        continue;
      }
      auto relative = std::vector<std::string>{};
      for (auto const& segment : segments.subspan(1)) {
        relative.push_back(segment.id.name);
      }
      wanted[segments.front().id.name].push_back(std::move(relative));
    }
    // Whether `SELECT *` contains a generated column depends on session
    // settings, so a projection that names one keeps `*` to match either way.
    auto names_generated = std::ranges::any_of(wanted, [&](auto const& entry) {
      return schema->is_generated(entry.first);
    });
    auto selected = std::vector<std::string>{};
    for (auto const& column : schema->columns()) {
      auto it = wanted.find(column);
      if (it == wanted.end()) {
        continue;
      }
      if (auto narrowed = schema->render_projection(column, it->second)) {
        selected.push_back(std::move(*narrowed));
      } else {
        selected.push_back(quote_sql_identifier(column));
      }
    }
    if (not names_generated and not selected.empty()) {
      result = fmt::format("{}", fmt::join(selected, ", "));
    }
  }
  return result;
}

auto quote_sql_identifier(std::string_view name) -> std::string {
  auto result = std::string{"`"};
  for (auto c : name) {
    if (c == '`' or c == '\\') {
      result += '\\';
    }
    result += c;
  }
  result += '`';
  return result;
}

auto quote_sql_string(std::string_view text) -> std::string {
  // Backslash-escapes the quote and the backslash, and hex-escapes control
  // bytes; every other byte, including non-ASCII, passes through.
  auto escaper = [](auto& f, auto out) {
    auto byte = static_cast<unsigned char>(*f);
    if (*f == '\'' or *f == '\\') {
      *out++ = '\\';
      *out++ = *f++;
    } else if (byte < 0x20 or byte == 0x7f) {
      detail::hex_escaper(f, out);
    } else {
      *out++ = *f++;
    }
  };
  return fmt::format("'{}'", detail::escape(text, escaper));
}

} // namespace tenzir::plugins::clickhouse
