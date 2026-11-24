//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/projection_tree.hpp"

#include <memory>
#include <string>
#include <vector>

namespace tenzir::nova {

namespace {

auto prune_schema_impl(type const& schema, ProjectionTree::Node const& node)
  -> type {
  if (node.is_all() or not is<record_type>(schema)) {
    return schema;
  }
  auto const& record = as<record_type>(schema);
  auto fields = std::vector<record_type::field_view>{};
  for (auto field : record.fields()) {
    auto const* child = node.lookup(field.name);
    if (not child) {
      continue;
    }
    fields.emplace_back(field.name, prune_schema_impl(field.type, *child));
  }
  auto result = type{record_type{std::move(fields)}};
  result.assign_metadata(schema);
  return result;
}

} // namespace

ProjectionTree::Node::Node(Node const&) = default;
auto ProjectionTree::Node::operator=(Node const&) -> Node& = default;
ProjectionTree::Node::~Node() = default;

auto ProjectionTree::Node::lookup(std::string_view key) const -> Node const* {
  if (all_) {
    return this;
  }
  auto it = children_.find(key);
  if (it == children_.end()) {
    return nullptr;
  }
  return std::addressof(*it->second);
}

auto ProjectionTree::add_path(std::span<std::string_view const> segments)
  -> void {
  if (root_.all_) {
    return;
  }
  if (segments.empty()) {
    root_.make_all();
    return;
  }
  auto* current = &root_;
  for (auto segment : segments) {
    if (current->all_) {
      return;
    }
    auto it = current->children_.find(segment);
    if (it == current->children_.end()) {
      it = current->children_
             .emplace(std::string{segment}, Box<Node>{std::in_place})
             .first;
    }
    current = std::addressof(*it.value());
  }
  current->make_all();
}

auto ProjectionTree::add_dotted_path(std::string_view dotted) -> void {
  auto segments = std::vector<std::string_view>{};
  while (true) {
    auto const dot = dotted.find('.');
    segments.push_back(dotted.substr(0, dot));
    if (dot == std::string_view::npos) {
      break;
    }
    dotted = dotted.substr(dot + 1);
  }
  add_path(segments);
}

auto ProjectionTree::prune_schema(type const& schema) const -> type {
  return prune_schema_impl(schema, root_);
}

} // namespace tenzir::nova
