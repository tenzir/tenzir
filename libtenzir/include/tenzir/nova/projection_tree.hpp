//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/box.hpp"
#include "tenzir/detail/heterogeneous_string_hash.hpp"
#include "tenzir/type.hpp"

#include <span>
#include <string_view>

namespace tenzir::nova {

/// A path trie describing which fields of an event a reader must decode.
///
/// Each node is either a wildcard (`is_all()`, meaning the entire subtree below
/// this path must be decoded) or an interior node listing the child segments
/// that are needed. An interior root with no children (`is_empty()`) means zero
/// fields are needed while preserving the event count.
class ProjectionTree {
public:
  class Node {
  public:
    Node() = default;
    Node(Node const&);
    auto operator=(Node const&) -> Node&;
    Node(Node&&) noexcept = default;
    auto operator=(Node&&) noexcept -> Node& = default;
    ~Node();

    auto is_all() const -> bool {
      return all_;
    }

    auto is_empty() const -> bool {
      return not all_ and children_.empty();
    }

    /// Looks up a single literal segment `key`. Returns `this` when the node
    /// is a wildcard (`is_all()`), the matching child node if present, or
    /// `nullptr` if the segment is not selected.
    auto lookup(std::string_view key) const -> Node const*;

  private:
    friend class ProjectionTree;

    auto make_all() -> void {
      all_ = true;
      children_.clear();
    }

    bool all_ = false;
    detail::heterogeneous_string_hashmap<Box<Node>> children_ = {};
  };

  ProjectionTree() = default;

  /// Marks `segments` as required, subsuming any deeper children beneath it.
  /// An empty segment list turns the root into a wildcard (`root().is_all()`).
  auto add_path(std::span<std::string_view const> segments) -> void;

  /// Splits `dotted` on `.` (mirroring `EventBuilder::SelectorPolicy`) and
  /// adds the resulting path.
  auto add_dotted_path(std::string_view dotted) -> void;

  auto root() const -> Node const& {
    return root_;
  }

  /// Prunes `schema` so that record types retain only fields selected by this
  /// tree, preserving type names, attributes, and non-record interior types.
  auto prune_schema(type const& schema) const -> type;

private:
  Node root_;
};

} // namespace tenzir::nova
