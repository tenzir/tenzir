//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ir.hpp"
#include "tenzir/option.hpp"

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <cstddef>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

namespace tenzir {

namespace {

/// The glyph drawn for a regular edge. The distribution (direct/scatter/
/// gather/shuffle) is not rendered.
constexpr auto edge_glyph = std::string_view{"->"};

/// The glyph drawn for a fused edge (run-to-completion per item).
constexpr auto fused_edge_glyph = std::string_view{">"};

/// Selects the glyph for a channel based on whether it is fused.
constexpr auto glyph_for(bool fused) -> std::string_view {
  return fused ? fused_edge_glyph : edge_glyph;
}

/// An inline edge to another node, i.e., one that is drawn within a chain.
struct InlineEdge {
  /// Index into `Plan::operators` of the downstream operator.
  size_t to{};
  /// Whether the channel behind this edge is fused.
  bool fused{};
};

/// Per-node state derived from the plan's channels.
///
/// An edge is drawn inline when it is the only outgoing edge of its source and
/// the only incoming edge of its target; all other edges end up in the
/// `links:` section.
struct Node {
  /// The number of channels entering and leaving this node.
  size_t in_degree{};
  size_t out_degree{};
  /// The inline predecessor, if any.
  Option<size_t> prev;
  /// The inline successor, if any.
  Option<InlineEdge> next;
  /// Whether the inline edge from `{input}` exists, and whether it is fused.
  Option<bool> from_input;
  /// Whether the inline edge to `{output}` exists, and whether it is fused.
  Option<bool> to_output;
};

/// The minimal, stable label for a planned operator node.
///
/// Non-default properties are appended as a single parenthesized, comma-
/// separated annotation group: `x<n>` for the number of parallel instances,
/// and `keyed` when input is partitioned by key. Operators with no such
/// properties carry no parentheses.
auto plan_node_label(const ir::PlannedOperator& node) -> std::string {
  auto label = node.op->display_name();
  auto annotations = std::vector<std::string>{};
  if (node.parallelism > 1) {
    annotations.push_back(fmt::format("x{}", node.parallelism));
  }
  if (node.keyed()) {
    annotations.emplace_back("keyed");
  }
  if (not annotations.empty()) {
    label += fmt::format("({})", fmt::join(annotations, ", "));
  }
  return label;
}

} // namespace

auto ir::fmt_ir_plan(const ir::Plan& plan) -> std::string {
  const auto num_ops = plan.operators.size();
  const auto is_op = [&](size_t x) {
    return x < num_ops;
  };
  auto nodes = std::vector<Node>(num_ops);
  for (const auto& c : plan.channels) {
    if (is_op(c.from)) {
      ++nodes[c.from].out_degree;
    }
    if (is_op(c.to)) {
      ++nodes[c.to].in_degree;
    }
  }
  // Classify every channel as either an inline edge or a link.
  auto link_channels = std::vector<const ir::Channel*>{};
  for (const auto& c : plan.channels) {
    const auto sole_out = is_op(c.from) and nodes[c.from].out_degree == 1;
    const auto sole_in = is_op(c.to) and nodes[c.to].in_degree == 1;
    if (c.from == ir::Port::input and sole_in) {
      nodes[c.to].from_input = c.fused;
    } else if (c.to == ir::Port::output and sole_out) {
      nodes[c.from].to_output = c.fused;
    } else if (sole_out and sole_in) {
      nodes[c.from].next = InlineEdge{c.to, c.fused};
      nodes[c.to].prev = c.from;
    } else {
      link_channels.push_back(&c);
    }
  }
  // Build maximal chains, starting at the nodes without inline predecessor.
  // The `visited` bookkeeping guards against cyclic plans, which have no such
  // node at all; their nodes form chains of their own below.
  auto chains = std::vector<std::vector<size_t>>{};
  auto visited = std::vector<bool>(num_ops, false);
  const auto make_chain = [&](size_t start) {
    auto chain = std::vector<size_t>{start};
    visited[start] = true;
    auto cur = start;
    while (nodes[cur].next and not visited[nodes[cur].next->to]) {
      cur = nodes[cur].next->to;
      visited[cur] = true;
      chain.push_back(cur);
    }
    chains.push_back(std::move(chain));
  };
  for (auto u = size_t{0}; u < num_ops; ++u) {
    if (not nodes[u].prev) {
      make_chain(u);
    }
  }
  for (auto u = size_t{0}; u < num_ops; ++u) {
    if (not visited[u]) {
      make_chain(u);
    }
  }
  // Deterministic chain order by head node index.
  std::ranges::sort(chains, [](const auto& a, const auto& b) {
    return a.front() < b.front();
  });
  // Map each node to its chain coordinate.
  auto chain_of = std::vector<size_t>(num_ops, 0);
  for (auto ci = size_t{0}; ci < chains.size(); ++ci) {
    for (const auto node : chains[ci]) {
      chain_of[node] = ci;
    }
  }
  auto out = std::string{};
  // Print chains.
  for (auto ci = size_t{0}; ci < chains.size(); ++ci) {
    const auto& chain = chains[ci];
    const auto& head = nodes[chain.front()];
    const auto label = [&](size_t node) {
      return plan_node_label(plan.operators[node]);
    };
    if (head.from_input) {
      out += fmt::format("c{}: {{input}} {} {}", ci,
                         glyph_for(*head.from_input), label(chain.front()));
    } else {
      out += fmt::format("c{}: {}", ci, label(chain.front()));
    }
    for (auto i = size_t{1}; i < chain.size(); ++i) {
      out += fmt::format(" {} {}", glyph_for(nodes[chain[i - 1]].next->fused),
                         label(chain[i]));
    }
    if (const auto& tail = nodes[chain.back()]; tail.to_output) {
      out += fmt::format(" {} {{output}}", glyph_for(*tail.to_output));
    }
    out += '\n';
  }
  // Print links, ordered by the chains they connect. The plan's input sorts
  // first and its output last.
  if (not link_channels.empty()) {
    const auto rank = [&](size_t x) -> size_t {
      if (x == ir::Port::input) {
        return 0;
      }
      if (x == ir::Port::output) {
        return chains.size() + 1;
      }
      return chain_of[x] + 1;
    };
    std::ranges::sort(link_channels, [&](const auto* a, const auto* b) {
      return std::tuple{rank(a->from), rank(a->to), a->from_port}
             < std::tuple{rank(b->from), rank(b->to), b->from_port};
    });
    const auto ref = [&](size_t x) -> std::string {
      if (x == ir::Port::input) {
        return "{input}";
      }
      if (x == ir::Port::output) {
        return "{output}";
      }
      return fmt::format("c{}", chain_of[x]);
    };
    out += "\nlinks:\n";
    for (const auto* c : link_channels) {
      out += fmt::format("  {} {} {}\n", ref(c->from), glyph_for(c->fused),
                         ref(c->to));
    }
  }
  return out;
}

} // namespace tenzir
