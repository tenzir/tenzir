//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/assert.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/option.hpp"

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <cstddef>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

namespace tenzir {

namespace {

/// The plan is rendered as a `git log --graph`-style lane diagram that flows
/// top-down in data-flow direction: one node per line, with the channels drawn
/// as connector lines in between.
///
/// Channels use box-drawing glyphs: single for regular channels, doubled for
/// fused ones (run-to-completion per item), and dashed for tiny ones (one of
/// many channels, hence a small memory budget each).
///
/// Unicode has no dashed tees or corners, so runs involving tiny channels reuse
/// the single-line connectors; only the straight segments are dashed.

using Kind = ir::ChannelKind;

/// The glyph marking a node in its own lane.
constexpr auto node_glyph = std::string_view{"●"};

constexpr auto vertical(Kind kind) -> std::string_view {
  switch (kind) {
    case Kind::fused:
      return "║";
    case Kind::tiny:
      return "╎";
    case Kind::regular:
      return "│";
  }
  TENZIR_UNREACHABLE();
}

constexpr auto horizontal(Kind kind) -> std::string_view {
  switch (kind) {
    case Kind::fused:
      return "═";
    case Kind::tiny:
      return "╌";
    case Kind::regular:
      return "─";
  }
  TENZIR_UNREACHABLE();
}

/// Whether a glyph for `kind` is drawn with doubled lines. Tiny channels borrow
/// the single-line connectors.
constexpr auto doubled(Kind kind) -> bool {
  return kind == Kind::fused;
}

/// The corner where a channel leaves the fan-out run downwards. The vertical
/// and left-horizontal styles can differ.
constexpr auto corner_down(Kind vertical_kind, Kind horizontal_kind)
  -> std::string_view {
  if (doubled(vertical_kind)) {
    return doubled(horizontal_kind) ? "╗" : "╖";
  }
  return doubled(horizontal_kind) ? "╕" : "┐";
}

/// The corner where a channel joins the fan-in run from above. The vertical
/// and left-horizontal styles can differ.
constexpr auto corner_up(Kind vertical_kind, Kind horizontal_kind)
  -> std::string_view {
  if (doubled(vertical_kind)) {
    return doubled(horizontal_kind) ? "╝" : "╜";
  }
  return doubled(horizontal_kind) ? "╛" : "┘";
}

/// An intermediate tee of a fan-out or fan-in run. The vertical and horizontal
/// styles can differ. When the left and right horizontal styles differ (left
/// single, right fused — which happens when this leg is the rightmost
/// non-fused one), there is no matching Unicode box-drawing glyph; the left
/// (single) style is used as the approximation.
constexpr auto tee_down(Kind vertical_kind, Kind horizontal_kind)
  -> std::string_view {
  if (doubled(vertical_kind)) {
    return doubled(horizontal_kind) ? "╦" : "╥";
  }
  return doubled(horizontal_kind) ? "╤" : "┬";
}

constexpr auto tee_up(Kind vertical_kind, Kind horizontal_kind)
  -> std::string_view {
  if (doubled(vertical_kind)) {
    return doubled(horizontal_kind) ? "╩" : "╨";
  }
  return doubled(horizontal_kind) ? "╧" : "┴";
}

/// The tee at the anchor lane of a run, where the vertical and the horizontal
/// style can differ.
constexpr auto tee_right(Kind vertical_kind, Kind horizontal_kind)
  -> std::string_view {
  if (doubled(vertical_kind)) {
    return doubled(horizontal_kind) ? "╠" : "╟";
  }
  return doubled(horizontal_kind) ? "╞" : "├";
}

/// A channel that has been drawn from its source but not yet into its target.
struct Lane {
  bool used = false;
  size_t target = 0;
  Kind kind = Kind::regular;
};

/// One participant of a fan-out or fan-in run: its lane and channel style.
struct Leg {
  size_t lane{};
  Kind kind{};
};

/// The lanes currently in flight, indexed by column.
class LaneSet {
public:
  auto operator[](size_t i) -> Lane& {
    return lanes_[i];
  }

  auto get(size_t i) const -> Lane {
    return i < lanes_.size() ? lanes_[i] : Lane{};
  }

  /// Allocate the leftmost free lane at or after `start`.
  auto allocate(size_t start) -> size_t {
    auto i = start;
    while (i < lanes_.size() and lanes_[i].used) {
      ++i;
    }
    if (i >= lanes_.size()) {
      lanes_.resize(i + 1);
    }
    return i;
  }

  /// The rightmost lane in use, or `None` if all lanes are free.
  auto last_used() const -> Option<size_t> {
    for (auto i = lanes_.size(); i > 0; --i) {
      if (lanes_[i - 1].used) {
        return i - 1;
      }
    }
    return None{};
  }

private:
  std::vector<Lane> lanes_;
};

/// Draw the line that carries a node, with `node_glyph` in the node's own lane.
auto node_line(const LaneSet& lanes, size_t node_lane, std::string_view label)
  -> std::string {
  auto width = node_lane;
  if (const auto last = lanes.last_used()) {
    width = std::max(width, *last);
  }
  auto out = std::string{};
  for (auto i = size_t{0}; i <= width; ++i) {
    if (i > 0) {
      out += ' ';
    }
    if (i == node_lane) {
      out += node_glyph;
    } else if (const auto lane = lanes.get(i); lane.used) {
      out += vertical(lane.kind);
    } else {
      out += ' ';
    }
  }
  out += ' ';
  out += label;
  out += '\n';
  return out;
}

/// Draw a line that merely continues all lanes in flight.
auto plain_line(const LaneSet& lanes) -> std::string {
  const auto width = lanes.last_used();
  if (not width) {
    return {};
  }
  auto out = std::string{};
  for (auto i = size_t{0}; i <= *width; ++i) {
    if (i > 0) {
      out += ' ';
    }
    if (const auto lane = lanes.get(i); lane.used) {
      out += vertical(lane.kind);
    } else {
      out += ' ';
    }
  }
  out += '\n';
  return out;
}

/// Draw a fan-out (`down`) or fan-in (not `down`) run. The legs must be sorted
/// by lane; the first one is the anchor lane that the run extends from.
auto fan_line(const LaneSet& lanes, std::span<const Leg> legs, bool down)
  -> std::string {
  TENZIR_ASSERT(legs.size() > 1);
  const auto lo = legs.front().lane;
  const auto hi = legs.back().lane;
  const auto leg_at = [&](size_t lane) -> Option<Leg> {
    const auto it = std::ranges::find(legs, lane, &Leg::lane);
    if (it == legs.end()) {
      return None{};
    }
    return *it;
  };
  // A horizontal run right of column `x` carries all legs beyond it, so it only
  // adopts their style if they agree on it; otherwise it stays regular.
  const auto run_kind = [&](size_t x) {
    auto beyond = legs | std::views::filter([&](const Leg& leg) {
                    return leg.lane > x;
                  });
    auto it = beyond.begin();
    if (it == beyond.end()) {
      return Kind::regular;
    }
    const auto kind = it->kind;
    return std::ranges::all_of(beyond,
                               [&](const Leg& leg) {
                                 return leg.kind == kind;
                               })
             ? kind
             : Kind::regular;
  };
  auto width = hi;
  if (const auto last = lanes.last_used()) {
    width = std::max(width, *last);
  }
  auto out = std::string{};
  for (auto i = size_t{0}; i <= width; ++i) {
    if (i > 0) {
      out += (i - 1 >= lo and i <= hi) ? horizontal(run_kind(i - 1)) : " ";
    }
    const auto leg = leg_at(i);
    if (i == lo) {
      out += tee_right(leg->kind, run_kind(i));
    } else if (leg) {
      const auto last = i == hi;
      // The left-horizontal style is run_kind(i-1); right is run_kind(i). When
      // the two sides disagree there is no matching Unicode glyph, so the left
      // style is used as the approximation, as documented in tee_down/tee_up.
      const auto horiz_kind = run_kind(i - 1);
      if (down) {
        out += last ? corner_down(leg->kind, horiz_kind)
                    : tee_down(leg->kind, horiz_kind);
      } else {
        out += last ? corner_up(leg->kind, horiz_kind)
                    : tee_up(leg->kind, horiz_kind);
      }
    } else if (lo < i and i < hi) {
      // A run passing over an unrelated lane interrupts it, so that it is
      // obvious that the two do not merge.
      out += horizontal(run_kind(i));
    } else if (const auto lane = lanes.get(i); lane.used) {
      out += vertical(lane.kind);
    } else {
      out += ' ';
    }
  }
  while (not out.empty() and out.back() == ' ') {
    out.pop_back();
  }
  out += '\n';
  return out;
}

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

/// An outgoing channel of a node, in port order.
struct OutEdge {
  size_t to{};
  size_t port{};
  Kind kind{};
};

} // namespace

auto ir::fmt_ir_plan(const ir::Plan& plan) -> std::string {
  // The plan's external input and output are modeled as two extra nodes right
  // after the operators, so that boundary channels need no special casing.
  const auto num_ops = plan.operators.size();
  const auto input_node = num_ops;
  const auto output_node = num_ops + 1;
  const auto total = num_ops + 2;
  const auto node_of = [&](size_t x) {
    if (x == ir::Port::input) {
      return input_node;
    }
    if (x == ir::Port::output) {
      return output_node;
    }
    return x;
  };
  auto out_edges = std::vector<std::vector<OutEdge>>(total);
  auto in_degree = std::vector<size_t>(total, 0);
  auto exists = std::vector<bool>(total, false);
  for (auto i = size_t{0}; i < num_ops; ++i) {
    exists[i] = true;
  }
  for (const auto& c : plan.channels) {
    const auto from = node_of(c.from);
    const auto to = node_of(c.to);
    exists[from] = true;
    exists[to] = true;
    out_edges[from].push_back({to, c.from_port, c.kind});
    ++in_degree[to];
  }
  for (auto& edges : out_edges) {
    std::ranges::sort(edges, [](const OutEdge& a, const OutEdge& b) {
      return std::tuple{a.port, a.to} < std::tuple{b.port, b.to};
    });
  }
  const auto label = [&](size_t node) -> std::string {
    if (node == input_node) {
      return "{input}";
    }
    if (node == output_node) {
      return "{output}";
    }
    return plan_node_label(plan.operators[node]);
  };
  // Nodes without incoming channels are emitted in this order: the plan input
  // first, then the operators, and the plan output last.
  const auto root_rank = [&](size_t node) -> size_t {
    if (node == input_node) {
      return 0;
    }
    if (node == output_node) {
      return total;
    }
    return node + 1;
  };
  auto out = std::string{};
  auto lanes = LaneSet{};
  auto emitted = std::vector<bool>(total, false);
  auto pending_in = std::vector<size_t>(total, 0);
  auto remaining = static_cast<size_t>(std::ranges::count(exists, true));
  // Whether the last line drawn is already a connector, in which case no plain
  // continuation line is inserted before the next node.
  auto connector = false;
  auto first = true;
  while (remaining > 0) {
    // A node can be drawn once all of its incoming channels are in flight.
    const auto ready = [&](size_t node) {
      return exists[node] and not emitted[node]
             and pending_in[node] == in_degree[node];
    };
    // Prefer the ready node that occupies the leftmost lane in flight, so that
    // lanes are consumed in the order they were opened.
    auto next = Option<size_t>{};
    if (const auto last = lanes.last_used()) {
      for (auto lane = size_t{0}; lane <= *last; ++lane) {
        const auto l = lanes.get(lane);
        if (l.used and ready(l.target)) {
          next = l.target;
          break;
        }
      }
    }
    if (not next) {
      // No node is fed by a lane in flight, so pick a source node.
      for (auto node = size_t{0}; node < total; ++node) {
        if (ready(node) and (not next or root_rank(node) < root_rank(*next))) {
          next = node;
        }
      }
    }
    if (not next) {
      // Nothing is ready: the plan has a cycle, which should not occur. Draw
      // the remaining nodes in index order, ignoring their unresolved inputs.
      for (auto node = size_t{0}; node < total; ++node) {
        if (exists[node] and not emitted[node]) {
          next = node;
          break;
        }
      }
    }
    TENZIR_ASSERT(next);
    const auto node = *next;
    // Collect the lanes feeding this node, left to right.
    auto legs = std::vector<Leg>{};
    if (const auto last = lanes.last_used()) {
      for (auto lane = size_t{0}; lane <= *last; ++lane) {
        if (const auto l = lanes.get(lane); l.used and l.target == node) {
          legs.push_back({lane, l.kind});
        }
      }
    }
    auto node_lane = size_t{};
    if (legs.size() > 1) {
      out += fan_line(lanes, legs, false);
      for (const auto& leg : std::span{legs}.subspan(1)) {
        lanes[leg.lane] = Lane{};
      }
      node_lane = legs.front().lane;
    } else {
      if (not first and not connector) {
        out += plain_line(lanes);
      }
      node_lane = legs.empty() ? lanes.allocate(0) : legs.front().lane;
    }
    if (not legs.empty()) {
      lanes[node_lane] = Lane{};
    }
    out += node_line(lanes, node_lane, label(node));
    // Open one lane per outgoing channel; the first port keeps the node's lane.
    connector = false;
    const auto& edges = out_edges[node];
    if (not edges.empty()) {
      auto next_legs = std::vector<Leg>{};
      lanes[node_lane] = Lane{true, edges.front().to, edges.front().kind};
      ++pending_in[edges.front().to];
      next_legs.push_back({node_lane, edges.front().kind});
      for (const auto& edge : std::span{edges}.subspan(1)) {
        const auto lane = lanes.allocate(node_lane + 1);
        lanes[lane] = Lane{true, edge.to, edge.kind};
        ++pending_in[edge.to];
        next_legs.push_back({lane, edge.kind});
      }
      if (next_legs.size() > 1) {
        out += fan_line(lanes, next_legs, true);
        connector = true;
      }
    }
    emitted[node] = true;
    first = false;
    --remaining;
  }
  return out;
}

} // namespace tenzir
