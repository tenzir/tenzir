//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ir.hpp"

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace tenzir {

namespace {

/// Whether a channel of this kind is drawn inline inside a chain (as opposed
/// to being listed in the `links:` section).
auto is_inline_kind(ir::ChannelKind kind) -> bool {
  switch (kind) {
    case ir::ChannelKind::Direct:
    case ir::ChannelKind::DirectFused:
    case ir::ChannelKind::Bytes:
    case ir::ChannelKind::Scatter:
    case ir::ChannelKind::Gather:
    case ir::ChannelKind::Shuffle:
      return true;
    case ir::ChannelKind::Broadcast:
    case ir::ChannelKind::Split:
    case ir::ChannelKind::GatherSignals:
    case ir::ChannelKind::BroadcastSignals:
      return false;
  }
  return false;
}

/// The inline glyph representing a channel kind inside a chain.
auto inline_glyph(ir::ChannelKind kind) -> std::string_view {
  switch (kind) {
    case ir::ChannelKind::Direct:
      return "--";
    case ir::ChannelKind::DirectFused:
      return "-";
    case ir::ChannelKind::Bytes:
      return "-b-";
    case ir::ChannelKind::Scatter:
      return "-<";
    case ir::ChannelKind::Gather:
      return ">-";
    case ir::ChannelKind::Shuffle:
      return "-x-";
    case ir::ChannelKind::Broadcast:
    case ir::ChannelKind::Split:
    case ir::ChannelKind::GatherSignals:
    case ir::ChannelKind::BroadcastSignals:
      return "--";
  }
  return "--";
}

/// The name used for a channel kind in the `links:` section.
auto link_kind_name(ir::ChannelKind kind) -> std::string_view {
  switch (kind) {
    case ir::ChannelKind::Direct:
      return "direct";
    case ir::ChannelKind::DirectFused:
      return "direct-fused";
    case ir::ChannelKind::Bytes:
      return "bytes";
    case ir::ChannelKind::Scatter:
      return "scatter";
    case ir::ChannelKind::Gather:
      return "gather";
    case ir::ChannelKind::Shuffle:
      return "shuffle";
    case ir::ChannelKind::Broadcast:
      return "broadcast";
    case ir::ChannelKind::Split:
      return "split";
    case ir::ChannelKind::GatherSignals:
      return "gather-signals";
    case ir::ChannelKind::BroadcastSignals:
      return "broadcast-signals";
  }
  return "link";
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
  if (not node.partition_keys.empty()) {
    annotations.emplace_back("keyed");
  }
  if (not annotations.empty()) {
    label += fmt::format("({})", fmt::join(annotations, ", "));
  }
  return label;
}

} // namespace

auto ir::fmt_ir_plan(const ir::Plan& plan) -> std::string {
  const auto n = plan.operators.size();
  const auto is_real = [&](size_t x) {
    return x < n;
  };
  // Inline adjacency between real nodes, plus the external input/output
  // markers attached to individual nodes.
  struct edge {
    size_t node;
    ir::ChannelKind kind;
  };
  auto out_inline = std::vector<std::vector<edge>>(n);
  auto in_inline = std::vector<std::vector<edge>>(n);
  auto input_glyph = std::vector<std::optional<ir::ChannelKind>>(n);
  auto output_glyph = std::vector<std::optional<ir::ChannelKind>>(n);
  auto link_channels = std::vector<const ir::PlannedChannel*>{};
  for (const auto& c : plan.channels) {
    const auto drawable
      = is_inline_kind(c.kind) and c.from.size() == 1 and c.to.size() == 1;
    if (not drawable) {
      link_channels.push_back(&c);
      continue;
    }
    const auto f = c.from.front();
    const auto t = c.to.front();
    if (f == ir::PlanPort::input and is_real(t)) {
      input_glyph[t] = c.kind;
    } else if (is_real(f) and t == ir::PlanPort::output) {
      output_glyph[f] = c.kind;
    } else if (is_real(f) and is_real(t)) {
      out_inline[f].push_back({t, c.kind});
      in_inline[t].push_back({f, c.kind});
    } else {
      link_channels.push_back(&c);
    }
  }
  // A node starts a new chain unless it has exactly one inline predecessor
  // that in turn has exactly one inline successor.
  auto is_head = std::vector<bool>(n, true);
  for (auto u = size_t{0}; u < n; ++u) {
    if (in_inline[u].size() == 1
        and out_inline[in_inline[u].front().node].size() == 1) {
      is_head[u] = false;
    }
  }
  // Build maximal chains starting at each head.
  auto chains = std::vector<std::vector<size_t>>{};
  for (auto u = size_t{0}; u < n; ++u) {
    if (not is_head[u]) {
      continue;
    }
    auto chain = std::vector<size_t>{u};
    auto cur = u;
    while (out_inline[cur].size() == 1) {
      const auto next = out_inline[cur].front().node;
      if (is_head[next]) {
        break;
      }
      chain.push_back(next);
      cur = next;
    }
    chains.push_back(std::move(chain));
  }
  // Deterministic chain order by head node index.
  std::ranges::sort(chains, [](const auto& a, const auto& b) {
    return a.front() < b.front();
  });
  // Map each real node to its (chain, position) coordinate. Position counts
  // the leading `input` marker so it matches the printed token order.
  struct coord {
    size_t chain;
    size_t pos;
  };
  auto coords = std::vector<coord>(n);
  for (auto ci = size_t{0}; ci < chains.size(); ++ci) {
    const auto& chain = chains[ci];
    const auto prefix = input_glyph[chain.front()].has_value() ? size_t{1} : 0;
    for (auto i = size_t{0}; i < chain.size(); ++i) {
      coords[chain[i]] = coord{ci, prefix + i};
    }
  }
  auto out = std::string{};
  // Print chains.
  for (auto ci = size_t{0}; ci < chains.size(); ++ci) {
    const auto& chain = chains[ci];
    out += fmt::format("c{}:", ci);
    auto first = true;
    const auto emit = [&](std::string_view glyph, std::string_view tok) {
      if (first) {
        out += fmt::format(" {}", tok);
        first = false;
      } else {
        out += fmt::format(" {} {}", glyph, tok);
      }
    };
    if (const auto g = input_glyph[chain.front()]) {
      emit({}, "{input}");
      emit(inline_glyph(*g), plan_node_label(plan.operators[chain.front()]));
    } else {
      emit({}, plan_node_label(plan.operators[chain.front()]));
    }
    for (auto i = size_t{1}; i < chain.size(); ++i) {
      emit(inline_glyph(out_inline[chain[i - 1]].front().kind),
           plan_node_label(plan.operators[chain[i]]));
    }
    if (const auto g = output_glyph[chain.back()]) {
      emit(inline_glyph(*g), "{output}");
    }
    out += '\n';
  }
  // Print links.
  if (not link_channels.empty()) {
    const auto ref = [&](size_t x) -> std::string {
      if (x == ir::PlanPort::input) {
        return "{input}";
      }
      if (x == ir::PlanPort::output) {
        return "{output}";
      }
      return fmt::format("c{}", coords[x].chain);
    };
    const auto join = [&](const std::vector<size_t>& nodes) -> std::string {
      auto s = std::string{};
      for (const auto x : nodes) {
        if (not s.empty()) {
          s += ' ';
        }
        s += ref(x);
      }
      return s;
    };
    auto rows = std::vector<std::string>{};
    rows.reserve(link_channels.size());
    for (const auto* c : link_channels) {
      rows.push_back(fmt::format("  {} --{}-> {}", join(c->from),
                                 link_kind_name(c->kind), join(c->to)));
    }
    std::ranges::sort(rows);
    out += "\nlinks:\n";
    for (const auto& row : rows) {
      out += row;
      out += '\n';
    }
  }
  return out;
}

} // namespace tenzir
