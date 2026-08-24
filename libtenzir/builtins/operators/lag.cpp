//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/async.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/set.hpp>

#include <algorithm>
#include <bit>
#include <deque>
#include <span>
#include <vector>

namespace tenzir::plugins::lag {

namespace {

constexpr auto default_offset = uint64_t{1};

struct LagArgs {
  Option<ast::expression> value;
  Option<located<uint64_t>> offset;
  ast::field_path into;

  friend auto inspect(auto& f, LagArgs& x) -> bool {
    return f.object(x).fields(f.field("value", x.value),
                              f.field("offset", x.offset),
                              f.field("into", x.into));
  }
};

template <class Range>
auto length(Range const& parts) -> int64_t {
  auto result = int64_t{0};
  for (auto const& part : parts) {
    result += part.length();
  }
  return result;
}

template <class Range>
auto append_slice(std::vector<series>& output, Range const& parts,
                  int64_t parts_length, int64_t begin, int64_t end) -> void {
  TENZIR_ASSERT(begin >= 0);
  TENZIR_ASSERT(begin <= end);
  TENZIR_ASSERT(end <= parts_length);
  auto offset = int64_t{0};
  for (auto const& part : parts) {
    auto const part_end = offset + part.length();
    if (part_end <= begin) {
      offset = part_end;
      continue;
    }
    if (offset >= end) {
      break;
    }
    auto const local_begin = std::max(begin - offset, int64_t{0});
    auto const local_end = std::min(end - offset, part.length());
    output.push_back(part.slice(local_begin, local_end));
    offset = part_end;
  }
}

auto compact(std::span<const series> parts) -> std::vector<series> {
  auto result = std::vector<series>{};
  result.reserve(parts.size());
  for (auto const& part : parts) {
    auto builder = part.type.make_arrow_builder(arrow_memory_pool());
    check(append_array(*builder, part.type, *part.array));
    result.emplace_back(part.type, finish(*builder));
  }
  return result;
}

auto size_class(uint64_t size) -> unsigned {
  TENZIR_ASSERT(size > 0);
  return std::bit_width(size) - 1;
}

class Lag final : public Operator<table_slice, table_slice> {
public:
  explicit Lag(LagArgs args)
    : value_{args.value ? std::move(*args.value)
                        : ast::expression{ast::this_{}}},
      offset_{args.offset ? args.offset->inner : default_offset},
      into_{std::move(args.into)} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto current = eval(value_, input, ctx);
    auto const current_length = detail::narrow<uint64_t>(current.length());
    TENZIR_ASSERT(history_length_ <= offset_);
    auto const missing = std::min(current_length, offset_ - history_length_);
    auto lagged = std::vector<series>{};
    auto null_parts = std::vector<series>{};
    append_slice(null_parts, current.parts(), current.length(), 0,
                 detail::narrow<int64_t>(missing));
    for (auto const& part : null_parts) {
      lagged.push_back(series::null(part.type, part.length()));
    }
    auto const available = current_length - missing;
    auto const from_history = std::min(history_length_, available);
    append_slice(lagged, history_, detail::narrow<int64_t>(history_length_), 0,
                 detail::narrow<int64_t>(from_history));
    append_slice(lagged, current.parts(), current.length(), 0,
                 detail::narrow<int64_t>(available - from_history));
    update_history(current);
    auto begin = size_t{0};
    for (auto& part : lagged) {
      auto const end = begin + detail::narrow<size_t>(part.length());
      co_await push(
        assign(into_, std::move(part), subslice(input, begin, end), ctx));
      begin = end;
    }
    TENZIR_ASSERT(begin == input.rows());
  }

  auto snapshot(Serde& serde) -> void override {
    serde("history", history_);
    if (serde.is_loading()) {
      history_length_ = detail::narrow<uint64_t>(length(history_));
    }
  }

private:
  auto append_history(series part) -> void {
    if (part.length() == 0) {
      return;
    }
    history_length_ += detail::narrow<uint64_t>(part.length());
    history_.push_back(std::move(part));
    // Maintain descending power-of-two size classes within each homogeneous
    // run. Merge an entire comparable suffix in one pass so one append never
    // rebuilds the growing result repeatedly.
    auto first = history_.size() - 1;
    auto combined_length = detail::narrow<uint64_t>(history_.back().length());
    while (first > 0) {
      auto const& previous = history_[first - 1];
      if (previous.type != history_.back().type
          or size_class(detail::narrow<uint64_t>(previous.length()))
               > size_class(combined_length)) {
        break;
      }
      combined_length += detail::narrow<uint64_t>(previous.length());
      --first;
    }
    if (first == history_.size() - 1) {
      return;
    }
    auto type = history_.back().type;
    auto builder = type.make_arrow_builder(arrow_memory_pool());
    check(builder->Reserve(detail::narrow<int64_t>(combined_length)));
    for (auto i = first; i < history_.size(); ++i) {
      check(append_array(*builder, type, *history_[i].array));
    }
    history_.erase(history_.begin() + detail::narrow<ptrdiff_t>(first),
                   history_.end());
    history_.emplace_back(std::move(type), finish(*builder));
  }

  auto trim_history() -> void {
    if (history_length_ <= offset_) {
      return;
    }
    auto excess = history_length_ - offset_;
    while (excess > 0) {
      auto const front_length
        = detail::narrow<uint64_t>(history_.front().length());
      if (front_length <= excess) {
        history_.pop_front();
        history_length_ -= front_length;
        excess -= front_length;
        continue;
      }
      history_.front() = history_.front().slice(detail::narrow<int64_t>(excess),
                                                history_.front().length());
      history_length_ -= excess;
      excess = 0;
    }
  }

  // Copy only the retained boundary into compact Arrow arrays. Keeping slices
  // here would pin the full buffers of otherwise-consumed input batches.
  auto update_history(multi_series const& current) -> void {
    auto const current_length = detail::narrow<uint64_t>(current.length());
    if (current_length == 0) {
      return;
    }
    if (current_length >= offset_) {
      auto tail = std::vector<series>{};
      append_slice(tail, current.parts(), current.length(),
                   detail::narrow<int64_t>(current_length - offset_),
                   detail::narrow<int64_t>(current_length));
      history_.clear();
      history_length_ = 0;
      for (auto& part : compact(tail)) {
        append_history(std::move(part));
      }
      return;
    }
    for (auto& part : compact(current.parts())) {
      append_history(std::move(part));
    }
    trim_history();
  }

  ast::expression value_;
  uint64_t offset_;
  ast::field_path into_;
  std::deque<series> history_;
  uint64_t history_length_ = 0;
};

class Plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "lag";
  }

  auto describe() const -> Description override {
    auto d = Describer<LagArgs, Lag>{};
    d.named("value", &LagArgs::value, "any");
    auto offset = d.named("offset", &LagArgs::offset);
    d.named("into", &LagArgs::into);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      if (auto value = ctx.get(offset); value and value->inner == 0) {
        diagnostic::error("`offset` must be greater than zero")
          .primary(*value)
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::lag

TENZIR_REGISTER_PLUGIN(tenzir::plugins::lag::Plugin)
