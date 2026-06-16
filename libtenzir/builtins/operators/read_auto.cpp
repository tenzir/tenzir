//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"
#include "tenzir/compile_ctx.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/read_detection.hpp"
#include "tenzir/si_literals.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/variant.hpp"

#include <algorithm>
#include <limits>
#include <optional>
#include <ranges>
#include <string_view>

namespace tenzir::plugins::read_auto {

namespace {

using namespace tenzir::si_literals;

constexpr auto operator_name = std::string_view{"read_auto"};

using detection_state = read_detection_result::result_state;

struct ReadAutoArgs {
  std::string fallback = "none";
  uint64_t max_probe_bytes = 1_Mi;
  location operator_location;
};

struct PluginCandidate {
  ReadOperatorPlugin const* plugin = nullptr;
  read_detection_candidate candidate;
};

auto plugin_detection_candidates() -> const std::vector<PluginCandidate>& {
  static const auto result = [] {
    auto result = std::vector<PluginCandidate>{};
    for (auto const* plugin : plugins::get<ReadOperatorPlugin>()) {
      auto candidates = plugin->read_detection_candidates();
      for (auto& candidate : candidates) {
        result.push_back(PluginCandidate{
          .plugin = plugin,
          .candidate = std::move(candidate),
        });
      }
    }
    return result;
  }();
  return result;
}

template <class Session>
auto parse_reader_pipeline(std::string_view pipeline, Session reader_session)
  -> failure_or<ast::pipeline> {
  if constexpr (requires { tenzir::parse(pipeline, reader_session); }) {
    return tenzir::parse(pipeline, reader_session);
  } else {
    return tenzir::parse(pipeline, location::unknown, reader_session);
  }
}

struct indeterminate {};

using selection_result = variant<size_t, failure, indeterminate>;

constexpr auto fallback_candidate = std::numeric_limits<size_t>::max();

class ReadAuto final : public Operator<chunk_ptr, table_slice> {
public:
  explicit ReadAuto(ReadAutoArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    live_candidates_.assign(plugin_detection_candidates().size(), true);
    co_return;
  }

  auto process(chunk_ptr input, Push<table_slice>&, OpCtx& ctx)
    -> Task<void> override {
    if (selected_) {
      co_await push_to_selected(std::move(input), ctx);
      co_return;
    }
    if (input->size() == 0) {
      co_return;
    }
    seen_bytes_ = true;
    buffered_.push_back(input);
    if (probe_.size() < args_.max_probe_bytes) {
      auto remaining = args_.max_probe_bytes - probe_.size();
      auto bytes = std::string_view{
        reinterpret_cast<const char*>(input->data()),
        std::min<size_t>(input->size(), detail::narrow<size_t>(remaining)),
      };
      probe_.append(bytes);
    }
    auto exhausted = probe_.size() >= args_.max_probe_bytes;
    auto selected = select({.eof = false, .done_probing = exhausted}, ctx);
    if (auto* index = try_as<size_t>(selected)) {
      co_await spawn_selected(*index, ctx);
    }
  }

  auto finalize(Push<table_slice>&, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    if (selected_) {
      co_await close_selected(ctx);
      co_return FinalizeBehavior::continue_;
    }
    if (not seen_bytes_) {
      done_ = true;
      co_return FinalizeBehavior::done;
    }
    auto selected = select({.eof = true, .done_probing = true}, ctx);
    if (auto* index = try_as<size_t>(selected)) {
      co_await spawn_selected(*index, ctx);
      co_await close_selected(ctx);
      co_return FinalizeBehavior::continue_;
    }
    if (is<failure>(selected)) {
      co_return FinalizeBehavior::done;
    }
    done_ = true;
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

  auto process_sub(SubKeyView, table_slice slice, Push<table_slice>& push,
                   OpCtx&) -> Task<void> override {
    co_await push(std::move(slice));
  }

  auto finish_sub(SubKeyView, Push<table_slice>&, OpCtx&)
    -> Task<void> override {
    done_ = true;
    co_return;
  }

  auto finish_sub(SubKeyView key, failure error, Push<table_slice>& push,
                  OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(error);
    co_await finish_sub(key, push, ctx);
  }

private:
  struct SelectionInput {
    bool eof = false;
    bool done_probing = false;
  };

  auto select(SelectionInput input, OpCtx& ctx) -> selection_result {
    auto matches = std::vector<size_t>{};
    auto const& candidates = plugin_detection_candidates();
    TENZIR_ASSERT(live_candidates_.size() == candidates.size());
    for (auto i = size_t{0}; i < candidates.size(); ++i) {
      if (not live_candidates_[i]) {
        continue;
      }
      auto const& plugin_candidate = candidates[i].candidate;
      auto result = plugin_candidate.detect(read_detection_input{
        .bytes = probe_,
        .eof = input.eof,
      });
      switch (result.state) {
        case detection_state::reject:
          live_candidates_[i] = false;
          break;
        case detection_state::need_more:
          break;
        case detection_state::match:
          matches.push_back(i);
          break;
      }
    }
    if (matches.empty()) {
      if (not input.done_probing) {
        return indeterminate{};
      }
      auto result = fallback(input, ctx);
      if (result.is_error()) {
        return failure::promise();
      }
      return *result;
    }
    auto score = [&](size_t index) {
      return candidates[index].candidate.specificity;
    };
    std::ranges::sort(matches, [&](size_t lhs, size_t rhs) {
      return score(lhs) > score(rhs);
    });
    if (args_.fallback != "none"
        and score(matches[0]) == read_detection::specificity::document) {
      if (not input.done_probing) {
        return indeterminate{};
      }
      auto result = fallback(input, ctx);
      if (result.is_error()) {
        return failure::promise();
      }
      return *result;
    }
    if (matches.size() >= 2 and score(matches[0]) == score(matches[1])) {
      diagnostic::error("read_auto detection is ambiguous")
        .primary(operator_primary())
        .note("candidates `{}` and `{}` both matched",
              candidates[matches[0]].candidate.pipeline,
              candidates[matches[1]].candidate.pipeline)
        .emit(ctx);
      return failure::promise();
    }
    return matches[0];
  }

  auto fallback(SelectionInput input, OpCtx& ctx) -> failure_or<size_t> {
    TENZIR_ASSERT(input.done_probing);
    if (args_.fallback == "none") {
      diagnostic::error("read_auto could not detect an input format")
        .primary(operator_primary())
        .emit(ctx);
      return failure::promise();
    }
    auto valid_utf8 = input.eof ? detail::is_valid_utf8(probe_)
                                : detail::is_valid_utf8_prefix(probe_);
    if (args_.fallback == "lines") {
      if (not valid_utf8) {
        diagnostic::error("read_auto fallback `lines` requires UTF-8 input")
          .primary(operator_primary())
          .emit(ctx);
        return failure::promise();
      }
      fallback_pipeline_ = "read_lines";
    } else {
      fallback_pipeline_
        = input.eof and valid_utf8 ? "read_all" : "read_all binary=true";
    }
    return fallback_candidate;
  }

  auto operator_primary() const -> location {
    return args_.operator_location.subloc(0, operator_name.size());
  }

  auto spawn_selected(size_t index, OpCtx& ctx) -> Task<void> {
    selected_ = index;
    auto const& pipeline = selected_pipeline(index);
    auto root = compile_ctx::make_root(base_ctx{ctx});
    auto provider = session_provider::make(ctx.dh());
    auto session = provider.as_session();
    auto ast = parse_reader_pipeline(pipeline, session);
    TENZIR_ASSERT(ast);
    auto pipe = std::move(*ast).compile(root);
    TENZIR_ASSERT(pipe);
    co_await ctx.spawn_sub<chunk_ptr>(int64_t{0}, std::move(*pipe),
                                      DiagnosticBehavior::Unchanged);
    for (auto& chunk : buffered_) {
      co_await push_to_selected(std::move(chunk), ctx);
    }
    buffered_.clear();
  }

  auto selected_pipeline(size_t index) const -> std::string const& {
    if (index == fallback_candidate) {
      return fallback_pipeline_;
    }
    auto const& candidates = plugin_detection_candidates();
    TENZIR_ASSERT(index < candidates.size());
    return candidates[index].candidate.pipeline;
  }

  auto push_to_selected(chunk_ptr chunk, OpCtx& ctx) -> Task<void> {
    auto sub = ctx.get_sub(int64_t{0});
    if (not sub) {
      co_return;
    }
    std::ignore
      = co_await as<SubHandle<chunk_ptr>>(*sub).push(std::move(chunk));
  }

  auto close_selected(OpCtx& ctx) -> Task<void> {
    auto sub = ctx.get_sub(int64_t{0});
    if (not sub) {
      co_return;
    }
    co_await as<SubHandle<chunk_ptr>>(*sub).close();
  }

  auto snapshot(Serde& serde) -> void override {
    TENZIR_UNUSED(serde);
    // `read_auto` cannot checkpoint its internal state until the executor can
    // restore dynamically spawned subpipelines. In particular, restoring the
    // selected reader in the parent without restoring the child reader loses
    // the parser state owned by the child. Keep this intentionally empty
    // instead of restoring incomplete state.
  }

  ReadAutoArgs args_;
  std::vector<bool> live_candidates_;
  std::vector<chunk_ptr> buffered_;
  std::string probe_;
  std::string fallback_pipeline_;
  std::optional<size_t> selected_;
  bool seen_bytes_ = false;
  bool done_ = false;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.read_auto";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadAutoArgs, ReadAuto>{};
    auto fallback = d.named_optional("fallback", &ReadAutoArgs::fallback);
    d.named_optional("max_probe_bytes", &ReadAutoArgs::max_probe_bytes);
    d.operator_location(&ReadAutoArgs::operator_location);
    d.validate([fallback](DescribeCtx& ctx) -> Empty {
      auto value = ctx.get(fallback).value_or("none");
      if (value != "none" and value != "lines" and value != "all") {
        diagnostic::error("`fallback` must be one of `none`, `lines`, or `all`")
          .primary(ctx.get_location(fallback).value_or(location::unknown))
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::read_auto

TENZIR_REGISTER_PLUGIN(tenzir::plugins::read_auto::plugin)
