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

#include <algorithm>
#include <optional>
#include <ranges>
#include <string_view>

namespace tenzir::plugins::read_auto {

namespace {

using namespace tenzir::si_literals;

using detection_state = read_detection_result::result_state;

struct ReadAutoArgs {
  std::string fallback = "none";
  uint64_t max_probe_bytes = 1_Mi;
  location operator_location;
};

struct Candidate {
  std::string format_name;
  std::string pipeline;
  uint64_t specificity = 0;
  std::optional<size_t> plugin_candidate;
  bool live = true;
};

auto plugin_detection_candidates()
  -> const std::vector<read_detection_candidate>& {
  static const auto result = [] {
    auto result = std::vector<read_detection_candidate>{};
    for (auto const* plugin : plugins::get<ReadOperatorPlugin>()) {
      auto candidates = plugin->read_detection_candidates();
      std::move(candidates.begin(), candidates.end(),
                std::back_inserter(result));
    }
    return result;
  }();
  return result;
}

class ReadAuto final : public Operator<chunk_ptr, table_slice> {
public:
  explicit ReadAuto(ReadAutoArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto names = ctx.reg().operator_names();
    auto strip_tql2_prefix = [](std::string_view name) {
      constexpr auto prefix = std::string_view{"tql2."};
      return name.starts_with(prefix) ? name.substr(prefix.size()) : name;
    };
    auto operator_available = [&](std::string_view name) {
      name = strip_tql2_prefix(name);
      return std::ranges::find(names, name) != names.end();
    };
    auto& plugin_candidates = plugin_detection_candidates();
    for (auto index = size_t{0}; index < plugin_candidates.size(); ++index) {
      auto& candidate = plugin_candidates[index];
      candidates_.push_back(Candidate{
        .format_name = candidate.format_name,
        .pipeline = candidate.pipeline,
        .specificity = candidate.specificity,
        .plugin_candidate = index,
        .live = not candidate.format_name.empty()
                and not candidate.operator_name.empty()
                and not candidate.pipeline.empty() and candidate.specificity > 0
                and candidate.detect
                and operator_available(candidate.operator_name),
      });
    }
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
    if (auto selected
        = select({.eof = false, .done_probing = exhausted}, ctx)) {
      co_await spawn_selected(*selected, ctx);
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
    if (auto selected = select({.eof = true, .done_probing = true}, ctx)) {
      co_await spawn_selected(*selected, ctx);
      co_await close_selected(ctx);
      co_return FinalizeBehavior::continue_;
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

  auto select(SelectionInput input, OpCtx& ctx) -> std::optional<size_t> {
    auto any_need_more = false;
    auto matches = std::vector<size_t>{};
    for (auto i = size_t{0}; i < candidates_.size(); ++i) {
      auto& candidate = candidates_[i];
      if (not candidate.live) {
        continue;
      }
      TENZIR_ASSERT(candidate.plugin_candidate);
      auto& plugin_candidate
        = plugin_detection_candidates()[*candidate.plugin_candidate];
      auto result = plugin_candidate.detect(read_detection_input{
        .bytes = probe_,
        .eof = input.eof,
      });
      switch (result.state) {
        case detection_state::reject:
          candidate.live = false;
          break;
        case detection_state::need_more:
          any_need_more = true;
          break;
        case detection_state::match:
          matches.push_back(i);
          break;
      }
    }
    if (matches.empty()) {
      if (not input.done_probing and any_need_more) {
        return std::nullopt;
      }
      return fallback(input, ctx);
    }
    auto score = [&](size_t index) {
      return candidates_[index].specificity;
    };
    std::ranges::sort(matches, [&](size_t lhs, size_t rhs) {
      return score(lhs) > score(rhs);
    });
    if (matches.size() >= 2 and score(matches[0]) == score(matches[1])) {
      diagnostic::error("read_auto detection is ambiguous")
        .primary(args_.operator_location)
        .note("candidates `{}` and `{}` both matched",
              candidates_[matches[0]].format_name,
              candidates_[matches[1]].format_name)
        .emit(ctx);
      return std::nullopt;
    }
    return matches[0];
  }

  auto fallback(SelectionInput input, OpCtx& ctx) -> std::optional<size_t> {
    if (not input.done_probing) {
      return std::nullopt;
    }
    if (args_.fallback == "none") {
      diagnostic::error("read_auto could not detect an input format")
        .primary(args_.operator_location)
        .emit(ctx);
      return std::nullopt;
    }
    auto pipeline = std::string{};
    auto valid_utf8 = input.eof ? detail::is_valid_utf8(probe_)
                                : detail::is_valid_utf8_prefix(probe_);
    if (args_.fallback == "lines") {
      if (not valid_utf8) {
        diagnostic::error("read_auto fallback `lines` requires UTF-8 input")
          .primary(args_.operator_location)
          .emit(ctx);
        return std::nullopt;
      }
      pipeline = "read_lines";
    } else {
      pipeline = valid_utf8 ? "read_all" : "read_all binary=true";
    }
    candidates_.push_back(Candidate{
      .format_name = fmt::format("fallback.{}", args_.fallback),
      .pipeline = std::move(pipeline),
    });
    return candidates_.size() - 1;
  }

  auto spawn_selected(size_t index, OpCtx& ctx) -> Task<void> {
    selected_ = index;
    auto& candidate = candidates_[index];
    auto root = compile_ctx::make_root(base_ctx{ctx});
    auto provider = session_provider::make(ctx.dh());
    auto session = provider.as_session();
    auto ast = parse_pipeline_with_bad_diagnostics(candidate.pipeline, session);
    if (not ast) {
      diagnostic::error("failed to parse read-auto candidate `{}`",
                        candidate.format_name)
        .primary(args_.operator_location)
        .emit(ctx);
      co_return;
    }
    auto pipe = std::move(*ast).compile(root);
    if (not pipe) {
      diagnostic::error("failed to compile read-auto candidate `{}`",
                        candidate.format_name)
        .primary(args_.operator_location)
        .emit(ctx);
      co_return;
    }
    co_await ctx.spawn_sub<chunk_ptr>(int64_t{0}, std::move(*pipe),
                                      DiagnosticBehavior::Unchanged);
    for (auto& chunk : buffered_) {
      co_await push_to_selected(std::move(chunk), ctx);
    }
    buffered_.clear();
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
  std::vector<Candidate> candidates_;
  std::vector<chunk_ptr> buffered_;
  std::string probe_;
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
