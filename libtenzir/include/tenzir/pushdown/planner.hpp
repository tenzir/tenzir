//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/assert.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/option.hpp"
#include "tenzir/result.hpp"

#include <cstdint>
#include <functional>
#include <span>
#include <string>
#include <string_view>
#include <variant>

/// How a source plans the stages behind it that it takes over.
///
/// Each source has one planner, which plans a prefix of the stages into the
/// request that the source sends, or returns why it cannot. At plan time, it
/// decides from the syntax of the stages alone. At run time, it also has what
/// the source looked up, such as the columns of a table, and it must plan
/// every prefix that it planned before from the syntax alone.
namespace tenzir::pushdown {

template <class Request, class Known = std::monostate>
struct Planner {
  /// The name of the target, such as `DuckDB`.
  std::string target;
  /// Plans `stages`, with what the source looked up at run time, or with
  /// `nullptr` at plan time. Never fails without stages.
  std::function<auto(std::span<ir::QueryStage const>, Known const*)
                  ->Result<Request, std::string>>
    plan;
};

/// Returns the longest prefix of `chain` that `planner` plans from its syntax,
/// with the reason why it does not plan the prefix one stage longer.
template <class Request, class Known>
auto absorb(ir::QueryChain const& chain, Planner<Request, Known> const& planner)
  -> ir::Absorption {
  auto reason = std::string{};
  for (auto n = chain.stages.size(); n > 0; --n) {
    auto planned = planner.plan(std::span{chain.stages}.first(n), nullptr);
    if (planned.is_ok()) {
      return {.prefix = n, .declined = std::move(reason)};
    }
    reason = std::move(planned).unwrap_err();
  }
  return {.prefix = 0, .declined = std::move(reason)};
}

/// Adds the argument `pushdown=bool` to a source, which takes over the stages
/// that the planner from `make(args)` plans. With `pushdown=false`, it takes
/// over none, so that they run in Tenzir with the semantics of TQL. The
/// projection that downstream needs still applies.
template <class Args, class... Impls, class Make>
auto add_pushdown(Describer<Args, Impls...>& d,
                  Option<located<bool>> Args::* flag, Make make) -> void {
  d.named("pushdown", flag);
  d.query_plan([flag, make = std::move(make)](Args const& args,
                                              ir::QueryChain const& chain) {
    if (args.*flag and not(args.*flag)->inner) {
      return ir::Absorption{.prefix = 0, .declined = "`pushdown=false`"};
    }
    return absorb(chain, make(args));
  });
}

/// Plans the stages that the operator at `op` took over with what the source
/// looked up at run time. A failure is an error that suggests
/// `pushdown=false`, since the stages planned from their syntax.
template <class Request, class Known>
auto replan(Planner<Request, Known> const& planner,
            std::span<ir::QueryStage const> stages, Known const& known,
            location op, diagnostic_handler& dh) -> failure_or<Request> {
  auto planned = planner.plan(stages, &known);
  if (planned.is_ok()) {
    return std::move(planned).unwrap();
  }
  TENZIR_ASSERT(planner.plan(stages, nullptr).is_ok(), "{}",
                planned.unwrap_err());
  diagnostic::error("{} cannot compute the stages that the operator took over",
                    planner.target)
    .primary(op)
    .note("{}", planned.unwrap_err())
    .hint("use `pushdown=false` to run them in Tenzir")
    .emit(dh);
  return failure::promise();
}

/// Plans the stages that a source that looks up nothing took over, which
/// its planner planned before from the same syntax.
template <class Request>
auto replan(Planner<Request> const& planner,
            std::span<ir::QueryStage const> stages) -> Request {
  auto planned = planner.plan(stages, nullptr);
  TENZIR_ASSERT(planned.is_ok(), "{}", planned.unwrap_err());
  return std::move(planned).unwrap();
}

/// Returns the limit of a read with `stages` if the last stage is a limit.
inline auto final_limit(std::span<ir::QueryStage const> stages)
  -> Option<uint64_t> {
  if (stages.empty()) {
    return None{};
  }
  auto const* limit = try_as<ir::QueryStage::Limit>(stages.back().kind);
  return limit ? Option<uint64_t>{limit->count} : None{};
}

/// Adds to `d`, the error of a request that `target` rejected, that it
/// computes the `stages` that the operator took over, with the hint to run
/// them in Tenzir. Without stages, returns `d` unchanged.
[[nodiscard]] inline auto
note_absorbed(diagnostic_builder d, std::span<ir::QueryStage const> stages,
              std::string_view target) -> diagnostic_builder {
  if (stages.empty()) {
    return d;
  }
  return std::move(d)
    .note("{} computes the stages that the operator took over with its own "
          "semantics",
          target)
    .hint("use `pushdown=false` to run them in Tenzir");
}

} // namespace tenzir::pushdown
