//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/builtins/join/common.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/ir_lift.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/ast.hpp>

#include <caf/binary_serializer.hpp>

#include <set>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace tenzir::plugins::join {

namespace {

auto build_field_path_expr(std::span<ast::field_path::segment const> segments)
  -> ast::expression {
  TENZIR_ASSERT(not segments.empty());
  auto expr = ast::expression{ast::root_field{
    segments[0].id,
    segments[0].has_question_mark,
  }};
  for (auto i = size_t{1}; i < segments.size(); ++i) {
    auto const loc = segments[i].id.location;
    expr = ast::expression{ast::field_access{
      std::move(expr),
      loc,
      segments[i].has_question_mark,
      segments[i].id,
    }};
  }
  return expr;
}

auto strip_field_path_prefix(ast::field_path const& fp, size_t prefix_len)
  -> ast::field_path {
  TENZIR_ASSERT_LT(prefix_len, fp.path().size());
  auto segments = std::vector<ast::field_path::segment>{
    fp.path().begin() + detail::narrow<ptrdiff_t>(prefix_len),
    fp.path().end(),
  };
  auto expr = build_field_path_expr(segments);
  return ast::field_path::make(std::move(expr), false, std::move(segments));
}

auto replace_field_path_prefix(ast::field_path const& fp, size_t prefix_len,
                               ast::field_path const& replacement)
  -> ast::expression {
  TENZIR_ASSERT_GT(prefix_len, 0u);
  TENZIR_ASSERT_LEQ(prefix_len, fp.path().size());
  TENZIR_ASSERT(not replacement.path().empty());
  auto segments = std::vector<ast::field_path::segment>{
    replacement.path().begin(),
    replacement.path().end(),
  };
  segments.back().has_question_mark
    = fp.path()[prefix_len - 1].has_question_mark;
  segments.insert(segments.end(),
                  fp.path().begin() + detail::narrow<ptrdiff_t>(prefix_len),
                  fp.path().end());
  return build_field_path_expr(segments);
}

auto has_prefix_segments(ast::field_path const& fp,
                         std::span<std::string_view const> prefix) -> bool {
  if (fp.path().size() < prefix.size()) {
    return false;
  }
  for (auto i = size_t{0}; i < prefix.size(); ++i) {
    if (fp.path()[i].id.name != prefix[i]) {
      return false;
    }
  }
  return true;
}

auto is_prefix_of_segments(ast::field_path const& fp,
                           std::span<std::string_view const> target) -> bool {
  if (fp.path().size() > target.size()) {
    return false;
  }
  for (auto i = size_t{0}; i < fp.path().size(); ++i) {
    if (fp.path()[i].id.name != target[i]) {
      return false;
    }
  }
  return true;
}

auto make_prefix_stripper(std::vector<std::string_view> prefix)
  -> ast::FieldSubstitution {
  return [prefix = std::move(prefix)](
           ast::field_path const& fp) -> Option<ast::expression> {
    if (not has_prefix_segments(fp, prefix)) {
      return None{};
    }
    if (fp.path().size() == prefix.size()) {
      return ast::expression{ast::this_{fp.get_location()}};
    }
    return strip_field_path_prefix(fp, prefix.size()).unwrap();
  };
}

/// Extracts the sub-projection under `prefix` from a downstream projection.
/// Returns `None` if downstream needs all fields or references `prefix` itself
/// (or a parent of `prefix`).
auto extract_sub_projection(Option<ir::OptimizeProjection> const& proj,
                            std::span<std::string_view const> prefix)
  -> Option<ir::OptimizeProjection> {
  if (not proj) {
    return None{};
  }
  auto result = Option<ir::OptimizeProjection>{ir::OptimizeProjection{}};
  for (auto const& fp : *proj) {
    if (is_prefix_of_segments(fp, prefix)) {
      return None{};
    }
    if (has_prefix_segments(fp, prefix)) {
      ir::add_to_projection(result, strip_field_path_prefix(fp, prefix.size()));
    }
  }
  return result;
}

auto is_plain_field_path(ast::expression const& expr)
  -> Option<ast::field_path> {
  auto fp = ast::field_path::try_from(expr);
  if (not fp or fp->path().empty()) {
    return None{};
  }
  for (auto const& seg : fp->path()) {
    if (seg.has_question_mark) {
      return None{};
    }
  }
  return fp;
}

auto is_equal_congruent_constant(ast::expression const& expr) -> bool {
  return match(
    *expr.kind,
    [](ast::constant const&) {
      return true;
    },
    [](ast::unary_expr const& un) {
      return (un.op == ast::unary_op::pos or un.op == ast::unary_op::neg)
             and is_equal_congruent_constant(un.expr);
    },
    [](ast::list const& list) {
      return std::ranges::all_of(list.items, [](ast::list::item const& item) {
        auto const* e = try_as<ast::expression>(item);
        return e and is_equal_congruent_constant(*e);
      });
    },
    [](ast::record const& rec) {
      return std::ranges::all_of(rec.items, [](ast::record::item const& item) {
        auto const* f = try_as<ast::record::field>(item);
        return f and is_equal_congruent_constant(f->expr);
      });
    },
    [](auto const&) {
      return false;
    });
}

auto is_equal_congruent_operand(ast::expression const& expr) -> bool {
  if (auto fp = ast::field_path::try_from(expr);
      fp and not fp->path().empty()) {
    return true;
  }
  return is_equal_congruent_constant(expr);
}

/// Returns true only when `expr` is invariant under `nova::equal` substitution
/// of its referenced field paths (comparisons and `in` over field paths and
/// constants, plus logical connectives). Rejects expressions such as
/// `f"{id}" == "1"` or type-sensitive calls that distinguish equal values like
/// `1` and `1.0`.
auto is_equal_congruent_predicate(ast::expression const& expr) -> bool {
  if (auto fp = ast::field_path::try_from(expr);
      fp and not fp->path().empty()) {
    return true;
  }
  return match(
    *expr.kind,
    [](ast::unary_expr const& un) {
      return un.op == ast::unary_op::not_
             and is_equal_congruent_predicate(un.expr);
    },
    [](ast::binary_expr const& bin) {
      switch (bin.op) {
        case ast::binary_op::and_:
        case ast::binary_op::or_:
          return is_equal_congruent_predicate(bin.left)
                 and is_equal_congruent_predicate(bin.right);
        case ast::binary_op::eq:
        case ast::binary_op::neq:
        case ast::binary_op::lt:
        case ast::binary_op::leq:
        case ast::binary_op::gt:
        case ast::binary_op::geq:
        case ast::binary_op::in:
          return is_equal_congruent_operand(bin.left)
                 and is_equal_congruent_operand(bin.right);
        case ast::binary_op::add:
        case ast::binary_op::sub:
        case ast::binary_op::mul:
        case ast::binary_op::div:
        case ast::binary_op::if_:
        case ast::binary_op::else_:
          return false;
      }
      TENZIR_UNREACHABLE();
    },
    [](auto const&) {
      return false;
    });
}

auto serialize_expression(ast::expression const& expr) -> caf::byte_buffer {
  auto buf = caf::byte_buffer{};
  auto sink = caf::binary_serializer{buf};
  auto ok = sink.apply(const_cast<ast::expression&>(expr));
  TENZIR_ASSERT(ok);
  return buf;
}

auto describe_join() -> Description {
  auto d = Describer<JoinArgs>{};
  d.name("join");
  d.operator_location(&JoinArgs::operator_location);
  auto keys_arg
    = d.optional_variadic("key", &JoinArgs::keys, "expr|left==right");
  auto kind_arg = d.named("kind", &JoinArgs::kind, "string");
  auto into_arg = d.named("into", &JoinArgs::into);
  auto max_memory_arg = d.named("max_memory", &JoinArgs::max_memory);
  auto refresh_arg = d.named("refresh", &JoinArgs::refresh);
  auto before_arg = d.named("before", &JoinArgs::before);
  auto after_arg = d.named("after", &JoinArgs::after);
  auto within_arg = d.named("within", &JoinArgs::within);
  auto on_arg = d.named("on", &JoinArgs::on, "expr|left==right");
  auto tolerance_arg = d.named("tolerance", &JoinArgs::tolerance);
  auto idle_timeout_arg = d.named("idle_timeout", &JoinArgs::idle_timeout);
  auto pick_arg = d.named("pick", &JoinArgs::pick, "string");
  // `JoinIr::substitute` instantiates `right_pipe` inline for finite and
  // streaming joins, while deferring instantiation for `refresh=` so each
  // periodic snapshot evaluates expressions like `now()` freshly.
  d.pipeline(SubOptimize::off);
  d.validate([=](DescribeCtx& ctx) -> Empty {
    auto keys = ctx.get_all(keys_arg);
    if (keys.empty()) {
      diagnostic::error("missing join key")
        .primary(ctx.operator_location())
        .hint("specify one or more key expressions, e.g. `join id { ... }` "
              "or `join left_id == right_id { ... }`")
        .emit(ctx);
    } else {
      for (auto& key : keys) {
        if (key) {
          std::ignore = classify_join_key(std::move(*key), ctx.reg(), ctx);
        }
      }
    }
    auto parsed_kind = JoinKind::inner;
    if (auto kind_val = ctx.get(kind_arg)) {
      if (auto parsed = parse_kind(kind_val->inner)) {
        parsed_kind = *parsed;
      } else {
        diagnostic::error(
          "`kind` must be one of `\"inner\"`, `\"left\"`, `\"semi\"`, or "
          "`\"anti\"`")
          .primary(*kind_val)
          .emit(ctx);
      }
    }
    if (auto into_val = ctx.get(into_arg)) {
      if (parsed_kind == JoinKind::semi or parsed_kind == JoinKind::anti) {
        diagnostic::error("`into` cannot be used with `kind=\"{}\"`",
                          kind_name(parsed_kind))
          .primary(into_val->get_location())
          .emit(ctx);
      } else if (into_val->path().empty()) {
        diagnostic::error("`into` must be a field path, not `this`")
          .primary(into_val->get_location())
          .emit(ctx);
      }
    }
    if (auto mem = ctx.get(max_memory_arg); mem and mem->inner == 0) {
      diagnostic::error("`max_memory` must be greater than zero")
        .primary(*mem)
        .emit(ctx);
    }
    auto const refresh_val = ctx.get(refresh_arg);
    auto const before_val = ctx.get(before_arg);
    auto const after_val = ctx.get(after_arg);
    auto const within_val = ctx.get(within_arg);
    auto const on_val = ctx.get(on_arg);
    auto const tolerance_val = ctx.get(tolerance_arg);
    auto const idle_timeout_val = ctx.get(idle_timeout_arg);
    auto const pick_val = ctx.get(pick_arg);

    if (refresh_val and refresh_val->inner <= duration::zero()) {
      diagnostic::error("`refresh` must be positive")
        .primary(*refresh_val)
        .emit(ctx);
    }
    if (before_val and before_val->inner < duration::zero()) {
      diagnostic::error("`before` cannot be negative")
        .primary(*before_val)
        .emit(ctx);
    }
    if (after_val and after_val->inner < duration::zero()) {
      diagnostic::error("`after` cannot be negative")
        .primary(*after_val)
        .emit(ctx);
    }
    if (within_val and within_val->inner < duration::zero()) {
      diagnostic::error("`within` cannot be negative")
        .primary(*within_val)
        .emit(ctx);
    }
    if (tolerance_val and tolerance_val->inner < duration::zero()) {
      diagnostic::error("`tolerance` cannot be negative")
        .primary(*tolerance_val)
        .emit(ctx);
    }
    if (idle_timeout_val and idle_timeout_val->inner <= duration::zero()) {
      diagnostic::error("`idle_timeout` must be positive")
        .primary(*idle_timeout_val)
        .emit(ctx);
    }
    if (within_val and (before_val or after_val)) {
      diagnostic::error("`within` cannot be combined with `before` or `after`")
        .primary(*within_val)
        .hint("`within=D` is shorthand for `before=D, after=D`")
        .emit(ctx);
    }
    auto const has_time_bound = before_val.has_value() or after_val.has_value()
                                or within_val.has_value();
    if (refresh_val) {
      auto reject_with_refresh = [&](std::string_view opt, location loc) {
        diagnostic::error("`refresh` cannot be combined with `{}`", opt)
          .primary(loc)
          .secondary(refresh_val->source, "`refresh` specified here")
          .note("`refresh` selects periodic snapshot mode, but `{}` only "
                "applies to streaming windowed mode",
                opt)
          .emit(ctx);
      };
      if (before_val) {
        reject_with_refresh("before", before_val->source);
      }
      if (after_val) {
        reject_with_refresh("after", after_val->source);
      }
      if (within_val) {
        reject_with_refresh("within", within_val->source);
      }
      if (on_val) {
        reject_with_refresh("on", on_val->get_location());
      }
      if (tolerance_val) {
        reject_with_refresh("tolerance", tolerance_val->source);
      }
      if (idle_timeout_val) {
        reject_with_refresh("idle_timeout", idle_timeout_val->source);
      }
      if (pick_val) {
        reject_with_refresh("pick", pick_val->source);
      }
    } else if (not has_time_bound) {
      if (on_val) {
        diagnostic::error("`on` requires `before`, `after`, or `within`")
          .primary(on_val->get_location())
          .emit(ctx);
      }
      if (tolerance_val) {
        diagnostic::error("`tolerance` requires `on` and a time bound "
                          "(`before`, `after`, or `within`)")
          .primary(*tolerance_val)
          .emit(ctx);
      }
      if (idle_timeout_val) {
        diagnostic::error("`idle_timeout` requires `on` and a time bound "
                          "(`before`, `after`, or `within`)")
          .primary(*idle_timeout_val)
          .emit(ctx);
      }
      if (pick_val) {
        diagnostic::error("`pick` requires `before`, `after`, or `within`")
          .primary(*pick_val)
          .emit(ctx);
      }
    } else if (not on_val) {
      if (tolerance_val) {
        diagnostic::error("`tolerance` requires `on`")
          .primary(*tolerance_val)
          .emit(ctx);
      }
      if (idle_timeout_val) {
        diagnostic::error("`idle_timeout` requires `on`")
          .primary(*idle_timeout_val)
          .emit(ctx);
      }
    }
    if (on_val) {
      auto on_copy = *on_val;
      std::ignore = classify_timestamp_expr(std::move(on_copy), ctx.reg(), ctx);
    }
    if (pick_val) {
      if (not parse_pick(pick_val->inner)) {
        diagnostic::error(
          "`pick` must be one of `\"all\"`, `\"latest\"`, `\"earliest\"`, or "
          "`\"nearest\"`")
          .primary(*pick_val)
          .emit(ctx);
      } else if (parsed_kind == JoinKind::semi
                 or parsed_kind == JoinKind::anti) {
        diagnostic::error("`pick` cannot be used with `kind=\"{}\"`",
                          kind_name(parsed_kind))
          .primary(*pick_val)
          .emit(ctx);
      }
    }
    return {};
  });
  return d.only_arguments();
}

using JoinArguments = OperatorArguments<JoinArgs, describe_join>;

class JoinIr final : public ir::Operator {
public:
  JoinIr() = default;

  explicit JoinIr(JoinArguments args,
                  Option<ir::OptimizeProjection> refresh_right_projection
                  = None{})
    : args_{std::move(args)},
      refresh_right_projection_{std::move(refresh_right_projection)} {
  }

  auto name() const -> std::string override {
    return "join_ir";
  }

  auto display_name() const -> std::string override {
    return "join";
  }

  auto main_location() const -> location override {
    return args_.main_location();
  }

  auto copy() const -> Box<ir::Operator> override {
    return JoinIr{args_, refresh_right_projection_};
  }

  auto move() && -> Box<ir::Operator> override {
    return JoinIr{std::move(args_), std::move(refresh_right_projection_)};
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    TRY(args_.substitute(ctx, instantiate));
    if (instantiate and not args_.get().is_refresh()) {
      auto pipe = args_.pipe();
      TENZIR_ASSERT(pipe);
      TRY(pipe->inner.substitute(ctx, true));
    }
    return {};
  }

  auto infer_type(element_type_tag input, base_ctx ctx) const
    -> failure_or<element_type_tag> override {
    if (input.is_not<nova::Events>()) {
      diagnostic::error("operator does not accept {}", input)
        .primary(args_.main_location())
        .docs("https://tenzir.com/docs/reference/operators/join")
        .emit(ctx);
      return failure::promise();
    }
    auto pipe = args_.pipe();
    TENZIR_ASSERT(pipe);
    TRY(auto right_ty, pipe->inner.infer_type(tag_v<void>, ctx));
    if (right_ty.is_not<nova::Events>()) {
      diagnostic::error("`join` subpipeline must be a source producing events")
        .primary(pipe->source)
        .emit(ctx);
      return failure::promise();
    }
    return tag_v<nova::Events>;
  }

  auto optimize(ir::OptimizeRequest req,
                const ir::OptimizeCtx& octx) && -> ir::OptimizeResult override {
    auto pipe = args_.pipe();
    TENZIR_ASSERT(pipe);
    auto args = args_.get();
    auto kind = JoinKind::inner;
    if (args.kind) {
      auto parsed = parse_kind(args.kind->inner);
      TENZIR_ASSERT(parsed);
      kind = *parsed;
    }
    auto pick = PickMode::all;
    if (args.pick) {
      auto parsed = parse_pick(args.pick->inner);
      TENZIR_ASSERT(parsed);
      pick = *parsed;
    }
    auto const is_streaming = args.is_streaming();
    auto const is_refresh = args.is_refresh();
    auto const& into = args.into;
    auto noop_dh = null_diagnostic_handler{};
    auto key_pairs = std::vector<JoinKeyPair>{};
    key_pairs.reserve(args.keys.size());
    for (auto& key_expr : args.keys) {
      auto pair = classify_join_key(std::move(key_expr), octx.reg, noop_dh);
      if (not pair) {
        if (not is_refresh) {
          auto right_opt = std::move(pipe->inner)
                             .optimize(
                               ir::OptimizeRequest{
                                 .filter = {},
                                 .order = EventOrder::ordered,
                               },
                               ir::OptimizeCtx{
                                 .reg = octx.reg,
                                 .can_any_op_reorder = false,
                               });
          pipe->inner = std::move(right_opt.replacement);
          pipe->inner.prepend(std::move(right_opt.filter), octx.reg);
        }
        return std::move(*this).ir::Operator::optimize(std::move(req), octx);
      }
      key_pairs.push_back(std::move(*pair));
    }
    auto ts_pair = Option<JoinKeyPair>{};
    if (args.on) {
      auto parsed
        = classify_timestamp_expr(std::move(*args.on), octx.reg, noop_dh);
      if (parsed) {
        ts_pair = std::move(*parsed);
      }
    }

    auto const no_meta
      = ast::MetaSubstitution{[](ast::meta const&) -> Option<ast::expression> {
          return None{};
        }};
    auto into_segments = std::vector<std::string_view>{};
    if (into) {
      into_segments.reserve(into->path().size());
      for (auto const& seg : into->path()) {
        into_segments.push_back(seg.id.name);
      }
    }
    auto const left_prefix = std::vector<std::string_view>{"left"};
    auto const right_prefix
      = into ? into_segments : std::vector<std::string_view>{"right"};
    auto const left_stripper = make_prefix_stripper(left_prefix);
    auto const right_stripper = make_prefix_stripper(right_prefix);

    auto left_filter = ir::OptimizeFilter{};
    auto direct_right_filter = ir::OptimizeFilter{};
    auto kept_filter = ir::OptimizeFilter{};
    auto any_kept = false;
    auto const into_touched
      = into ? ast::ExprRefs{.field_paths = {*into}} : ast::ExprRefs{};
    auto const is_event_time_streaming = is_streaming and ts_pair.has_value();

    // Inner and left joins may emit several rows for one left row, and inner
    // joins may emit several rows for one right row. A predicate in front of
    // such a leg runs once for all of these rows, so it must yield the same
    // value for each of them. A non-repeatable one, such as `random() < 0.5`,
    // would otherwise keep or drop all copies together instead of each one.
    auto const left_fans_out
      = kind == JoinKind::inner or kind == JoinKind::left;
    for (auto& expr : req.filter) {
      // Event-time streaming joins rely on every left and right event's
      // timestamp to advance `left_watermark_` and `right_watermark_` (driving
      // window eviction, finalization, and early termination). Keep all
      // filters behind the join so filtered rows still advance watermarks;
      // projection pushdown still applies to both legs.
      if (is_event_time_streaming) {
        kept_filter.push_back(std::move(expr));
        any_kept = true;
        continue;
      }
      auto const must_be_infallible_left = any_kept or kind != JoinKind::left;
      auto const repeatable = ir::is_repeatable(expr, octx.reg);
      auto left_candidate = Option<ast::expression>{};
      if (kind == JoinKind::semi or kind == JoinKind::anti) {
        left_candidate = expr;
      } else if (into) {
        if (auto refs = ast::collect_refs(expr);
            refs and not refs->overlaps(into_touched)) {
          left_candidate = expr;
        }
      } else {
        if (auto sub = ast::substitute_refs(expr, octx.reg, left_stripper);
            sub
            and ir::expression_size(*sub) <= ir::max_rewritten_predicate_size) {
          left_candidate = std::move(*sub);
        }
      }
      if (left_candidate and (repeatable or not left_fans_out)) {
        *left_candidate
          = ir::simplify_predicate(std::move(*left_candidate), octx.reg);
        if (not must_be_infallible_left or ir::is_infallible(*left_candidate)) {
          left_filter.push_back(std::move(*left_candidate));
          continue;
        }
      }
      // Direct right-side filter pushdown is only sound when `kind == inner`
      // and `pick == all` (filtering right candidates before
      // `pick="latest"|"earliest"|"nearest"` would change the winner).
      if (kind == JoinKind::inner and pick == PickMode::all and repeatable) {
        if (auto sub
            = ast::substitute_refs(expr, octx.reg, right_stripper, no_meta);
            sub
            and ir::expression_size(*sub) <= ir::max_rewritten_predicate_size) {
          *sub = ir::simplify_predicate(std::move(*sub), octx.reg);
          if (ir::is_infallible(*sub)) {
            direct_right_filter.push_back(std::move(*sub));
            continue;
          }
        }
      }
      kept_filter.push_back(std::move(expr));
      any_kept = true;
    }

    struct PlainKeyPair {
      ast::field_path left;
      ast::field_path right;
    };
    auto plain_keys = std::vector<PlainKeyPair>{};
    for (auto const& kp : key_pairs) {
      auto lfp = is_plain_field_path(kp.left);
      auto rfp = is_plain_field_path(kp.right);
      if (lfp and rfp) {
        plain_keys.push_back(PlainKeyPair{std::move(*lfp), std::move(*rfp)});
      }
    }
    auto right_filter = direct_right_filter;
    if (not plain_keys.empty()) {
      auto make_equiv_subst
        = [&](bool left_to_right) -> ast::FieldSubstitution {
        return [&, left_to_right](
                 ast::field_path const& fp) -> Option<ast::expression> {
          for (auto const& pk : plain_keys) {
            auto const& src = left_to_right ? pk.left : pk.right;
            auto const& dst = left_to_right ? pk.right : pk.left;
            if (ir::is_field_path_prefix(src, fp)) {
              return replace_field_path_prefix(fp, src.path().size(), dst);
            }
          }
          return None{};
        };
      };
      auto seen_right = std::set<caf::byte_buffer>{};
      for (auto const& expr : right_filter) {
        seen_right.insert(serialize_expression(expr));
      }
      // Never filter right rows in an event-time streaming join: even rows
      // whose keys cannot match carry timestamps that advance
      // `right_watermark_` so open left rows finalize and evict promptly.
      if (not is_event_time_streaming) {
        auto const l_to_r = make_equiv_subst(true);
        for (auto const& expr : left_filter) {
          if (not ir::is_infallible(expr)
              or not is_equal_congruent_predicate(expr)) {
            continue;
          }
          if (auto implied
              = ast::substitute_refs(expr, octx.reg, l_to_r, no_meta);
              implied
              and ir::expression_size(*implied)
                    <= ir::max_rewritten_predicate_size) {
            *implied = ir::simplify_predicate(std::move(*implied), octx.reg);
            if (ir::is_infallible(*implied)
                and seen_right.insert(serialize_expression(*implied)).second) {
              right_filter.push_back(std::move(*implied));
            }
          }
        }
      }
      if (kind == JoinKind::inner and not direct_right_filter.empty()) {
        auto seen_left = std::set<caf::byte_buffer>{};
        for (auto const& expr : left_filter) {
          seen_left.insert(serialize_expression(expr));
        }
        auto const r_to_l = make_equiv_subst(false);
        for (auto const& expr : direct_right_filter) {
          if (not ir::is_infallible(expr)
              or not is_equal_congruent_predicate(expr)) {
            continue;
          }
          if (auto implied
              = ast::substitute_refs(expr, octx.reg, r_to_l, no_meta);
              implied
              and ir::expression_size(*implied)
                    <= ir::max_rewritten_predicate_size) {
            *implied = ir::simplify_predicate(std::move(*implied), octx.reg);
            if (ir::is_infallible(*implied)
                and seen_left.insert(serialize_expression(*implied)).second) {
              left_filter.push_back(std::move(*implied));
            }
          }
        }
      }
    }
    left_filter = ir::simplify_filter(std::move(left_filter), octx.reg);
    right_filter = ir::simplify_filter(std::move(right_filter), octx.reg);

    auto out_proj = std::move(req.projection);
    for (auto const& expr : kept_filter) {
      ir::add_refs_to_projection(out_proj, expr);
    }
    auto left_proj = Option<ir::OptimizeProjection>{};
    auto right_proj = Option<ir::OptimizeProjection>{};
    if (kind == JoinKind::semi or kind == JoinKind::anti) {
      left_proj = out_proj;
      right_proj = ir::OptimizeProjection{};
    } else if (into) {
      left_proj = out_proj;
      ir::project_assignment(left_proj, *into);
      right_proj = extract_sub_projection(out_proj, into_segments);
    } else {
      left_proj = extract_sub_projection(out_proj, left_prefix);
      right_proj = extract_sub_projection(out_proj, right_prefix);
    }
    for (auto const& kp : key_pairs) {
      ir::add_refs_to_projection(left_proj, kp.left);
      ir::add_refs_to_projection(right_proj, kp.right);
    }
    if (ts_pair) {
      ir::add_refs_to_projection(left_proj, ts_pair->left);
      ir::add_refs_to_projection(right_proj, ts_pair->right);
    }
    for (auto const& expr : left_filter) {
      ir::add_refs_to_projection(left_proj, expr);
    }
    for (auto const& expr : right_filter) {
      ir::add_refs_to_projection(right_proj, expr);
    }

    auto const left_order = ts_pair ? EventOrder::ordered : req.order;
    auto const right_order
      = ts_pair ? EventOrder::ordered
                : ((kind == JoinKind::semi or kind == JoinKind::anti)
                     ? EventOrder::unordered
                     : req.order);
    auto const left_limit = (kind == JoinKind::left and kept_filter.empty())
                              ? req.limit
                              : Option<uint64_t>{};

    if (is_refresh) {
      // Refresh pipelines are instantiated and optimized fresh on every tick
      // via `ir::make_plan`, keeping `now()` and dynamic sources current. Keep
      // the projection hint so every reload prunes unused right-hand fields.
      pipe->inner.append(std::move(right_filter), octx.reg);
      refresh_right_projection_ = std::move(right_proj);
    } else {
      auto right_octx = ir::OptimizeCtx{
        .reg = octx.reg,
        .can_any_op_reorder = false,
      };
      auto right_opt = std::move(pipe->inner)
                         .optimize(
                           ir::OptimizeRequest{
                             .filter = std::move(right_filter),
                             .order = right_order,
                             .limit = None{},
                             .projection = std::move(right_proj),
                           },
                           right_octx);
      pipe->inner = std::move(right_opt.replacement);
      pipe->inner.prepend(std::move(right_opt.filter), octx.reg);
    }

    auto replacement = std::vector<Box<ir::Operator>>{};
    replacement.push_back(std::move(*this).move());
    for (auto& expr : kept_filter) {
      replacement.push_back(make_where_ir(std::move(expr), octx.reg));
    }
    return {
      .filter = std::move(left_filter),
      .order = left_order,
      .replacement = ir::pipeline{{}, std::move(replacement)},
      .limit = left_limit,
      .projection = std::move(left_proj),
    };
  }

  auto spawn(element_type_tag input, registry const&) const
    -> AnyOperator override {
    TENZIR_ASSERT(input.is<nova::Events>());
    auto pipe = args_.pipe();
    TENZIR_ASSERT(pipe);
    auto args = args_.get();
    if (args.is_refresh()) {
      return make_refresh_join(std::move(args), pipe->inner,
                               refresh_right_projection_);
    }
    if (args.is_streaming()) {
      return make_streaming_join(std::move(args), pipe->inner);
    }
    return make_finite_join(std::move(args), pipe->inner);
  }

  friend auto inspect(auto& f, JoinIr& x) -> bool {
    return f.object(x).fields(f.field("args", x.args_),
                              f.field("refresh_right_projection",
                                      x.refresh_right_projection_));
  }

private:
  JoinArguments args_;
  /// Projection hint for the right-hand subpipeline of a `refresh=` join,
  /// which is planned anew on every reload instead of during `optimize`.
  Option<ir::OptimizeProjection> refresh_right_projection_;
};

} // namespace

class Plugin final : public virtual operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return "join_operator";
  }

  auto operator_name() const -> std::string override {
    return "join";
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::CompileResult> override {
    TRY(auto args, JoinArguments::parse(std::move(inv), ctx));
    return JoinIr{std::move(args)};
  }
};

using join_ir_plugin = inspection_plugin<ir::Operator, JoinIr>;

} // namespace tenzir::plugins::join

TENZIR_REGISTER_PLUGIN(tenzir::plugins::join::Plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::join::join_ir_plugin)
