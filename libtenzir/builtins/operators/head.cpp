//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>

namespace tenzir::plugins::head {

namespace {

struct HeadArgs {
  uint64_t count = 10;
};

class Head final : public Operator<table_slice, table_slice> {
public:
  explicit Head(HeadArgs args) : remaining_{args.count} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    // TODO: Do we want to guarantee this?
    TENZIR_ASSERT(remaining_ > 0);
    auto result = tenzir::head(input, remaining_);
    TENZIR_ASSERT(result.rows() <= remaining_);
    remaining_ -= result.rows();
    co_await push(std::move(result));
  }

  auto state() -> OperatorState override {
    if (remaining_ == 0) {
      // TODO: We also want to declare that we'll produce no more output and
      // that we are ready to shutdown.
      return OperatorState::done;
    }
    return OperatorState::normal;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("remaining", remaining_);
  }

private:
  uint64_t remaining_;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "head";
  };

  auto describe() const -> Description override {
    auto d = Describer<HeadArgs, Head>{};
    auto count = d.optional_positional("count", &HeadArgs::count);
    return d.optimize(
      [=](DescribeCtx& ctx, ir::OptimizeRequest req) -> Optimization {
        // `head` is a filter barrier that needs ordered input. It stays in the
        // pipeline and emits its count as a limit hint for upstream. Since all
        // incoming predicates stay behind `head`, an incoming limit (which
        // counts events after those predicates) can only be combined with our
        // count if there are no incoming predicates.
        auto limit = Option<uint64_t>{};
        if (ctx.get_location(count)) {
          // The argument is present. If it cannot be evaluated, we emit no hint
          // rather than a wrong one.
          limit = ctx.get(count);
        } else {
          limit = HeadArgs{}.count;
        }
        if (limit and req.limit and req.filter.empty()) {
          limit = std::min(*limit, *req.limit);
        }
        return {
          .order = EventOrder::ordered,
          .filter_self = std::move(req.filter),
          .limit_upstream = limit,
          .projection_upstream = std::move(req.projection),
        };
      });
  }
};

} // namespace

} // namespace tenzir::plugins::head

TENZIR_REGISTER_PLUGIN(tenzir::plugins::head::plugin)
