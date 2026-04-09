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

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
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
    d.optional_positional("count", &HeadArgs::count);
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::head

TENZIR_REGISTER_PLUGIN(tenzir::plugins::head::plugin)
