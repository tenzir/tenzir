//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/detail/assert.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::assertion_test_internal {

namespace {

class assertion_test_operator final : public crtp_operator<assertion_test_operator> {

public:
  assertion_test_operator() = default;

  static inline auto l = [](std::string, std::vector<int>) {
    TENZIR_ASSERT(false);
  };

  auto
  operator()(generator<table_slice>, operator_control_plane&) const
    -> generator<table_slice> {
    auto i = 42;
    [&i](){ l({},{i}); }();
    co_return;
  }

  auto name() const -> std::string override {
    return "_assertion_test_internal";
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return optimize_result{std::nullopt, event_order::unordered, copy()};
  }

private:
  friend auto inspect(auto& f, assertion_test_operator& x) -> bool {
    return f.object(x).fields();
  }
};

class plugin final : public virtual operator_plugin2<assertion_test_operator> {
public:
  auto make(invocation, session) const
    -> failure_or<operator_ptr> override {
    return std::make_unique<assertion_test_operator>();
  }
};

} // namespace

} // namespace tenzir::plugins::enumerate

TENZIR_REGISTER_PLUGIN(tenzir::plugins::assertion_test_internal::plugin)
