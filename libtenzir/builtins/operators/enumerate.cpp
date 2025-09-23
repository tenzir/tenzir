//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/parseable/tenzir/option_set.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/detail/escapers.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/error.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>

#include <arrow/type.h>
#include <arrow/type_fwd.h>

namespace tenzir::plugins::enumerate {

namespace {

struct enumerate_args {
  ast::field_path out;
  std::optional<ast::expression> group;

  friend auto inspect(auto& f, enumerate_args& x) -> bool {
    return f.object(x).fields(f.field("out", x.out), f.field("group", x.group));
  }
};

class enumerate_operator final : public crtp_operator<enumerate_operator> {
public:
  enumerate_operator() = default;

  explicit enumerate_operator(enumerate_args args) : args_{std::move(args)} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto& dh = ctrl.diagnostics();
    auto idx = int64_t{};
    auto groups = tsl::robin_map<data, int64_t>{};
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto b = int64_type::make_arrow_builder(arrow::default_memory_pool());
      check(b->Reserve(detail::narrow_cast<int64_t>(slice.rows())));
      if (args_.group) {
        for (const auto& s : eval(args_.group.value(), slice, dh)) {
          for (const auto& val : s.values()) {
            auto it = groups.find(materialize(val));
            match(
              val,
              [&]<typename T>(const T& x)
                requires concepts::integer<T>
              {
                using to_type = std::conditional_t<std::same_as<int64_t, T>,
                                                   uint64_t, int64_t>;
                if (it == groups.end() and std::in_range<to_type>(x)) {
                  it = groups.find(data_view{static_cast<to_type>(x)});
                }
              },
              [](const auto&) {});
            if (it == groups.end()) {
              it = groups.emplace_hint(it, materialize(val), 0);
            }
            check(b->Append(it.value()++));
          }
        }
      } else {
        for (auto i = int64_t{}; i < detail::narrow<int64_t>(slice.rows());
             ++i) {
          check(b->Append(idx++));
        }
      }
      co_yield assign(args_.out, series{int64_type{}, finish(*b)}, slice, dh,
                      assign_position::front);
    }
  }

  auto name() const -> std::string override {
    return "enumerate";
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

private:
  friend auto inspect(auto& f, enumerate_operator& x) -> bool {
    return f.apply(x.args_);
  }

  enumerate_args args_;
};

class plugin final : public operator_plugin2<enumerate_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = enumerate_args{};
    auto out = ast::field_path::try_from(
      ast::root_field{ast::identifier{"#", inv.self.get_location()}});
    TENZIR_ASSERT(out.has_value());
    TRY(argument_parser2::operator_("enumerate")
          .positional("out", out)
          .named("group", args.group, "any")
          .parse(inv, ctx));
    args.out = std::move(out).value();
    return std::make_unique<enumerate_operator>(std::move(args));
  }
};

} // namespace

} // namespace tenzir::plugins::enumerate

TENZIR_REGISTER_PLUGIN(tenzir::plugins::enumerate::plugin)
