//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/concepts.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/error.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/set.hpp>

#include <arrow/type.h>
#include <tsl/robin_map.h>

#include <type_traits>
#include <utility>

namespace tenzir::plugins::enumerate {

namespace {

auto default_output_field() -> ast::field_path {
  auto out = ast::field_path::try_from(
    ast::root_field{ast::identifier{"#", location::unknown}});
  TENZIR_ASSERT(out);
  return std::move(*out);
}

struct EnumerateArgs {
  ast::field_path out = default_output_field();
  Option<ast::expression> group;

  friend auto inspect(auto& f, EnumerateArgs& x) -> bool {
    return f.object(x).fields(f.field("out", x.out), f.field("group", x.group));
  }
};

using GroupMap = tsl::robin_map<data, int64_t>;

auto find_group(GroupMap& groups, data_view3 const value)
  -> GroupMap::iterator {
  auto key = materialize(value);
  auto it = groups.find(key);
  match(
    value,
    [&]<class T>(T const& x)
      requires concepts::integer<T>
    {
      using OtherType
        = std::conditional_t<std::same_as<T, int64_t>, uint64_t, int64_t>;
      if (it == groups.end() and std::in_range<OtherType>(x)) {
        it = groups.find(data_view{static_cast<OtherType>(x)});
      }
    },
    [](auto const&) {});
  if (it == groups.end()) {
    it = groups.emplace_hint(it, std::move(key), int64_t{0});
  }
  return it;
}

class enumerate_operator final : public crtp_operator<enumerate_operator> {
public:
  enumerate_operator() = default;

  explicit enumerate_operator(EnumerateArgs args) : args_{std::move(args)} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto& dh = ctrl.diagnostics();
    auto next_id = int64_t{0};
    auto groups = GroupMap{};
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto builder = int64_type::make_arrow_builder(arrow_memory_pool());
      check(builder->Reserve(detail::narrow_cast<int64_t>(slice.rows())));
      if (args_.group) {
        for (auto const& result : eval(*args_.group, slice, dh)) {
          for (auto const& value : result.values()) {
            auto it = find_group(groups, value);
            check(builder->Append(it.value()++));
          }
        }
      } else {
        for (auto i = int64_t{0}; i < detail::narrow<int64_t>(slice.rows());
             ++i) {
          check(builder->Append(next_id++));
        }
      }
      co_yield assign(args_.out, series{int64_type{}, finish(*builder)}, slice,
                      dh, assign_position::front);
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

  EnumerateArgs args_;
};

class Enumerate final : public Operator<table_slice, table_slice> {
public:
  explicit Enumerate(EnumerateArgs args) : args_{std::move(args)} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (input.rows() == 0) {
      co_return;
    }
    auto builder = int64_type::make_arrow_builder(arrow_memory_pool());
    check(builder->Reserve(detail::narrow_cast<int64_t>(input.rows())));
    if (args_.group) {
      for (auto const& result : eval(*args_.group, input, ctx)) {
        for (auto const& value : result.values()) {
          auto it = find_group(groups_, value);
          check(builder->Append(it.value()++));
        }
      }
    } else {
      for (auto i = int64_t{0}; i < detail::narrow<int64_t>(input.rows());
           ++i) {
        check(builder->Append(next_id_++));
      }
    }
    auto output = assign(args_.out, series{int64_type{}, finish(*builder)},
                         input, ctx, assign_position::front);
    co_await push(std::move(output));
  }

  auto snapshot(Serde& serde) -> void override {
    serde("next_id", next_id_);
    serde("groups", groups_);
  }

private:
  EnumerateArgs args_;
  int64_t next_id_ = 0;
  GroupMap groups_;
};

class Plugin final : public virtual operator_plugin2<enumerate_operator>,
                     public virtual OperatorPlugin {
public:
  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = EnumerateArgs{};
    auto out = ast::field_path::try_from(
      ast::root_field{ast::identifier{"#", inv.self.get_location()}});
    TENZIR_ASSERT(out);
    TRY(argument_parser2::operator_("enumerate")
          .positional("out", out)
          .named("group", args.group, "any")
          .parse(inv, ctx));
    args.out = std::move(*out);
    return std::make_unique<enumerate_operator>(std::move(args));
  }

  auto describe() const -> Description override {
    auto d = Describer<EnumerateArgs, Enumerate>{};
    d.optional_positional("out", &EnumerateArgs::out);
    d.named("group", &EnumerateArgs::group, "any");
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::enumerate

TENZIR_REGISTER_PLUGIN(tenzir::plugins::enumerate::Plugin)
