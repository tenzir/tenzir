//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/argument_parser2.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/collect.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/series.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <arrow/record_batch.h>
#include <arrow/type_fwd.h>

namespace tenzir::plugins::substitute {

namespace {

auto can_compare(const type& x, const type& y) -> bool {
  // TODO: Handle cases like int-uint, int-double...
  return x == y;
}

// TODO: Handle `what=null`
auto sub(const basic_series<record_type>& input, const type& what_type,
         const data& what, const data& with) -> basic_series<record_type> {
  auto fields = collect(input.fields());
  for (auto&& field : fields) {
    if (can_compare(field.data.type, what_type)) {
      auto b = what_type.make_arrow_builder(arrow::default_memory_pool());
      auto begin = int64_t{};
      auto to_be_replaced = false;
      for (const auto& [current, val] :
           detail::enumerate<int64_t>(values(what_type, *field.data.array))) {
        const auto eq = val == what;
        if (eq != to_be_replaced) {
          const auto len = current - begin;
          if (to_be_replaced) {
            if (is<caf::none_t>(with)) {
              check(b->AppendNulls(len));
            } else {
              const auto s = data_to_series(with, len);
              check(append_array(*b, what_type, *s.array));
            }
          } else {
            check(
              append_array_slice(*b, what_type, *field.data.array, begin, len));
          }
          to_be_replaced = eq;
          begin = current;
        }
      }
      const auto len = field.data.length() - begin;
      if (to_be_replaced) {
        if (is<caf::none_t>(with)) {
          check(b->AppendNulls(len));
        } else {
          const auto s = data_to_series(with, len);
          check(append_array(*b, what_type, *s.array));
        }
      } else {
        check(append_array_slice(*b, what_type, *field.data.array, begin, len));
      }
      field.data = series{field.data.type, finish(*b)};
      continue;
    }
    if (is<record_type>(field.data.type)) {
      field.data
        = sub(field.data.as<record_type>().value(), what_type, what, with);
    }
  }
  return make_record_series(fields, *input.array);
}

struct substitute_args {
  std::optional<ast::field_path> path;
  located<data> what;
  located<data> with;

  auto add_to(argument_parser2& p) {
    p.positional("path", path);
    p.named("what", what);
    p.named("with", with);
  }

  auto validate(diagnostic_handler& dh) const -> failure_or<void> {
    const auto what_type = type::infer(what.inner);
    const auto with_type = type::infer(with.inner);
    if (not what_type) {
      diagnostic::error("failed to infer type of `what`").primary(what).emit(dh);
      return failure::promise();
    }
    if (not with_type) {
      diagnostic::error("failed to infer type of `with`").primary(with).emit(dh);
      return failure::promise();
    }
    if (what_type != with_type and what_type->kind().is_not<null_type>()
        and with_type->kind().is_not<null_type>()) {
      diagnostic::error("substitute must have the same type or be `null`")
        .primary(what)
        .primary(with)
        .emit(dh);
      return failure::promise();
    }
    return {};
  }

  friend auto inspect(auto& f, substitute_args& x) -> bool {
    return f.object(x).fields(f.field("path", x.path), f.field("what", x.what),
                              f.field("with", x.with));
  }
};

class substitute_operator final : public crtp_operator<substitute_operator> {
public:
  substitute_operator() = default;

  substitute_operator(substitute_args args) : args_{std::move(args)} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto& _ = ctrl.diagnostics();
    const auto what_type = type::infer(args_.what.inner).value();
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto s = basic_series<record_type>{slice};
      auto subbed = sub(s, what_type, args_.what.inner, args_.with.inner);
      auto fin = series{
        type{
          slice.schema().name(),
          subbed.type,
          collect(slice.schema().attributes()),
        },
        std::move(subbed.array),
      };
      co_yield table_slice{
        arrow::RecordBatch::Make(fin.type.to_arrow_schema(), fin.length(),
                                 as<arrow::StructArray>(*fin.array).fields()),
        std::move(fin.type),
      };
    }
  }

  auto name() const -> std::string override {
    return "substitute";
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  };

  friend auto inspect(auto& f, substitute_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  substitute_args args_;
};

struct substitute : public virtual operator_plugin2<substitute_operator> {
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto p = argument_parser2::operator_(name());
    auto args = substitute_args{};
    args.add_to(p);
    TRY(p.parse(inv, ctx));
    TRY(args.validate(ctx));
    return std::make_unique<substitute_operator>(std::move(args));
  }
};

} // namespace

} // namespace tenzir::plugins::substitute

TENZIR_REGISTER_PLUGIN(tenzir::plugins::substitute::substitute)
