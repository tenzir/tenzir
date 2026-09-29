//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/storage.hpp"

#include <tenzir/arrow_memory_pool.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/enumerate.hpp>
#include <tenzir/detail/heterogeneous_string_hash.hpp>
#include <tenzir/detail/stable_map.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/series.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/compute/api.h>
#include <boost/process/v2/environment.hpp>
#if __has_include(<boost/process/v1/environment.hpp>)
#  include <boost/process/v1/environment.hpp>
#else
#  include <boost/process/environment.hpp>
#endif

#include "tenzir/nova/array.hpp"
#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/array_merge.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/data_array_builder.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/eval_kernel.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/function_plugin.hpp"
#include "tenzir/nova/type_id.hpp"
#include "tenzir/nova/type_system.hpp"

#include <ranges>

namespace tenzir::plugins::misc {

namespace {

struct TypeIdArgs {
  nova::ValueArgument x;
};

class TypeIdFunction {
public:
  static auto eval(TypeIdArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    return nova::Array<nova::Data>{nova::type_id(args.x.data, frame.mask())};
  }
};

class type_id final : public nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "type_id";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<TypeIdArgs, TypeIdFunction>{};
    d.positional("x", &TypeIdArgs::x, "any");
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function("type_id")
          .positional("x", expr, "any")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> series {
        TENZIR_UNUSED(ctx);
        auto value = eval(expr);
        // TODO: This is a 64-bit hex-encoded hash. We could also use just use
        // an integer for this.
        auto b = arrow::StringBuilder{};
        check(b.Reserve(eval.length()));
        for (auto& part : value.parts()) {
          auto type_id = part.type.make_fingerprint();
          for (auto i = int64_t{0}; i < part.length(); ++i) {
            check(b.Append(type_id));
          }
        }
        return {string_type{}, finish(b)};
      });
  }
};

auto nova_type_definition(std::string_view kind,
                          nova::Data state = nova::Null{}) -> nova::Record {
  // Nova values do not carry schema names or attributes.
  return {{"name", nova::Null{}},
          {"kind", std::string{kind}},
          {"attributes", nova::List{}},
          {"state", std::move(state)}};
}

auto nova_type_definition(nova::RowView<nova::Data> row) -> nova::Record {
  using namespace nova;
  return match(row, []<class Tag>(RowView<Tag> value) -> Record {
    if constexpr (std::same_as<Tag, Record>) {
      auto fields = List{};
      for (auto [name, field] : value) {
        fields.emplace_back(Record{{"name", std::string{name}},
                                   {"type", nova_type_definition(field)}});
      }
      return nova_type_definition(Type<Tag>::static_name,
                                  Record{{"fields", std::move(fields)}});
    } else if constexpr (std::same_as<Tag, List>) {
      // Unlike Arrow lists, Nova lists may be heterogeneous. Keep distinct
      // element types in first-occurrence order.
      auto types = List{};
      for (auto element : value) {
        auto type = nova_type_definition(element);
        if (std::ranges::none_of(types, [&](Data const& existing) {
              return equal(RowView<Data>{existing}, RowView<Record>{type});
            })) {
          types.emplace_back(std::move(type));
        }
      }
      auto element_type = nova_type_definition(Type<Null>::static_name);
      if (types.size() == 1) {
        element_type = as<Record>(std::move(types.front()));
      } else if (not types.empty()) {
        element_type
          = nova_type_definition("union", Record{{"types", std::move(types)}});
      }
      return nova_type_definition(Type<Tag>::static_name,
                                  Record{{"type", std::move(element_type)}});
    } else {
      return nova_type_definition(Type<Tag>::static_name);
    }
  });
}

struct TypeOfArgs {
  nova::ValueArgument x;
};

struct TypeOfFunction {
  static auto eval(TypeOfArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using namespace nova;
    auto describe_rows = [&]() -> Array<Data> {
      auto builder = ArrayBuilder<Data>{};
      for (auto row : storage::bitmap_iteration(frame.mask())) {
        if (row) {
          append_data(builder, nova_type_definition(args.x.data.get(*row)));
        } else {
          builder.skip();
        }
      }
      return builder.finish();
    };
    return match(
      args.x.data,
      [&]<data_type Tag>(Array<Tag> const&) -> Array<Data> {
        if constexpr (fundamental_type<Tag>) {
          return repeat(nova_type_definition(Type<Tag>::static_name),
                        frame.length());
        } else {
          return describe_rows();
        }
      },
      [&](UnionArray const&) {
        return describe_rows();
      });
  }
};

class type_of final : public nova::FunctionPlugin {
public:
  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<TypeOfArgs, TypeOfFunction>{};
    d.positional("x", &TypeOfArgs::x);
    return std::move(d).finish();
  }

  auto name() const -> std::string override {
    return "type_of";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function("type_of")
          .positional("x", expr, "any")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> multi_series {
        TENZIR_UNUSED(ctx);
        return map_series(eval(expr), [](auto&& x) {
          auto builder = series_builder{};
          auto definition = x.type.to_definition();
          for (auto i = int64_t{0}; i < x.length(); ++i) {
            builder.data(definition);
          }
          return builder.finish_assert_one_array();
        });
      });
  }
};

struct EnvArgs {
  nova::ValueArgument key;
  std::shared_ptr<const detail::heterogeneous_string_hashmap<std::string>> env;
};

class EnvFunction {
public:
  static auto eval(EnvArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    auto builder = nova::ArrayBuilder<nova::Data>{};
    auto invalid_type = false;
    for (auto row : nova::storage::bitmap_iteration(frame.mask())) {
      if (not row) {
        builder.skip();
        continue;
      }
      match(args.key.data.get(*row),
            [&]<nova::data_type Tag>(nova::RowView<Tag> x) {
              if constexpr (std::same_as<Tag, nova::String>) {
                auto it = args.env->find(*x);
                if (it == args.env->end()) {
                  builder.null();
                } else {
                  builder.data(it->second);
                }
              } else if constexpr (std::same_as<Tag, nova::Null>) {
                builder.null();
              } else {
                invalid_type = true;
                builder.null();
              }
            });
    }
    if (invalid_type) {
      diagnostic::warning("expected `string`")
        .primary(args.key.source)
        .emit(frame);
    }
    return builder.finish();
  }
};

class env final : public nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "env";
  }

  auto initialize(const record& plugin_config, const record& global_config)
    -> caf::error override {
    TENZIR_UNUSED(plugin_config);
    TENZIR_UNUSED(global_config);
    auto env = detail::heterogeneous_string_hashmap<std::string>{};
    for (const auto& entry : boost::this_process::environment()) {
      env.emplace(entry.get_name(), entry.to_string());
    }
    env_ = std::make_shared<decltype(env)>(std::move(env));
    return {};
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<EnvArgs, EnvFunction>{};
    d.positional("key", &EnvArgs::key, "string");
    d.validate([env = env_](auto& args, diagnostic_handler&) {
      args.env = env;
      return failure_or<void>{};
    });
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function("env")
          .positional("key", expr, "string")
          .parse(inv, ctx));
    if (auto key = try_const_eval(expr, ctx)) {
      auto value = Option<std::string>{};
      const auto* typed_key = try_as<std::string>(key->inner);
      if (not typed_key) {
        diagnostic::warning("expected `string`, got `{}`",
                            type::infer(key->inner).value_or(type{}).kind())
          .primary(expr)
          .emit(ctx);
      } else if (auto it = env_->find(*typed_key); it != env_->end()) {
        value = it->second;
      }
      return function_use::make(
        [value = std::move(value)](evaluator eval, session ctx) -> series {
          TENZIR_UNUSED(ctx);
          if (not value) {
            return series::null(string_type{}, eval.length());
          }
          return {
            string_type{},
            check(arrow::MakeArrayFromScalar(arrow::StringScalar{*value},
                                             eval.length(),
                                             tenzir::arrow_memory_pool())),
          };
        });
    }
    return function_use::make(
      [this, expr = std::move(expr)](evaluator eval, session ctx) -> series {
        auto b = arrow::StringBuilder{};
        check(b.Reserve(eval.length()));
        for (auto& value : eval(expr)) {
          auto f = detail::overload{
            [&](const arrow::StringArray& array) {
              for (auto i = int64_t{0}; i < array.length(); ++i) {
                if (array.IsNull(i)) {
                  check(b.AppendNull());
                  continue;
                }
                const auto it = env_->find(array.GetView(i));
                if (it == env_->end()) {
                  check(b.AppendNull());
                  continue;
                }
                check(b.Append(it->second));
              }
            },
            [&](const arrow::NullArray& array) {
              check(b.AppendNulls(array.length()));
            },
            [&](const auto& array) {
              diagnostic::warning("expected `string`, got `{}`",
                                  value.type.kind())
                .primary(expr)
                .emit(ctx);
              check(b.AppendNulls(array.length()));
            },
          };
          match(*value.array, f);
        }
        return series{string_type{}, finish(b)};
      });
  }

private:
  std::shared_ptr<const detail::heterogeneous_string_hashmap<std::string>> env_
    = std::make_shared<detail::heterogeneous_string_hashmap<std::string>>();
};

struct LengthArgs {
  nova::ValueArgument x;
};

class LengthFunction final {
public:
  static auto eval(LengthArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using namespace nova;
    auto present = frame.mask();
    auto subject = args.x;

    auto compute_lengths = [&](const Array<List>& arr,
                               const storage::BitMap& valid) -> Array<Data> {
      auto mut = Type<Int>::PrimaryPhysicalStorage::Mutable{arr.length()};
      for (auto i = storage::Index{0}; i < arr.length(); ++i) {
        if (valid.get(i)) {
          mut.set(i, static_cast<std::int64_t>(arr.get(i).length()));
        }
      }
      // Rows outside `valid` never got a length written, so they become
      // explicit `Null`s.
      return Array<Data>{Array<Int>{std::move(mut).finish()}}.null_where(
        present.and_not(valid));
    };

    return match(
      subject.data,
      [&](const Array<List>& arr) -> Array<Data> {
        return compute_lengths(arr, present);
      },
      [&](const UnionArray& u) -> Array<Data> {
        auto list_alt = u.get_alternative<List>();
        auto list_present
          = list_alt ? list_alt->present : storage::BitMap{u.length(), false};
        auto null_alt = u.get_alternative<Null>();
        auto null_present
          = null_alt ? null_alt->present : storage::BitMap{u.length(), false};
        // Rows that are present but whose active alternative is neither
        // `List` nor `Null` (a legitimate null propagates without warning).
        if (present.and_not(list_present).and_not(null_present).any()) {
          diagnostic::warning("expected `list`, got a different type")
            .primary(args.x.source)
            .emit(frame);
        }
        if (not list_alt) {
          return frame.null();
        }
        return compute_lengths(list_alt->data, list_present & present);
      },
      [&]<data_type Tag>(Array<Tag> const&) -> Array<Data> {
        auto d = diagnostic::warning("expected `list`, got `{}`",
                                     Type<Tag>::static_name)
                   .primary(args.x.source);
        if constexpr (std::same_as<Tag, String>) {
          d = std::move(d).hint(
            "use `.length_bytes()` or `.length_chars()` instead");
        }
        std::move(d).emit(frame);
        return frame.null();
      });
  }
};

class length final : public nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "length";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<LengthArgs, LengthFunction>{};
    d.positional("x", &LengthArgs::x, "list");
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "list")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> series {
        auto b = arrow::Int64Builder{tenzir::arrow_memory_pool()};
        check(b.Reserve(eval.length()));
        for (auto& value : eval(expr)) {
          auto f = detail::overload{
            [&](const arrow::ListArray& array) {
              for (auto i = int64_t{0}; i < array.length(); ++i) {
                if (array.IsNull(i)) {
                  check(b.AppendNull());
                  continue;
                }
                check(b.Append(array.value_length(i)));
              }
            },
            [&](const arrow::NullArray& array) {
              check(b.AppendNulls(array.length()));
            },
            [&]<class T>(const T& array) {
              auto d = diagnostic::warning("expected `list`, got `{}`",
                                           value.type.kind())
                         .primary(expr);
              if constexpr (std::same_as<T, arrow::StringArray>) {
                d = std::move(d).hint(
                  "use `.length_bytes()` or `.length_chars()` instead");
              }
              std::move(d).emit(ctx);
              check(b.AppendNulls(array.length()));
            },
          };
          match(*value.array, f);
        }
        return series{int64_type{}, finish(b)};
      });
  }
};

struct IsEmptyArgs {
  nova::ValueArgument x;
};

class IsEmptyFunction final {
public:
  static auto eval(IsEmptyArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using namespace nova;
    auto result = ArrayBuilder<Data>{};
    auto warn = false;
    for (auto i = storage::Index{0}; i < frame.length(); ++i) {
      if (not frame.mask().get(i)) {
        result.skip();
        continue;
      }
      match(args.x.data.get(i), [&]<data_type Tag>(RowView<Tag> value) {
        if constexpr (std::same_as<Tag, Null>) {
          result.null();
        } else if constexpr (std::same_as<Tag, String>) {
          result.data((*value).empty());
        } else if constexpr (std::same_as<Tag, List>) {
          result.data(value.length() == 0);
        } else if constexpr (std::same_as<Tag, Record>) {
          result.data(value.begin() == value.end());
        } else {
          warn = true;
          result.null();
        }
      });
    }
    if (warn) {
      diagnostic::warning("expected `string`, `list`, or `record`")
        .primary(args.x.source)
        .emit(frame);
    }
    return result.finish();
  }
};

class is_empty final : public nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "is_empty";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<IsEmptyArgs, IsEmptyFunction>{};
    d.positional("x", &IsEmptyArgs::x, "string|list|record");
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "string|list|record")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> series {
        auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
        check(b.Reserve(eval.length()));
        for (auto& value : eval(expr)) {
          auto f = detail::overload{
            [&](const arrow::StringArray& array) {
              for (auto i = int64_t{0}; i < array.length(); ++i) {
                if (array.IsNull(i)) {
                  check(b.AppendNull());
                  continue;
                }
                check(b.Append(array.value_length(i) == 0));
              }
            },
            [&](const arrow::ListArray& array) {
              for (auto i = int64_t{0}; i < array.length(); ++i) {
                if (array.IsNull(i)) {
                  check(b.AppendNull());
                  continue;
                }
                check(b.Append(array.value_length(i) == 0));
              }
            },
            [&](const arrow::StructArray& array) {
              for (auto i = int64_t{0}; i < array.length(); ++i) {
                if (array.IsNull(i)) {
                  check(b.AppendNull());
                  continue;
                }
                // Records are empty if they have no fields
                check(b.Append(array.num_fields() == 0));
              }
            },
            [&](const arrow::NullArray& array) {
              check(b.AppendNulls(array.length()));
            },
            [&]<class T>(const T& array) {
              diagnostic::warning("expected `string`, `list`, or `record`, got "
                                  "`{}`",
                                  value.type.kind())
                .primary(expr)
                .emit(ctx);
              check(b.AppendNulls(array.length()));
            },
          };
          match(*value.array, f);
        }
        return series{bool_type{}, finish(b)};
      });
  }
};

struct NetworkArgs {
  nova::ValueArgument x;
  location call;
};

class NetworkFunction final {
public:
  static auto eval(NetworkArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using namespace nova;
    return apply_kernel<1>(
      frame, "network", {args.x}, args.call,
      detail::overload{[](diagnostic_handler&, Subnet value) -> Option<Ip> {
                         return value.network();
                       },
                       [](diagnostic_handler&, Null) -> Option<Ip> {
                         return None{};
                       }});
  }
};

class network final : public nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "network";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<NetworkArgs, NetworkFunction>{};
    d.positional("x", &NetworkArgs::x, "subnet");
    d.call_location(&NetworkArgs::call);
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "subnet")
          .parse(inv, ctx));
    return function_use::make([expr = std::move(expr)](evaluator eval,
                                                       session ctx) -> series {
      TENZIR_UNUSED(ctx);
      auto value = eval(expr);
      if (value.parts().size() == 1) {
        if (auto subnets = value.part(0).as<subnet_type>()) {
          return series{
            ip_type{},
            check(subnets->array->storage()->GetFlattenedField(
              0, tenzir::arrow_memory_pool())),
          };
        }
      }
      auto b = ip_type::make_arrow_builder(arrow_memory_pool());
      check(b->Reserve(eval.length()));
      for (auto& value : value) {
        auto f = detail::overload{
          [&](const subnet_type::array_type& array) {
            check(append_array(
              *b, ip_type{},
              as<ip_type::array_type>(*check(array.storage()->GetFlattenedField(
                0, tenzir::arrow_memory_pool())))));
          },
          [&](const arrow::NullArray& array) {
            check(b->AppendNulls(array.length()));
          },
          [&]<class T>(const T& array) {
            diagnostic::warning("expected `subnet`, got `{}`",
                                value.type.kind())
              .primary(expr)
              .emit(ctx);
            check(b->AppendNulls(array.length()));
          },
        };
        match(*value.array, f);
      }
      return series{ip_type{}, finish(*b)};
    });
  }
};

struct HasArgs {
  nova::ValueArgument x;
  located<std::string> field;
};

class HasFunction final {
public:
  static auto eval(HasArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using namespace nova;
    auto const& present = frame.mask();
    auto subject = args.x;
    auto const none = storage::BitMap{present.length(), false};
    auto has_field = [&](Array<Record> const& rec,
                         storage::BitMap const& rows) -> storage::BitMap {
      auto field = rec.field(args.field.inner);
      return field ? field->present & rows : none;
    };
    return match(
      subject.data,
      [&](Array<Record> const& rec) -> Array<Data> {
        return Array<Data>{Array<Bool>{has_field(rec, present)}};
      },
      [&](const Array<Null>&) -> Array<Data> {
        return frame.null();
      },
      [&](const UnionArray& u) -> Array<Data> {
        auto rec = u.get_alternative<Record>();
        auto const rec_present = present & (rec ? rec->present : none);
        auto const bad
          = present.and_not(rec_present).and_not(u.alternative_mask<Null>());
        if (bad.any()) {
          diagnostic::warning("expected `record`, got a different type")
            .primary(args.x.source)
            .emit(frame);
        }
        if (not rec) {
          return frame.null();
        }
        return Array<Data>{Array<Bool>{has_field(rec->data, rec_present)}}
          .null_where(present.and_not(rec_present));
      },
      [&]<data_type Tag>(Array<Tag> const&) -> Array<Data> {
        diagnostic::warning("expected `record`, got `{}`",
                            Type<Tag>::static_name)
          .primary(args.x.source)
          .emit(frame);
        return frame.null();
      });
  }
};

class has final : public nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "has";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<HasArgs, HasFunction>{};
    d.positional("x", &HasArgs::x, "record");
    d.positional("field", &HasArgs::field);
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto needle = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "record")
          .positional("field", needle, "string")
          .parse(inv, ctx));
    if (auto const_needle = try_const_eval(needle, ctx)) {
      auto* str = try_as<std::string>(const_needle->inner);
      if (not str) {
        diagnostic::error(
          "expected `string`, but got `{}`",
          type::infer(const_needle->inner).value_or(type{}).kind())
          .primary(needle)
          .emit(ctx);
      }
      return function_use::make(
        [needle = located{std::move(*str), needle.get_location()},
         expr = std::move(expr)](evaluator eval, session ctx) -> series {
          auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
          check(b.Reserve(eval.length()));
          for (auto value : eval(expr)) {
            auto f = detail::overload{
              [&](const arrow::NullArray& array) {
                check(b.AppendNulls(array.length()));
              },
              [&](const arrow::StructArray& array) {
                const auto names = array.struct_type()->fields()
                                   | std::views::transform(&arrow::Field::name);
                const auto result
                  = std::ranges::find(names, needle.inner) != std::end(names);
                for (auto i = int64_t{0}; i < array.length(); i++) {
                  if (array.IsNull(i)) {
                    check(b.AppendNull());
                    continue;
                  }
                  check(b.Append(result));
                }
              },
              [&](const auto& array) {
                diagnostic::warning("expected `record`, got `{}`",
                                    value.type.kind())
                  .primary(expr)
                  .emit(ctx);
                check(b.AppendNulls(array.length()));
              }};
            match(*value.array, f);
          }
          return series{bool_type{}, finish(b)};
        });
    }
    return function_use::make([needle = std::move(needle), expr
                                                           = std::move(expr)](
                                evaluator eval, session ctx) -> multi_series {
      TENZIR_UNUSED(ctx);
      const auto expr_location = expr.get_location();
      const auto needle_location = needle.get_location();
      auto builder = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
      check(builder.Reserve(eval.length()));
      for (auto split : split_multi_series(eval(expr), eval(needle))) {
        const auto& expr = split[0];
        const auto& needle = split[1];
        const auto* expr_type = try_as<record_type>(expr.type);
        if (not expr_type) {
          if (not is<null_type>(expr.type)) {
            diagnostic::warning("expected `record`, got `{}`", expr.type.kind())
              .primary(expr_location)
              .emit(ctx);
          }
          check(builder.AppendNulls(expr.length()));
          continue;
        }
        const auto typed_needle = needle.as<string_type>();
        if (not typed_needle) {
          diagnostic::warning("expected `string`, got `{}`", needle.type.kind())
            .primary(needle_location)
            .emit(ctx);
          check(builder.AppendNulls(expr.length()));
          continue;
        }
        if (typed_needle->array->null_count() > 0) {
          diagnostic::warning("expected `string`, got `null`")
            .primary(needle_location)
            .emit(ctx);
        }
        for (auto value : *typed_needle->array) {
          if (not value) {
            check(builder.AppendNull());
            continue;
          }
          check(builder.Append(expr_type->has_field(*value)));
        }
      }
      return series{bool_type{}, finish(builder)};
    });
  }
};

struct ContainsNullArgs {
  nova::ValueArgument x;
};

class ContainsNullFunction final {
public:
  static auto has_null(nova::RowView<nova::Data> value) -> bool {
    using namespace nova;
    return match(value, []<data_type Tag>(RowView<Tag> value) {
      if constexpr (std::same_as<Tag, Null>) {
        return true;
      } else if constexpr (std::same_as<Tag, List>) {
        for (auto element : value) {
          if (has_null(element)) {
            return true;
          }
        }
        return false;
      } else if constexpr (std::same_as<Tag, Record>) {
        for (auto field : value) {
          if (has_null(field.second)) {
            return true;
          }
        }
        return false;
      } else {
        return false;
      }
    });
  }

  static auto eval(ContainsNullArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using namespace nova;
    auto result = ArrayBuilder<Bool>{};
    for (auto i = storage::Index{0}; i < frame.length(); ++i) {
      if (frame.mask().get(i)) {
        result.data(has_null(args.x.data.get(i)));
      } else {
        result.skip();
      }
    }
    return Array<Data>{result.finish()};
  }
};

class contains_null final : public nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "contains_null";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<ContainsNullArgs, ContainsNullFunction>{};
    d.positional("x", &ContainsNullArgs::x, "any");
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "any")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session) -> series {
        auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
        check(b.Reserve(eval.length()));
        for (const auto& s : eval(expr)) {
          const auto& array = *s.array;
          auto mask = check(arrow::compute::IsNull(array));
          update_mask(mask, array);
          auto bool_array = mask.array_as<arrow::BooleanArray>();
          check(append_array(b, bool_type{}, *bool_array));
        }
        return series{bool_type{}, finish(b)};
      });
  }

  constexpr static auto
  update_mask(arrow::Datum& mask, const arrow::Array& array) -> void {
    if (const auto* sub = try_as<arrow::StructArray>(array)) {
      for (const auto& field : sub->fields()) {
        const auto fmask = check(arrow::compute::IsNull(*field));
        mask = check(arrow::compute::Or(mask, fmask));
        update_mask(mask, *field);
      }
      return;
    }
    if (const auto* sub = try_as<arrow::ListArray>(array)) {
      auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
      check(b.Reserve(sub->length()));
      for (auto i = int64_t{}; i < sub->length(); ++i) {
        if (sub->IsValid(i)) {
          const auto& slice = sub->value_slice(i);
          b.UnsafeAppend(has_null(*slice));
          continue;
        }
        b.UnsafeAppend(true);
      }
      const auto lmask = finish(b);
      mask = check(arrow::compute::Or(mask, *lmask));
    }
  }

  constexpr static auto has_null(const arrow::Array& array) -> bool {
    if (array.null_count() != 0) {
      return true;
    }
    if (const auto* sub = try_as<arrow::StructArray>(array)) {
      for (const auto& field : sub->fields()) {
        if (has_null(*field)) {
          return true;
        }
      }
    }
    if (const auto* sub = try_as<arrow::ListArray>(array)) {
      for (auto i = int64_t{}; i < sub->length(); ++i) {
        const auto& slice = (sub->value_slice(i));
        if (has_null(*slice)) {
          return true;
        }
      }
    }
    return false;
  }
};

struct KeysArgs {
  nova::ValueArgument x;
};

class KeysFunction final {
public:
  static auto eval(KeysArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using namespace nova;
    auto result = ArrayBuilder<Data>{};
    auto warn = false;
    for (auto i = storage::Index{0}; i < frame.length(); ++i) {
      if (not frame.mask().get(i)) {
        result.skip();
        continue;
      }
      match(args.x.data.get(i), [&]<data_type Tag>(RowView<Tag> value) {
        if constexpr (std::same_as<Tag, Record>) {
          auto list = result.list();
          for (auto const& [name, field] : value) {
            TENZIR_UNUSED(field);
            list.data(name);
          }
        } else if constexpr (std::same_as<Tag, Null>) {
          result.null();
        } else {
          warn = true;
          result.null();
        }
      });
    }
    if (warn) {
      diagnostic::warning("expected `record`")
        .primary(args.x.source)
        .emit(frame);
    }
    return result.finish();
  }
};

class keys final : public nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "keys";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<KeysArgs, KeysFunction>{};
    d.positional("x", &KeysArgs::x, "record");
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "record")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> multi_series {
        const auto result_type = list_type{string_type{}};
        return map_series(eval(expr), [&](auto&& subject) -> series {
          return match(
            subject.type,
            [&](const null_type&) {
              return series::null(result_type, subject.length());
            },
            [&](const record_type& type) {
              auto keys_builder = arrow::StringBuilder{};
              check(keys_builder.Reserve(
                detail::narrow<int64_t>(type.num_fields())));
              for (const auto& field : type.fields()) {
                check(keys_builder.Append(field.name));
              }
              return series{
                result_type,
                check(arrow::MakeArrayFromScalar(
                  arrow::ListScalar{
                    finish(keys_builder),
                    result_type.to_arrow_type(),
                  },
                  subject.length(), tenzir::arrow_memory_pool())),
              };
            },
            [&](const auto&) {
              diagnostic::warning("expected `record`, got `{}`",
                                  subject.type.kind())
                .primary(expr)
                .emit(ctx);
              return series::null(result_type, subject.length());
            });
        });
      });
  }
};

struct SelectDropMatchingArgs {
  nova::ValueArgument x;
  located<std::string> regex;
  pattern compiled;
  bool select = false;
};

class SelectDropMatchingFunction final {
public:
  static auto eval(SelectDropMatchingArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using namespace nova;
    auto records = args.x.data.get_alternative<Record>();
    auto valid = storage::BitMap{frame.length(), false};
    if (records) {
      valid = records->present;
    }
    if (auto nulls = args.x.data.get_alternative<Null>()) {
      valid = std::move(valid) | nulls->present;
    }
    auto invalid = frame.mask().and_not(valid);
    if (invalid.any()) {
      diagnostic::warning("expected `record`")
        .primary(args.x.source)
        .emit(frame);
    }
    if (not records) {
      return args.x.data.null_where(std::move(invalid));
    }
    auto removed = std::vector<std::string>{};
    auto seen = detail::heterogeneous_string_hashset{};
    auto record_rows = frame.mask() & records->present;
    for (auto i = storage::Index{0}; i < frame.length(); ++i) {
      if (not record_rows.get(i)) {
        continue;
      }
      for (auto const& [name, field] : records->data.get(i)) {
        TENZIR_UNUSED(field);
        if (seen.insert(std::string{name}).second
            and args.compiled.search(name) != args.select) {
          removed.emplace_back(name);
        }
      }
    }
    auto removed_views = std::vector<std::string_view>{};
    removed_views.reserve(removed.size());
    for (auto const& name : removed) {
      removed_views.push_back(name);
    }
    return args.x.data
      .map_alternative<Record>([&](MaskedArray<Array<Record>> records) {
        return std::move(records.data)
          .without_fields(removed_views, record_rows & records.present);
      })
      .null_where(std::move(invalid));
  }
};

class select_drop_matching final : public nova::FunctionPlugin {
public:
  explicit select_drop_matching(bool select) : select_{select} {
  }

  auto name() const -> std::string override {
    return select_ ? "select_matching" : "drop_matching";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<SelectDropMatchingArgs,
                                     SelectDropMatchingFunction>{};
    d.positional("x", &SelectDropMatchingArgs::x, "record");
    d.positional("regex", &SelectDropMatchingArgs::regex);
    d.validate([select = select_](auto& args,
                                  diagnostic_handler& dh) -> failure_or<void> {
      auto compiled = pattern::make(args.regex.inner);
      if (not compiled) {
        diagnostic::error(compiled.error()).primary(args.regex).emit(dh);
        return failure::promise();
      }
      args.compiled = std::move(*compiled);
      args.select = select;
      return {};
    });
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto str = located<std::string>{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "record")
          .positional("regex", str)
          .parse(inv, ctx));
    auto pattern = pattern::make(str.inner);
    if (not pattern) {
      diagnostic::error(pattern.error()).primary(str.source).emit(ctx);
      return failure::promise();
    }
    return function_use::make([pattern = std::move(*pattern),
                               expr = std::move(expr),
                               select = select_](evaluator eval, session ctx) {
      return map_series(eval(expr), [&](series value) {
        auto f = detail::overload{
          [&](const arrow::NullArray& array) -> series {
            return series::null(null_type{}, array.length());
          },
          [&](const arrow::StructArray& array) -> series {
            auto fields = std::vector<series_field>{};
            for (const auto& [field, field_array] : std::views::zip(
                   as<record_type>(value.type).fields(), array.fields())) {
              if (pattern.search(field.name) == select) {
                fields.push_back({field.name, {field.type, field_array}});
              }
            }
            return make_record_series(fields, array);
          },
          [&](const auto&) -> series {
            diagnostic::warning("expected `record`, got `{}`",
                                value.type.kind())
              .primary(expr)
              .emit(ctx);
            return series::null(null_type{}, value.length());
          }};
        return match(*value.array, f);
      });
    });
  }

private:
  bool select_ = {};
};

// Columnar deep merge for two series. Recurses into nested records so that
// overlapping record fields are merged rather than replaced. Non-record
// conflicts are resolved in favor of the right side.
auto deep_merge_series(series left, series right, size_t depth = 0) -> series {
  auto length = left.length();
  // Both null-typed: return null.
  if (is<null_type>(left.type) and is<null_type>(right.type)) {
    return series::null(null_type{}, length);
  }
  // Left is null-typed: right wins entirely.
  if (is<null_type>(left.type)) {
    return right;
  }
  // Right is null-typed: left wins entirely.
  if (is<null_type>(right.type)) {
    return left;
  }
  // Both records: deep merge.
  auto left_rec = left.as<record_type>();
  auto right_rec = right.as<record_type>();
  if (left_rec and right_rec) {
    if (depth >= defaults::max_recursion) {
      return right;
    }
    // Flatten propagates parent null bitmaps into children.
    auto left_flat
      = check(left_rec->array->Flatten(tenzir::arrow_memory_pool()));
    auto right_flat
      = check(right_rec->array->Flatten(tenzir::arrow_memory_pool()));
    // Collect left fields into a stable map (preserves insertion order).
    auto fields = detail::stable_map<std::string, series>{};
    for (auto [i, arr] : detail::enumerate(left_flat)) {
      auto f = left_rec->type.field(i);
      fields[std::string{f.name}] = series{f.type, arr};
    }
    // Condition for IfElse: where the right parent struct is valid.
    auto right_valid = check(arrow::compute::IsValid(*right_rec->array));
    // Process right fields.
    for (auto [i, arr] : detail::enumerate(right_flat)) {
      auto f = right_rec->type.field(i);
      auto right_f = series{f.type, arr};
      auto it = fields.find(f.name);
      if (it == fields.end()) {
        // Right-only field.
        fields[std::string{f.name}] = std::move(right_f);
        continue;
      }
      auto& left_f = it->second;
      // Overlapping field: both are records -> recurse.
      if (left_f.type.kind().is<record_type>()
          and right_f.type.kind().is<record_type>()) {
        it->second
          = deep_merge_series(std::move(left_f), std::move(right_f), depth + 1);
        continue;
      }
      // Overlapping field: same type -> IfElse on parent validity.
      if (left_f.type == right_f.type) {
        auto merged = check(arrow::compute::CallFunction(
          "if_else", {right_valid, *right_f.array, *left_f.array}));
        it->second = series{left_f.type, merged.make_array()};
        continue;
      }
      // Overlapping field: different non-record types -> right wins.
      // After Flatten, right_f already has nulls where the right parent was
      // null, which is the correct behavior for a type conflict.
      it->second = std::move(right_f);
    }
    // Build result null bitmap: valid where either parent was valid.
    auto left_valid = check(arrow::compute::IsValid(*left_rec->array));
    auto result_valid = check(arrow::compute::Or(left_valid, right_valid));
    auto result_bitmap = result_valid.make_array()->data()->buffers[1];
    // Reconstruct struct array.
    auto field_names = std::vector<std::string>{};
    auto field_arrays = arrow::ArrayVector{};
    auto field_types = std::vector<record_type::field_view>{};
    for (auto& [name, s] : fields) {
      field_names.push_back(name);
      field_arrays.push_back(s.array);
      field_types.push_back({name, s.type});
    }
    auto result_type = type{record_type{field_types}};
    auto result
      = make_struct_array(length, std::move(result_bitmap),
                          std::move(field_names), std::move(field_arrays),
                          as<record_type>(result_type));
    return series{std::move(result_type), std::move(result)};
  }
  // Non-record, same type: IfElse (right wins where valid).
  if (left.type == right.type) {
    auto right_valid = check(arrow::compute::IsValid(*right.array));
    auto merged = check(arrow::compute::CallFunction(
      "if_else", {right_valid, *right.array, *left.array}));
    return series{left.type, merged.make_array()};
  }
  // Non-record, different types: right wins entirely.
  return right;
}

struct MergeArgs {
  nova::ValueArgument x;
  nova::ValueArgument y;
};

class MergeFunction final {
public:
  template <class Builder>
  static auto append(Builder&& output, nova::RowView<nova::Data> left,
                     nova::RowView<nova::Data> right, size_t depth = 0)
    -> void {
    using namespace nova;
    match(left,
          detail::overload{
            [&](RowView<Null> left) {
              TENZIR_UNUSED(left);
              append_row(output, right);
            },
            [&](RowView<Record> left) {
              match(right,
                    detail::overload{
                      [&](RowView<Null> right) {
                        if (depth == 0) {
                          TENZIR_UNUSED(right);
                          append_row(output, left);
                        } else {
                          append_row(output, right);
                        }
                      },
                      [&](RowView<Record> right) {
                        if (depth >= defaults::max_recursion) {
                          append_row(output, right);
                          return;
                        }
                        auto right_fields
                          = std::vector<RowView<Record>::value_type>{};
                        auto right_indices
                          = std::unordered_map<std::string_view, size_t>{};
                        for (auto const& field : right) {
                          right_indices.emplace(field.first,
                                                right_fields.size());
                          right_fields.push_back(field);
                        }
                        auto matched
                          = std::vector<bool>(right_fields.size(), false);
                        auto record = output.record();
                        for (auto const& [name, left_value] : left) {
                          auto index = right_indices.find(name);
                          if (index == right_indices.end()) {
                            append_row(record.field(name), left_value);
                            continue;
                          }
                          matched[index->second] = true;
                          append(record.field(name), left_value,
                                 right_fields[index->second].second, depth + 1);
                        }
                        for (auto const& [index, field] :
                             detail::enumerate(right_fields)) {
                          if (not matched[index]) {
                            append_row(record.field(field.first), field.second);
                          }
                        }
                      },
                      [&](auto right) {
                        append_row(output, right);
                      },
                    });
            },
            [&](auto left) {
              match(right, detail::overload{
                             [&](RowView<Null> right) {
                               if (depth == 0) {
                                 TENZIR_UNUSED(right);
                                 append_row(output, left);
                               } else {
                                 append_row(output, right);
                               }
                             },
                             [&](auto right) {
                               append_row(output, right);
                             },
                           });
            },
          });
  }

  static auto eval(MergeArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using namespace nova;
    auto result = ArrayBuilder<Data>{};
    for (auto i = storage::Index{0}; i < frame.length(); ++i) {
      if (frame.mask().get(i)) {
        append(result, args.x.data.get(i), args.y.data.get(i));
      } else {
        result.skip();
      }
    }
    return result.finish();
  }
};

class merge final : public nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "merge";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<MergeArgs, MergeFunction>{};
    d.positional("x", &MergeArgs::x, "record");
    d.positional("y", &MergeArgs::y, "record");
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto record1 = ast::expression{};
    auto record2 = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", record1, "record")
          .positional("y", record2, "record")
          .parse(inv, ctx));
    return function_use::make(
      [record1 = std::move(record1),
       record2 = std::move(record2)](evaluator eval, session ctx) {
        TENZIR_UNUSED(ctx);
        return map_series(eval(record1), eval(record2),
                          [&](series left, series right) -> multi_series {
                            return deep_merge_series(std::move(left),
                                                     std::move(right));
                          });
      });
  }
};

struct GetArgs {
  nova::ValueArgument x;
  nova::ValueArgument field;
  nova::LazyArgument fallback;
};

class GetFunction final {
public:
  static auto eval(GetArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using namespace nova;
    auto records = args.x.data.get_alternative<Record>();
    if (records) {
      auto allowed = records->present;
      if (auto nulls = args.x.data.get_alternative<Null>()) {
        allowed = std::move(allowed) | nulls->present;
      }
      auto indices = args.field.data.try_as<String>();
      using ConstantString
        = storage::ConstantStorage<typename Type<String>::DataType,
                                   typename Type<String>::ViewType>;
      auto const* index
        = indices ? try_as<ConstantString>(indices->storage()) : nullptr;
      if (not frame.mask().and_not(allowed).any() and index) {
        auto record_rows = frame.mask() & records->present;
        auto field = records->data.field(index->get(0));
        auto value_rows = storage::BitMap{frame.length(), false};
        if (field) {
          value_rows = record_rows & field->present;
          if (auto nulls = field->data.get_alternative<Null>()) {
            value_rows = value_rows.and_not(nulls->present);
          }
        }
        auto fallback_rows = frame.mask().and_not(value_rows);
        if (not args.fallback) {
          if ((not field and record_rows.any())
              or (field and record_rows.and_not(field->present).any())) {
            diagnostic::warning("record has no field")
              .primary(args.field.source)
              .emit(frame);
          }
          if (frame.mask().and_not(records->present).any()) {
            diagnostic::warning("cannot index into `null`")
              .primary(args.x.source)
              .emit(frame);
          }
          return field ? field->data.null_where(std::move(fallback_rows))
                       : frame.null();
        }
        auto primary
          = field ? field->data.null_where(fallback_rows) : frame.null();
        if (not fallback_rows.any()) {
          return primary;
        }
        auto fallback = frame.narrow(fallback_rows).eval(args.fallback);
        return with_merged(MaskedArray<Array<Data>>{std::move(primary),
                                                    std::move(value_rows)},
                           MaskedArray<Array<Data>>{std::move(fallback.data),
                                                    std::move(fallback_rows)});
      }
    }
    auto result = ArrayBuilder<Data>{};
    auto fallback_rows = storage::BitMap::Mutable{frame.length()};
    auto record_fields
      = std::unordered_map<std::string, Option<MaskedArray<Array<Data>>>,
                           detail::heterogeneous_string_hash,
                           detail::heterogeneous_string_equal>{};
    auto bad_subject = false;
    auto bad_index = false;
    auto out_of_bounds = false;
    auto record_out_of_bounds = false;
    auto missing_field = false;
    auto null_subject = false;
    auto null_index = false;
    for (auto i = storage::Index{0}; i < frame.length(); ++i) {
      if (not frame.mask().get(i)) {
        result.skip();
        continue;
      }
      auto fallback = [&] {
        fallback_rows.set(i, true);
        result.null();
      };
      match(args.x.data.get(i), [&](auto subject) {
        if constexpr (std::same_as<decltype(subject), RowView<List>>) {
          auto position = Option<int64_t>{};
          auto index_is_null = false;
          match(args.field.data.get(i), [&](auto index) {
            if constexpr (std::same_as<decltype(index), RowView<Int>>) {
              position = *index < 0 ? subject.length() + *index : *index;
            } else if constexpr (std::same_as<decltype(index), RowView<UInt>>) {
              if (*index <= static_cast<uint64_t>(INT64_MAX)) {
                position = static_cast<int64_t>(*index);
              } else {
                out_of_bounds = true;
              }
            } else if constexpr (std::same_as<decltype(index), RowView<Null>>) {
              index_is_null = true;
              null_index = true;
            } else {
              bad_index = true;
            }
          });
          if (position and *position >= 0 and *position < subject.length()) {
            auto value = subject.get(*position);
            if (match(value, detail::overload{[](RowView<Null>) {
                                                return true;
                                              },
                                              [](auto) {
                                                return false;
                                              }})) {
              fallback();
            } else {
              append_row(result, value);
            }
          } else {
            if (not index_is_null and position) {
              out_of_bounds = true;
            }
            fallback();
          }
        } else if constexpr (std::same_as<decltype(subject), RowView<Record>>) {
          auto found = Option<RowView<Data>>{};
          auto index_is_null = false;
          auto index_is_invalid = false;
          auto index_is_numeric = false;
          match(args.field.data.get(i), [&](auto index) {
            if constexpr (std::same_as<decltype(index), RowView<String>>) {
              TENZIR_ASSERT(records);
              auto field = record_fields.find(*index);
              if (field == record_fields.end()) {
                field
                  = record_fields
                      .emplace(std::string{*index}, records->data.field(*index))
                      .first;
              }
              if (field->second and field->second->present.get(i)) {
                found = field->second->data.get(i);
              }
            } else if constexpr (std::same_as<decltype(index), RowView<Int>>
                                 or std::same_as<decltype(index),
                                                 RowView<UInt>>) {
              index_is_numeric = true;
              auto wanted = static_cast<uint64_t>(*index);
              auto current = uint64_t{0};
              for (auto const& [name, value] : subject) {
                TENZIR_UNUSED(name);
                if (current++ == wanted) {
                  found = value;
                  break;
                }
              }
            } else if constexpr (std::same_as<decltype(index), RowView<Null>>) {
              index_is_null = true;
              null_index = true;
            } else {
              index_is_invalid = true;
              bad_index = true;
            }
          });
          if (found) {
            if (match(*found, detail::overload{[](RowView<Null>) {
                                                 return true;
                                               },
                                               [](auto) {
                                                 return false;
                                               }})) {
              fallback();
            } else {
              append_row(result, *found);
            }
          } else {
            if (index_is_numeric) {
              record_out_of_bounds = true;
            } else if (not index_is_null and not index_is_invalid) {
              missing_field = true;
            }
            fallback();
          }
        } else if constexpr (std::same_as<decltype(subject), RowView<Null>>) {
          null_subject = true;
          fallback();
        } else {
          bad_subject = true;
          fallback();
        }
      });
    }
    if (not args.fallback) {
      if (out_of_bounds) {
        diagnostic::warning("list index out of bounds")
          .primary(args.field.source)
          .emit(frame);
      }
      if (record_out_of_bounds) {
        diagnostic::warning("index out of bounds")
          .primary(args.field.source)
          .emit(frame);
      }
      if (missing_field) {
        diagnostic::warning("record has no field")
          .primary(args.field.source)
          .emit(frame);
      }
      if (null_subject) {
        diagnostic::warning("cannot index into `null`")
          .primary(args.x.source)
          .emit(frame);
      }
      if (null_index) {
        diagnostic::warning("cannot use `null` as index")
          .primary(args.field.source)
          .emit(frame);
      }
    }
    if (bad_subject) {
      diagnostic::warning("expected `record` or `list`")
        .primary(args.x.source)
        .emit(frame);
    }
    if (bad_index) {
      diagnostic::warning("cannot use this value as index")
        .primary(args.field.source)
        .emit(frame);
    }
    auto primary = result.finish();
    if (not args.fallback) {
      return primary;
    }
    auto fallback_mask = std::move(fallback_rows).finish();
    if (not fallback_mask.any()) {
      return primary;
    }
    auto fallback = frame.narrow(fallback_mask).eval(args.fallback);
    return with_merged(
      MaskedArray<Array<Data>>{std::move(primary),
                               frame.mask().and_not(fallback_mask)},
      MaskedArray<Array<Data>>{std::move(fallback.data),
                               std::move(fallback_mask)});
  }
};

class get final : public nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "get";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<GetArgs, GetFunction>{};
    d.positional("x", &GetArgs::x, "record|list");
    d.positional("field", &GetArgs::field, "string|int|uint");
    d.optional_positional("fallback", &GetArgs::fallback, "any");
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject = ast::expression{};
    auto field = ast::expression{};
    auto fallback = Option<ast::expression>{};
    TRY(argument_parser2::function(name())
          .positional("x", subject, "record|list")
          .positional("field", field, "string|int|uint")
          .positional("fallback", fallback, "any")
          .parse(inv, ctx));
    return function_use::make(
      [subject = std::move(subject), field = std::move(field),
       fallback = std::move(fallback)](evaluator eval,
                                       session ctx) mutable -> multi_series {
        TENZIR_UNUSED(ctx);
        auto expr = ast::expression{
          ast::index_expr{
            subject,
            location::unknown,
            field,
            location::unknown,
            // We suppress warnings iff there is a fallback value provided.
            fallback.has_value(),
            // Mark this as synthesized by `get` to adjust warning phrasing.
            true,
          },
        };
        if (fallback) {
          expr = ast::expression{
            ast::binary_expr{
              std::move(expr),
              ast::binary_op::else_,
              std::move(*fallback),
            },
          };
        }
        return eval(expr);
      });
  }
};

} // namespace

} // namespace tenzir::plugins::misc

TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::env)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::get)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::has)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::contains_null)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::is_empty)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::keys)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::length)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::merge)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::network)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::select_drop_matching{false})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::select_drop_matching{true})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::type_id)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::misc::type_of)
