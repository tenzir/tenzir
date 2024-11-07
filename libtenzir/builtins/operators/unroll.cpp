//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/argument_parser2.hpp>
#include <tenzir/collect.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::unroll {

namespace {

auto unroll_type(const type& src, const offset& off, size_t index = 0) -> type {
  TENZIR_ASSERT(index <= off.size());
  if (index == off.size()) {
    auto list = caf::get_if<list_type>(&src);
    TENZIR_ASSERT(list);
    return list->value_type();
  }
  auto record = caf::get_if<record_type>(&src);
  TENZIR_ASSERT(record);
  auto fields = std::vector<struct record_type::field_view>{};
  auto current = size_t{0};
  auto target = off[index];
  for (auto&& field : record->fields()) {
    if (current == target) {
      fields.emplace_back(field.name, unroll_type(field.type, off, index + 1));
    } else {
      fields.push_back(field);
    }
    ++current;
  }
  return type{src.name(), record_type{fields}, collect(src.attributes())};
};

class unroller {
public:
  unroller(const offset& offset, const arrow::ListArray& list_array,
           int64_t row)
    : offset_{offset},
      list_array_{list_array},
      row_{row},
      list_begin_{list_array.value_offset(row)},
      list_length_{list_array.value_offset(row + 1) - list_begin_} {
  }

  void run(arrow::StructBuilder& builder, const arrow::StructArray& source,
           const record_type& ty) {
    TENZIR_ASSERT(row_ < source.length());
    process_struct(builder, source, ty, 0);
  }

private:
  void process_struct(arrow::StructBuilder& builder,
                      const arrow::StructArray& source, const record_type& ty,
                      size_t index) {
    TENZIR_ASSERT(index < offset_.size());
    for (auto i = 0; i < list_length_; ++i) {
      auto result = builder.Append();
      TENZIR_ASSERT(result.ok());
    }
    auto target = detail::narrow<int>(offset_[index]);
    for (auto current = 0; current < builder.num_fields(); ++current) {
      if (current == target) {
        process(*builder.field_builder(target), *source.field(target),
                ty.field(current).type, index + 1);
      } else {
        for (auto i = int64_t{0}; i < list_length_; ++i) {
          auto status = append_array_slice(*builder.field_builder(current),
                                           ty.field(current).type,
                                           *source.field(current), row_, 1);
          TENZIR_ASSERT(status.ok(), status.ToString());
        }
      }
    }
  }

  void process(arrow::ArrayBuilder& builder, const arrow::Array& source,
               const type& ty, size_t index) {
    TENZIR_ASSERT(index <= offset_.size());
    if (index == offset_.size()) {
      // We arrived at the offset where the list values shall be placed.
      auto result
        = append_array_slice(builder, caf::get<list_type>(ty).value_type(),
                             *list_array_.values(), list_begin_, list_length_);
      TENZIR_ASSERT(result.ok());
      return;
    }
    auto fb = dynamic_cast<arrow::StructBuilder*>(&builder);
    TENZIR_ASSERT(fb);
    auto fs = dynamic_cast<const arrow::StructArray*>(&source);
    TENZIR_ASSERT(fs);
    auto ty2 = caf::get_if<record_type>(&ty);
    TENZIR_ASSERT(ty2);
    process_struct(*fb, *fs, *ty2, index);
  }

  const offset& offset_;
  const arrow::ListArray& list_array_;
  int64_t row_;
  int64_t list_begin_;
  int64_t list_length_;
};

/// Unrolls the list located at `offset` by duplicating the surrounding data,
/// once for each list item.
auto unroll(const table_slice& slice, const offset& offset) -> table_slice {
  auto resolved = offset.get(slice);
  auto list_array = dynamic_cast<arrow::ListArray*>(&*resolved.second);
  TENZIR_ASSERT(list_array);
  auto list_offsets
    = std::dynamic_pointer_cast<arrow::Int32Array>(list_array->offsets());
  TENZIR_ASSERT(list_offsets);
  auto result_ty = unroll_type(slice.schema(), offset);
  auto builder = std::dynamic_pointer_cast<arrow::StructBuilder>(
    result_ty.make_arrow_builder(arrow::default_memory_pool()));
  TENZIR_ASSERT(builder);
  for (auto row = int64_t{0}; row < list_array->length(); ++row) {
    if (list_array->IsNull(row)) {
      continue;
    }
    TENZIR_ASSERT(row + 1 < list_offsets->length());
    auto begin = list_offsets->Value(row);
    auto end = list_offsets->Value(row + 1);
    TENZIR_ASSERT(begin <= end);
    if (begin == end) {
      continue;
    }
    auto source = to_record_batch(slice)->ToStructArray();
    TENZIR_ASSERT(source.ok());
    TENZIR_ASSERT(*source);
    unroller{offset, *list_array, row}.run(
      *builder, **source, caf::get<record_type>(slice.schema()));
  }
  auto result = std::shared_ptr<arrow::StructArray>{};
  auto status = builder->Finish(&result);
  TENZIR_ASSERT(status.ok());
  auto batch = arrow::RecordBatch::Make(result_ty.to_arrow_schema(),
                                        result->length(), result->fields());
  return table_slice{batch, result_ty};
}

class unroll_operator final : public crtp_operator<unroll_operator> {
public:
  unroll_operator() = default;

  explicit unroll_operator(ast::simple_selector field)
    : field_{std::move(field)} {
  }

  explicit unroll_operator(located<std::string> field)
    : field_{std::move(field)} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    const auto get_offset = field_.match<
      std::function<auto(const table_slice&)->std::optional<offset>>>(
      [&](const located<std::string>& field) {
        return [&](const table_slice& slice) -> std::optional<offset> {
          auto offsets = collect(slice.schema().resolve(field.inner));
          if (offsets.empty()) {
            diagnostic::warning("field `{}` not found", field.inner)
              .primary(field)
              .emit(ctrl.diagnostics());
            return {};
          }
          if (offsets.size() > 1) {
            diagnostic::warning("field `{}` resolved multiple times for `{}` "
                                "and will be ignored",
                                field.inner, slice.schema().name())
              .primary(field)
              .emit(ctrl.diagnostics());
            return {};
          }
          const auto& field_type
            = caf::get<record_type>(slice.schema()).field(offsets.front()).type;
          if (caf::holds_alternative<null_type>(field_type)) {
            return {};
          }
          if (not caf::holds_alternative<list_type>(field_type)) {
            diagnostic::warning("expected `list`, but got `{}`",
                                field_type.kind())
              .primary(field)
              .emit(ctrl.diagnostics());
            return {};
          }
          return offsets.front();
        };
      },
      [&](const ast::simple_selector& field) {
        return [&](const table_slice& slice) {
          return resolve(field, slice.schema())
            .match(
              [&](offset result) -> std::optional<offset> {
                const auto& field_type
                  = caf::get<record_type>(slice.schema()).field(result).type;
                if (caf::holds_alternative<null_type>(field_type)) {
                  return {};
                }
                if (not caf::holds_alternative<list_type>(field_type)) {
                  diagnostic::warning("expected `list`, but got `{}`",
                                      field_type.kind())
                    .primary(field)
                    .emit(ctrl.diagnostics());
                  return {};
                }
                return result;
              },
              [&](const resolve_error& err) -> std::optional<offset> {
                err.reason.match(
                  [&](resolve_error::field_not_found) {
                    diagnostic::warning("field `{}` not found", err.ident.name)
                      .primary(err.ident)
                      .emit(ctrl.diagnostics());
                  },
                  [&](const resolve_error::field_of_non_record& reason) {
                    diagnostic::warning("type `{}` has no field `{}`",
                                        reason.type.kind(), err.ident.name)
                      .primary(err.ident)
                      .emit(ctrl.diagnostics());
                  });
                return {};
              });
        };
      });
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      const auto offset = get_offset(slice);
      if (not offset) {
        // Zero or multiple offsets; cannot proceed.
        continue;
      }
      co_yield unroll(slice, *offset);
    }
  }

  auto name() const -> std::string override {
    return "unroll";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    return optimize_result::order_invariant(*this, order);
  }

  friend auto inspect(auto& f, unroll_operator& x) -> bool {
    return f.object(x).fields(f.field("field", x.field_));
  }

private:
  variant<ast::simple_selector, located<std::string>> field_;
};

class plugin final : public virtual operator_plugin<unroll_operator>,
                     public virtual operator_factory_plugin {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"unroll", "https://docs.tenzir.com/"
                                            "operators/unroll"};
    auto field = located<std::string>{};
    parser.add(field, "<field>");
    parser.parse(p);
    return std::make_unique<unroll_operator>(std::move(field));
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto field = ast::simple_selector{};
    auto parser = argument_parser2::operator_(name());
    parser.add(field, "<field>");
    TRY(parser.parse(inv, ctx));
    return std::make_unique<unroll_operator>(std::move(field));
  }
};

} // namespace

} // namespace tenzir::plugins::unroll

TENZIR_REGISTER_PLUGIN(tenzir::plugins::unroll::plugin)
