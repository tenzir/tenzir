//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arc.hpp>
#include <tenzir/argument_parser.hpp>
#include <tenzir/argument_parser2.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/bitmap.hpp>
#include <tenzir/collect.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/try.hpp>

#include <algorithm>
#include <limits>

namespace tenzir::plugins::unroll {

namespace {

using namespace tenzir::si_literals;

constexpr auto max_unroll_slice_rows
  = static_cast<int64_t>(defaults::import::table_slice_size);

constexpr auto max_unroll_slice_bytes = uint64_t{512_Mi};

auto unroll_type(const type& src, const offset& off, size_t index = 0) -> type {
  TENZIR_ASSERT(index <= off.size());
  if (index == off.size()) {
    auto list = try_as<list_type>(&src);
    TENZIR_ASSERT(list);
    return list->value_type();
  }
  auto record = try_as<record_type>(&src);
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
           int64_t row, int64_t list_begin, int64_t list_length)
    : offset_{offset},
      list_array_{list_array},
      row_{row},
      list_begin_{list_begin},
      list_length_{list_length} {
  }

  auto run(arrow::StructBuilder& builder, const arrow::StructArray& source,
           const record_type& ty) -> arrow::Status {
    TENZIR_ASSERT(row_ < source.length());
    return process_struct(builder, source, ty, 0);
  }

private:
  auto process_struct(arrow::StructBuilder& builder,
                      const arrow::StructArray& source, const record_type& ty,
                      size_t index) -> arrow::Status {
    TENZIR_ASSERT(index < offset_.size());
    TRY(builder.Reserve(list_length_));
    for (auto i = 0; i < list_length_; ++i) {
      TRY(builder.Append());
    }
    auto target = detail::narrow<int>(offset_[index]);
    for (auto current = 0; current < builder.num_fields(); ++current) {
      if (current == target) {
        TRY(process(*builder.field_builder(target), *source.field(target),
                    ty.field(current).type, index + 1));
      } else {
        for (auto i = int64_t{0}; i < list_length_; ++i) {
          TRY(append_array_slice(*builder.field_builder(current),
                                 ty.field(current).type, *source.field(current),
                                 row_, 1));
        }
      }
    }
    return arrow::Status::OK();
  }

  auto process(arrow::ArrayBuilder& builder, const arrow::Array& source,
               const type& ty, size_t index) -> arrow::Status {
    TENZIR_ASSERT(index <= offset_.size());
    if (index == offset_.size()) {
      // We arrived at the offset where the list values shall be placed.
      return append_array_slice(builder, as<list_type>(ty).value_type(),
                                *list_array_.values(), list_begin_,
                                list_length_);
    }
    auto fb = dynamic_cast<arrow::StructBuilder*>(&builder);
    TENZIR_ASSERT(fb);
    auto fs = dynamic_cast<const arrow::StructArray*>(&source);
    TENZIR_ASSERT(fs);
    auto ty2 = try_as<record_type>(&ty);
    TENZIR_ASSERT(ty2);
    return process_struct(*fb, *fs, *ty2, index);
  }

  const offset& offset_;
  const arrow::ListArray& list_array_;
  int64_t row_;
  int64_t list_begin_;
  int64_t list_length_;
};

auto make_unroll_builder(const type& ty) -> Arc<arrow::StructBuilder> {
  auto result = std::dynamic_pointer_cast<arrow::StructBuilder>(
    ty.make_arrow_builder(arrow_memory_pool()));
  TENZIR_ASSERT(result);
  return Arc<arrow::StructBuilder>::from_non_null(std::move(result));
}

auto emit_unroll_error(diagnostic_handler& dh, const arrow::Status& status)
  -> void {
  diagnostic::error("failed to unroll list: {}", status.ToString()).emit(dh);
}

struct finish_unroll_result {
  Option<table_slice> slice = None{};
  bool failed = {};
};

auto finish_unroll_builder(arrow::StructBuilder& builder, const type& result_ty,
                           diagnostic_handler& dh, bool allow_empty = false)
  -> finish_unroll_result {
  if (builder.length() == 0 and not allow_empty) {
    return {};
  }
  auto result = std::shared_ptr<arrow::StructArray>{};
  auto status = builder.Finish(&result);
  if (not status.ok()) {
    emit_unroll_error(dh, status);
    return {.failed = true};
  }
  auto batch
    = record_batch_from_struct_array(result_ty.to_arrow_schema(), *result);
  return {.slice = table_slice{batch, result_ty}};
}

auto add_saturated(uint64_t current, uint64_t value, int64_t count)
  -> uint64_t {
  const auto max = std::numeric_limits<uint64_t>::max();
  auto unsigned_count = detail::narrow<uint64_t>(count);
  if (value != 0 and unsigned_count > (max - current) / value) {
    return max;
  }
  return current + value * unsigned_count;
}

/// Unrolls the list located at `offset` by duplicating the surrounding data,
/// once for each list item.
auto unroll(const table_slice& slice, const offset& offset, bool unordered,
            diagnostic_handler& dh) -> generator<table_slice> {
  auto resolved = offset.get(slice);
  if (const auto* rt = try_as<record_type>(resolved.first)) {
    const auto& sa = as<arrow::StructArray>(*resolved.second);
    auto transformed_slices = std::vector<table_slice>{};
    transformed_slices.reserve(rt->num_fields());
    for (auto i = size_t{}; i < rt->num_fields(); ++i) {
      auto transformation = indexed_transformation::function_type{
        [&](struct record_type::field field,
            std::shared_ptr<arrow::Array>) noexcept {
          auto replacement = std::make_shared<arrow::StructArray>(
            arrow::struct_({sa.struct_type()->field(i)}), sa.length(),
            std::vector{sa.field(i)}, sa.null_bitmap(), sa.null_count(),
            sa.offset());
          auto replacement_type = type{record_type{{rt->field(i)}}};
          replacement_type.assign_metadata(field.type);
          field.type = std::move(replacement_type);
          return indexed_transformation::result_type{
            {std::move(field), std::move(replacement)},
          };
        }};
      auto transformations = std::vector<indexed_transformation>{};
      transformations.emplace_back(offset, std::move(transformation));
      transformed_slices.push_back(
        transform_columns(slice, std::move(transformations)));
    }
    if (unordered) {
      auto mask_builder = arrow::BooleanBuilder{arrow_memory_pool()};
      check(mask_builder.Reserve(resolved.second->length()));
      for (auto i = int64_t{}; i < resolved.second->length(); ++i) {
        check(mask_builder.Append(resolved.second->IsValid(i)));
      }
      auto mask = finish(mask_builder);
      for (const auto& transformed_slice : transformed_slices) {
        auto filtered = filter(transformed_slice, *mask);
        if (filtered.rows() > 0) {
          co_yield std::move(filtered);
        }
      }
      co_return;
    }
    for (auto i = int64_t{}; i < resolved.second->length(); ++i) {
      if (resolved.second->IsNull(i)) {
        continue;
      }
      for (const auto& transformed_slice : transformed_slices) {
        co_yield subslice(transformed_slice, i, i + 1);
      }
    }
    co_return;
  }
  auto list_array = dynamic_cast<arrow::ListArray*>(&*resolved.second);
  TENZIR_ASSERT(list_array);
  auto result_ty = unroll_type(slice.schema(), offset);
  auto builder = make_unroll_builder(result_ty);
  auto source = to_record_batch(slice)->ToStructArray();
  if (not source.ok()) {
    emit_unroll_error(dh, source.status());
    co_return;
  }
  TENZIR_ASSERT(*source);
  auto builder_bytes = uint64_t{};
  auto emitted = false;
  for (auto row = int64_t{0}; row < list_array->length(); ++row) {
    if (list_array->IsNull(row)) {
      continue;
    }
    auto begin = int64_t{list_array->value_offset(row)};
    auto end = int64_t{list_array->value_offset(row + 1)};
    TENZIR_ASSERT(begin <= end);
    if (begin == end) {
      continue;
    }
    auto remaining = end - begin;
    auto current = begin;
    // This deliberately overestimates when the unrolled field is a list with
    // many values, because the input row still includes values that are spread
    // across multiple output rows. The estimate keeps flushing conservative.
    auto row_bytes
      = std::max<uint64_t>(subslice(slice, row, row + 1).approx_bytes(), 1);
    while (remaining > 0) {
      if (builder->length() >= max_unroll_slice_rows
          or (builder->length() > 0
              and builder_bytes >= max_unroll_slice_bytes)) {
        auto result = finish_unroll_builder(*builder, result_ty, dh);
        if (result.failed) {
          co_return;
        }
        if (result.slice) {
          co_yield std::move(*result.slice);
          emitted = true;
        }
        builder = make_unroll_builder(result_ty);
        builder_bytes = 0;
      }
      auto rows_left = max_unroll_slice_rows - builder->length();
      TENZIR_ASSERT(rows_left > 0);
      auto bytes_left = builder_bytes >= max_unroll_slice_bytes
                          ? uint64_t{0}
                          : max_unroll_slice_bytes - builder_bytes;
      auto rows_left_by_byte_budget
        = row_bytes == 0 ? max_unroll_slice_rows
                         : detail::narrow<int64_t>(bytes_left / row_bytes);
      if (rows_left_by_byte_budget == 0) {
        rows_left_by_byte_budget
          = builder->length() == 0 ? int64_t{1} : int64_t{0};
      }
      if (rows_left_by_byte_budget == 0) {
        auto result = finish_unroll_builder(*builder, result_ty, dh);
        if (result.failed) {
          co_return;
        }
        if (result.slice) {
          co_yield std::move(*result.slice);
          emitted = true;
        }
        builder = make_unroll_builder(result_ty);
        builder_bytes = 0;
        continue;
      }
      auto rows_to_append
        = std::min({remaining, rows_left, rows_left_by_byte_budget});
      auto status
        = unroller{offset, *list_array, row, current, rows_to_append}.run(
          *builder, **source, as<record_type>(slice.schema()));
      if (not status.ok()) {
        emit_unroll_error(dh, status);
        co_return;
      }
      builder_bytes = add_saturated(builder_bytes, row_bytes, rows_to_append);
      remaining -= rows_to_append;
      current += rows_to_append;
    }
  }
  auto result = finish_unroll_builder(*builder, result_ty, dh, not emitted);
  if (result.failed) {
    co_return;
  }
  if (result.slice) {
    co_yield std::move(*result.slice);
  }
}

class unroll_operator final : public crtp_operator<unroll_operator> {
public:
  unroll_operator() = default;

  explicit unroll_operator(ast::field_path field) : field_{std::move(field)} {
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
          if (offsets.front().empty()) {
            return offsets.front();
          }
          const auto& field_type
            = as<record_type>(slice.schema()).field(offsets.front()).type;
          if (is<null_type>(field_type)) {
            return {};
          }
          if (not is<list_type>(field_type)) {
            diagnostic::warning("expected `list`, but got `{}`",
                                field_type.kind())
              .primary(field)
              .emit(ctrl.diagnostics());
            return {};
          }
          return offsets.front();
        };
      },
      [&](const ast::field_path& field) {
        return [&](const table_slice& slice) {
          return resolve(field, slice.schema())
            .match(
              [&](offset result) -> std::optional<offset> {
                if (result.empty()) {
                  return result;
                }
                const auto& field_type
                  = as<record_type>(slice.schema()).field(result).type;
                if (is<null_type>(field_type)) {
                  return {};
                }
                if (not is<list_type>(field_type)
                    and not is<record_type>(field_type)) {
                  diagnostic::warning("expected `list` or `record`, but got "
                                      "`{}`",
                                      field_type.kind())
                    .primary(field)
                    .emit(ctrl.diagnostics());
                  return {};
                }
                return result;
              },
              [&](const resolve_error& err) -> std::optional<offset> {
                err.reason.match(
                  [&](const resolve_error::field_not_found&) {
                    diagnostic::warning("field `{}` not found", err.ident.name)
                      .primary(err.ident)
                      .emit(ctrl.diagnostics());
                  },
                  [&](const resolve_error::field_not_found_no_error&) {},
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
      for (auto unrolled :
           unroll(slice, *offset, unordered_, ctrl.diagnostics())) {
        co_yield std::move(unrolled);
      }
    }
  }

  auto name() const -> std::string override {
    return "unroll";
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    auto replacement = std::make_unique<unroll_operator>(*this);
    replacement->unordered_ = order == event_order::unordered;
    return optimize_result{std::nullopt, order, std::move(replacement)};
  }

  friend auto inspect(auto& f, unroll_operator& x) -> bool {
    return f.object(x).fields(f.field("field", x.field_),
                              f.field("unordered", x.unordered_));
  }

private:
  variant<ast::field_path, located<std::string>> field_;
  bool unordered_ = {};
};

struct UnrollArgs {
  ast::field_path field;
  event_order order = event_order::ordered;
};

class Unroll final : public Operator<table_slice, table_slice> {
public:
  explicit Unroll(UnrollArgs args) : args_{std::move(args)} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (input.rows() == 0) {
      co_return;
    }
    const auto get_offset
      = [&](const table_slice& slice) -> std::optional<offset> {
      return resolve(args_.field, slice.schema())
        .match(
          [&](offset result) -> std::optional<offset> {
            if (result.empty()) {
              return result;
            }
            const auto& field_type
              = as<record_type>(slice.schema()).field(result).type;
            if (is<null_type>(field_type)) {
              return {};
            }
            if (not is<list_type>(field_type)
                and not is<record_type>(field_type)) {
              diagnostic::warning("expected `list` or `record`, but got `{}`",
                                  field_type.kind())
                .primary(args_.field)
                .emit(ctx);
              return {};
            }
            return result;
          },
          [&](const resolve_error& err) -> std::optional<offset> {
            err.reason.match(
              [&](const resolve_error::field_not_found&) {
                diagnostic::warning("field `{}` not found", err.ident.name)
                  .primary(err.ident)
                  .emit(ctx);
              },
              [&](const resolve_error::field_not_found_no_error&) {},
              [&](const resolve_error::field_of_non_record& reason) {
                diagnostic::warning("type `{}` has no field `{}`",
                                    reason.type.kind(), err.ident.name)
                  .primary(err.ident)
                  .emit(ctx);
              });
            return {};
          });
    };
    const auto off = get_offset(input);
    if (not off) {
      co_return;
    }
    for (auto unrolled :
         unroll(input, *off, args_.order == event_order::unordered, ctx)) {
      co_await push(std::move(unrolled));
    }
  }

private:
  UnrollArgs args_;
};

class plugin final : public virtual operator_plugin<unroll_operator>,
                     public virtual operator_factory_plugin,
                     public virtual OperatorPlugin {
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

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto field = ast::field_path{};
    auto parser
      = argument_parser2::operator_(name()).positional("field", field, "list");
    TRY(parser.parse(inv, ctx));
    return std::make_unique<unroll_operator>(std::move(field));
  }

  auto describe() const -> Description override {
    auto d = Describer<UnrollArgs, Unroll>{};
    d.positional("field", &UnrollArgs::field);
    d.optimization_order(&UnrollArgs::order);
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::unroll

TENZIR_REGISTER_PLUGIN(tenzir::plugins::unroll::plugin)
