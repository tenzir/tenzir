//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arc.hpp>
#include <tenzir/argument_parser2.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/bitmap.hpp>
#include <tenzir/collect.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/nova/array_builder.hpp>
#include <tenzir/nova/eval_util.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/try.hpp>

#include <algorithm>
#include <array>
#include <limits>
#include <span>
#include <type_traits>

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
          auto replacement_fields = std::array{
            series_field{rt->field(i).name,
                         {rt->field(i).type, sa.field(detail::narrow<int>(i))}},
          };
          auto replacement = make_record_series(replacement_fields, sa);
          auto replacement_type = type{replacement.type};
          replacement_type.assign_metadata(field.type);
          field.type = std::move(replacement_type);
          return indexed_transformation::result_type{
            {std::move(field), std::move(replacement.array)},
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

struct UnrollArgs {
  ast::field_path field;
  OptimizationArgs<opt::Order> optimization;
};

class Unroll final : public Operator<table_slice, table_slice> {
public:
  explicit Unroll(UnrollArgs args) : args_{std::move(args)} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    const auto get_offset = [&](const table_slice& slice) -> Option<offset> {
      return resolve(args_.field, slice.schema())
        .match(
          [&](offset result) -> Option<offset> {
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
          [&](const resolve_error& err) -> Option<offset> {
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
         unroll(input, *off, args_.optimization.order == EventOrder::unordered,
                ctx)) {
      co_await push(std::move(unrolled));
    }
  }

private:
  UnrollArgs args_;
};

auto approximate_row_bytes(nova::RowView<nova::Data> row) -> uint64_t {
  return match(row, []<nova::data_type Tag>(nova::RowView<Tag> view) {
    if constexpr (std::same_as<Tag, nova::String>
                  or std::same_as<Tag, nova::Blob>) {
      return detail::narrow<uint64_t>((*view).size());
    } else if constexpr (std::same_as<Tag, nova::List>) {
      auto result = uint64_t{};
      for (auto element : view) {
        result = add_saturated(result, approximate_row_bytes(element), 1);
      }
      return result;
    } else if constexpr (std::same_as<Tag, nova::Record>) {
      auto result = uint64_t{};
      for (auto [name, value] : view) {
        result = add_saturated(result, name.size(), 1);
        result = add_saturated(result, approximate_row_bytes(value), 1);
      }
      return result;
    } else {
      return uint64_t{sizeof(*view)};
    }
  });
}

auto append_unrolled_record(
  nova::ArrayBuilder<nova::Record>::RecordBuilder destination,
  nova::RowView<nova::Record> source,
  std::span<ast::field_path::segment const> path,
  nova::RowView<nova::Data> replacement) -> void {
  TENZIR_ASSERT(not path.empty());
  for (auto [name, value] : source) {
    if (name != path.front().id.name) {
      nova::append_row(destination.field(name), value);
      continue;
    }
    if (path.size() == 1) {
      nova::append_row(destination.field(name), replacement);
      continue;
    }
    match(value, [&](auto view) {
      using T = std::remove_cvref_t<decltype(view)>;
      if constexpr (std::same_as<T, nova::RowView<nova::Record>>) {
        append_unrolled_record(destination.field(name).record(), view,
                               path.subspan(1), replacement);
      } else {
        nova::append_row(destination.field(name), value);
      }
    });
  }
}

auto append_unrolled_record_field(
  nova::ArrayBuilder<nova::Record>::RecordBuilder destination,
  nova::RowView<nova::Record> source,
  std::span<ast::field_path::segment const> path, std::string_view name,
  nova::RowView<nova::Data> value) -> void {
  if (path.empty()) {
    nova::append_row(destination.field(name), value);
    return;
  }
  for (auto [field_name, field_value] : source) {
    if (field_name != path.front().id.name) {
      nova::append_row(destination.field(field_name), field_value);
      continue;
    }
    if (path.size() == 1) {
      nova::append_row(destination.field(field_name).record().field(name),
                       value);
      continue;
    }
    match(field_value, [&](auto view) {
      using T = std::remove_cvref_t<decltype(view)>;
      if constexpr (std::same_as<T, nova::RowView<nova::Record>>) {
        append_unrolled_record_field(destination.field(field_name).record(),
                                     view, path.subspan(1), name, value);
      } else {
        nova::append_row(destination.field(field_name), field_value);
      }
    });
  }
}

class UnrollNova final : public Operator<nova::Events, nova::Events> {
public:
  explicit UnrollNova(UnrollArgs args) : args_{std::move(args)} {
  }

  auto process(nova::Events input, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    auto data = nova::ArrayBuilder<nova::Record>{};
    auto names = nova::ArrayBuilder<nova::String>{};
    auto import_times = nova::ArrayBuilder<nova::Time>{};
    auto internals = nova::ArrayBuilder<nova::Bool>{};
    auto path = args_.field.path();
    auto builder_bytes = uint64_t{};
    auto wrong_type = Option<std::string_view>{};
    auto missing_field = Option<ast::field_path::segment const&>{};
    auto non_record_type = Option<std::string_view>{};
    const auto flush = [&]() -> Task<void> {
      if (data.length() == 0) {
        co_return;
      }
      auto length = data.length();
      co_await push(
        nova::Events{data.finish(), nova::storage::BitMap{length, true},
                     nova::Events::Meta{names.finish(), import_times.finish(),
                                        internals.finish()}});
      data = nova::ArrayBuilder<nova::Record>{};
      names = nova::ArrayBuilder<nova::String>{};
      import_times = nova::ArrayBuilder<nova::Time>{};
      internals = nova::ArrayBuilder<nova::Bool>{};
      builder_bytes = 0;
    };
    const auto append_meta = [&](nova::storage::Index row) {
      names.data(*input.meta.name.get(row));
      import_times.data(*input.meta.import_time.get(row));
      internals.data(*input.meta.internal.get(row));
    };
    const auto flush_before = [&](uint64_t row_bytes) -> Task<void> {
      if (data.length() > 0
          and (builder_bytes >= max_unroll_slice_bytes
               or row_bytes > max_unroll_slice_bytes - builder_bytes)) {
        co_await flush();
      }
    };
    const auto append
      = [&](nova::RowView<nova::Record> source, nova::storage::Index row,
            nova::RowView<nova::Data> replacement,
            uint64_t row_bytes) -> Task<void> {
      co_await flush_before(row_bytes);
      append_unrolled_record(data.record(), source, path, replacement);
      append_meta(row);
      builder_bytes = add_saturated(builder_bytes, row_bytes, 1);
      if (data.length() >= max_unroll_slice_rows
          or builder_bytes >= max_unroll_slice_bytes) {
        co_await flush();
      }
    };
    const auto append_field
      = [&](nova::RowView<nova::Record> source, nova::storage::Index row,
            std::string_view name, nova::RowView<nova::Data> value,
            uint64_t row_bytes) -> Task<void> {
      co_await flush_before(row_bytes);
      append_unrolled_record_field(data.record(), source, path, name, value);
      append_meta(row);
      builder_bytes = add_saturated(builder_bytes, row_bytes, 1);
      if (data.length() >= max_unroll_slice_rows
          or builder_bytes >= max_unroll_slice_bytes) {
        co_await flush();
      }
    };
    for (auto row = nova::storage::Index{0}; row < input.length(); ++row) {
      if (not input.mask.get(row)) {
        continue;
      }
      auto source = input.data.get(row);
      auto target = Option<nova::RowView<nova::Data>>{};
      if (path.empty()) {
        target.emplace(source);
      } else {
        auto lookup = nova::lookup_field_path(source, path);
        target = std::move(lookup.value);
        if (not target) {
          auto const& failed = path[lookup.matched_segments];
          if (lookup.non_record_type or not failed.has_question_mark) {
            missing_field.emplace(failed);
            non_record_type = lookup.non_record_type;
          }
        }
      }
      if (not target) {
        continue;
      }
      auto row_bytes = approximate_row_bytes(nova::RowView<nova::Data>{source});
      row_bytes = add_saturated(
        row_bytes, detail::narrow<uint64_t>((*input.meta.name.get(row)).size()),
        1);
      row_bytes = add_saturated(row_bytes, sizeof(nova::Time), 1);
      row_bytes = add_saturated(row_bytes, sizeof(nova::Bool), 1);
      row_bytes = std::max(row_bytes, uint64_t{1});
      co_await match(*target, [&](auto view) -> Task<void> {
        using T = std::remove_cvref_t<decltype(view)>;
        if constexpr (std::same_as<T, nova::RowView<nova::List>>) {
          for (auto element : view) {
            co_await append(source, row, element, row_bytes);
          }
        } else if constexpr (std::same_as<T, nova::RowView<nova::Record>>) {
          for (auto [name, value] : view) {
            co_await append_field(source, row, name, value, row_bytes);
          }
        } else if constexpr (not std::same_as<T, nova::RowView<nova::Null>>) {
          wrong_type = []<nova::data_type Tag>(nova::RowView<Tag>) {
            return nova::Type<Tag>::static_name;
          }(view);
        }
      });
    }
    if (missing_field) {
      if (non_record_type) {
        diagnostic::warning("type `{}` has no field `{}`", *non_record_type,
                            missing_field->id.name)
          .primary(missing_field->id)
          .emit(ctx);
      } else {
        diagnostic::warning("field `{}` not found", missing_field->id.name)
          .primary(missing_field->id)
          .emit(ctx);
      }
    }
    if (wrong_type) {
      diagnostic::warning("expected `list` or `record`, but got `{}`",
                          *wrong_type)
        .primary(args_.field)
        .emit(ctx);
    }
    co_await flush();
  }

private:
  UnrollArgs args_;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "unroll";
  }

  auto describe() const -> Description override {
    auto d = Describer<UnrollArgs, Unroll, UnrollNova>{};
    d.parallelizable();
    d.positional("field", &UnrollArgs::field);
    d.optimization(&UnrollArgs::optimization);
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::unroll

TENZIR_REGISTER_PLUGIN(tenzir::plugins::unroll::plugin)
