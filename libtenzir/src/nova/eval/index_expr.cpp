#include "tenzir/detail/heterogeneous_string_hash.hpp"
#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/data_array_builder.hpp"
#include "tenzir/nova/eval_internal.hpp"
#include "tenzir/nova/union_array.hpp"
#include "tenzir/tql2/ast.hpp"

#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace tenzir::nova {

namespace {

struct IndexDiagnostics {
  bool out_of_bounds = false;
  bool record_out_of_bounds = false;
  bool null_subject = false;
  bool null_index = false;
  Option<std::string_view> bad_subject;
  bool bad_index = false;
  Option<std::string> missing_field;

  auto emit(ast::index_expr const& x, EvalFrame const& frame) const -> void {
    if (not x.has_question_mark) {
      auto const hint = x.is_get
                          ? "provide a fallback value to suppress this warning"
                          : "use `[…]?` to suppress this warning";
      if (out_of_bounds) {
        diagnostic::warning("list index out of bounds")
          .primary(x.index, "is out of bounds")
          .hint(hint)
          .emit(frame);
      }
      if (record_out_of_bounds) {
        diagnostic::warning("index out of bounds")
          .primary(x.index, "is out of bounds")
          .hint(hint)
          .emit(frame);
      }
      if (missing_field) {
        diagnostic::warning("record has no field `{}`", *missing_field)
          .primary(x)
          .hint(hint)
          .emit(frame);
      }
      if (null_subject) {
        diagnostic::warning("cannot index into `null`")
          .primary(x.expr, "is null")
          .hint(hint)
          .emit(frame);
      }
      if (null_index) {
        diagnostic::warning("cannot use `null` as index")
          .primary(x.index, "is null")
          .hint(hint)
          .emit(frame);
      }
    }
    if (bad_subject) {
      diagnostic::warning("expected `record` or `list`")
        .primary(x.expr, "has type `{}`", *bad_subject)
        .emit(frame);
    }
    if (bad_index) {
      diagnostic::warning("cannot use this value as index")
        .primary(x.index)
        .emit(frame);
    }
  }
};

/// Returns the alternative only when every active row holds that type or null.
/// Other alternatives outside the active mask do not prevent specialization.
template <class Tag>
auto single_type(Array<Data> const& array, storage::BitMap const& mask)
  -> Option<MaskedArray<Array<Tag>>> {
  auto result = array.get_alternative<Tag>();
  if (not result) {
    return None{};
  }
  auto allowed = result->present;
  if (auto nulls = array.get_alternative<Null>()) {
    allowed = std::move(allowed) | nulls->present;
  }
  if (mask.and_not(allowed).any()) {
    return None{};
  }
  return result;
}

template <class Tag>
auto constant_value(Array<Data> const& array)
  -> Option<typename Type<Tag>::ViewType> {
  auto typed = array.try_as<Tag>();
  if (not typed) {
    return None{};
  }
  using Constant = storage::ConstantStorage<typename Type<Tag>::DataType,
                                            typename Type<Tag>::ViewType>;
  if (auto const* value = try_as<Constant>(typed->storage())) {
    return value->get(0);
  }
  return None{};
}

/// A constant field name keeps the original column and only fills missing or
/// null-subject rows. No per-row materialization is needed.
auto eval_constant_field(MaskedArray<Array<Record>> const& records,
                         std::string_view name, ast::index_expr const& x,
                         EvalFrame const& frame) -> Array<Data> {
  auto diagnostics = IndexDiagnostics{};
  auto const records_mask = frame.mask() & records.present;
  diagnostics.null_subject = frame.mask().and_not(records.present).any();
  auto field = records.data.field(name);
  if (not field) {
    if (records_mask.any()) {
      diagnostics.missing_field = std::string{name};
    }
    diagnostics.emit(x, frame);
    return frame.null();
  }
  field->present = std::move(field->present) & records.present;
  if (records_mask.and_not(field->present).any()) {
    diagnostics.missing_field = std::string{name};
  }
  diagnostics.emit(x, frame);
  return std::move(field->data).null_where(frame.mask().and_not(field->present));
}

/// Caches column lookup for dynamic string indices into record-only subjects.
auto eval_record_index(MaskedArray<Array<Record>> const& records,
                       MaskedArray<Array<String>> const& indices,
                       ast::index_expr const& x, EvalFrame const& frame)
  -> Array<Data> {
  auto builder = ArrayBuilder<Data>{};
  auto diagnostics = IndexDiagnostics{};
  auto cache = std::unordered_map<std::string, Option<MaskedArray<Array<Data>>>,
                                  detail::heterogeneous_string_hash,
                                  detail::heterogeneous_string_equal>{};
  for (auto row = storage::Index{0}; row < frame.length(); ++row) {
    if (not frame.mask().get(row)) {
      builder.null();
      continue;
    }
    if (not records.present.get(row)) {
      diagnostics.null_subject = true;
      builder.null();
      continue;
    }
    if (not indices.present.get(row)) {
      diagnostics.null_index = true;
      builder.null();
      continue;
    }
    auto const name = *indices.data.get(row);
    auto it = cache.find(name);
    if (it == cache.end()) {
      it = cache.emplace(std::string{name}, records.data.field(name)).first;
    }
    auto const& field = it->second;
    if (not field or not field->present.get(row)) {
      diagnostics.missing_field = std::string{name};
      builder.null();
    } else {
      append_row(builder, field->data.get(row));
    }
  }
  diagnostics.emit(x, frame);
  return builder.finish();
}

/// Resolves an index into a list of `length` elements, where negative indices
/// count from the end.
auto resolve_list_index(Option<int64_t> signed_index,
                        Option<uint64_t> unsigned_index, int64_t length)
  -> Option<int64_t> {
  if (signed_index) {
    auto index = *signed_index < 0 ? length + *signed_index : *signed_index;
    if (index < 0 or index >= length) {
      return None{};
    }
    return index;
  }
  TENZIR_ASSERT(unsigned_index);
  if (*unsigned_index >= static_cast<uint64_t>(length)) {
    return None{};
  }
  return static_cast<int64_t>(*unsigned_index);
}

/// A constant integer over list-only subjects needs no per-row type dispatch.
auto eval_list_index(MaskedArray<Array<List>> const& lists,
                     Option<int64_t> signed_index,
                     Option<uint64_t> unsigned_index, ast::index_expr const& x,
                     EvalFrame const& frame) -> Array<Data> {
  auto builder = ArrayBuilder<Data>{};
  auto diagnostics = IndexDiagnostics{};
  for (auto row = storage::Index{0}; row < frame.length(); ++row) {
    if (not frame.mask().get(row)) {
      builder.null();
      continue;
    }
    if (not lists.present.get(row)) {
      diagnostics.null_subject = true;
      builder.null();
      continue;
    }
    auto const list = lists.data.get(row);
    auto const position
      = resolve_list_index(signed_index, unsigned_index, list.length());
    if (not position) {
      diagnostics.out_of_bounds = true;
      builder.null();
    } else {
      append_row(builder, list.get(*position));
    }
  }
  diagnostics.emit(x, frame);
  return builder.finish();
}

/// Handles every mixed case in one row-wise pass. Integer access into records
/// always comes here so positions follow each row's own field order.
auto eval_index_rows(ast::index_expr const& x, Array<Data> const& subject,
                     Array<Data> const& index, EvalFrame const& frame)
  -> Array<Data> {
  auto builder = ArrayBuilder<Data>{};
  auto diagnostics = IndexDiagnostics{};
  for (auto row = storage::Index{0}; row < frame.length(); ++row) {
    if (not frame.mask().get(row)) {
      builder.null();
      continue;
    }
    auto signed_index = Option<int64_t>{};
    auto unsigned_index = Option<uint64_t>{};
    auto name = Option<std::string_view>{};
    auto index_is_null = false;
    match(
      index.get(row),
      [&](RowView<Int> value) {
        signed_index = *value;
      },
      [&](RowView<UInt> value) {
        unsigned_index = *value;
      },
      [&](RowView<String> value) {
        name = *value;
      },
      [&](RowView<Null>) {
        index_is_null = true;
      },
      [&](auto const&) {
        diagnostics.bad_index = true;
      });
    auto const has_number = signed_index or unsigned_index;
    match(
      subject.get(row),
      [&](RowView<List> const& list) {
        if (index_is_null) {
          diagnostics.null_index = true;
          builder.null();
          return;
        }
        if (not has_number) {
          diagnostics.bad_index = diagnostics.bad_index or name.is_some();
          builder.null();
          return;
        }
        auto const position
          = resolve_list_index(signed_index, unsigned_index, list.length());
        if (not position) {
          diagnostics.out_of_bounds = true;
          builder.null();
          return;
        }
        append_row(builder, list.get(*position));
      },
      [&](RowView<Record> const& record) {
        if (index_is_null) {
          diagnostics.null_index = true;
          builder.null();
          return;
        }
        auto target = Option<RowView<Data>>{};
        auto position = int64_t{0};
        for (auto const& [field_name, value] : record) {
          if (name ? field_name == *name
                   : (signed_index
                        ? *signed_index >= 0 and position == *signed_index
                        : unsigned_index
                            and static_cast<uint64_t>(position)
                                  == *unsigned_index)) {
            target = value;
            break;
          }
          ++position;
        }
        if (not target) {
          if (name) {
            diagnostics.missing_field = std::string{*name};
          } else if (has_number) {
            diagnostics.record_out_of_bounds = true;
          }
          builder.null();
          return;
        }
        append_row(builder, *target);
      },
      [&](RowView<Null>) {
        diagnostics.null_subject = true;
        builder.null();
      },
      [&](auto const& value) {
        diagnostics.bad_subject
          = match(RowView<Data>{value}, []<class T>(RowView<T> const&) {
              return Type<T>::static_name;
            });
        builder.null();
      });
  }
  diagnostics.emit(x, frame);
  return builder.finish();
}

} // namespace

auto _::EvalRun::eval(ast::index_expr const& x, EvalFrame frame)
  -> Array<Data> {
  auto const& mask = frame.mask();
  if (not mask.any()) {
    return frame.null();
  }
  auto subject = frame.eval(x.expr);
  auto index = frame.eval(x.index);
  if (auto records = single_type<Record>(subject, mask)) {
    if (auto name = constant_value<String>(index)) {
      return eval_constant_field(*records, *name, x, frame);
    }
    if (auto strings = single_type<String>(index, mask)) {
      return eval_record_index(*records, *strings, x, frame);
    }
  }
  if (auto lists = single_type<List>(subject, mask)) {
    if (auto value = constant_value<Int>(index)) {
      return eval_list_index(*lists, *value, None{}, x, frame);
    }
    if (auto value = constant_value<UInt>(index)) {
      return eval_list_index(*lists, None{}, *value, x, frame);
    }
  }
  return eval_index_rows(x, subject, index, frame);
}

} // namespace tenzir::nova
