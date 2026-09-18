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

auto append_field_at_row(diagnostic_handler& dh, ArrayBuilder<Data>& b,
                         const Array<Data>& field_data,
                         const storage::BitMap& field_present,
                         std::string_view name, storage::Index i,
                         const ast::expression& diag_expr,
                         bool suppress_warnings) -> void {
  if (not field_present.get(i)) {
    if (not suppress_warnings) {
      diagnostic::warning("record has no field `{}`", name)
        .primary(diag_expr)
        .emit(dh);
    }
    b.null();
    return;
  }
  append_row(b, field_data.get(i));
}

auto eval_record_index(diagnostic_handler& dh, const Array<Record>& record,
                       const Array<String>& index_strings,
                       storage::BitMap combined_mask,
                       const ast::expression& diag_expr, bool suppress_warnings)
  -> Array<Data> {
  auto builder = ArrayBuilder<Data>{};
  auto const length = record.length();
  auto cache
    = std::unordered_map<std::string, std::pair<Array<Data>, storage::BitMap>,
                         ::tenzir::detail::heterogeneous_string_hash,
                         ::tenzir::detail::heterogeneous_string_equal>{};
  for (auto i = storage::Index{0}; i < length; ++i) {
    if (not combined_mask.get(i)) {
      builder.null();
      continue;
    }
    auto name = *index_strings.get(i);
    auto it = cache.find(name);
    if (it == cache.end()) {
      auto field = record.field(name);
      auto entry
        = field
            ? std::pair{field->data.null_where(field->present.make_inverted()),
                        field->present}
            : std::pair{Array<Data>{Array<Null>{storage::NullStorage{length}}},
                        storage::BitMap{length, false}};
      it = cache.emplace(std::string{name}, std::move(entry)).first;
    }
    append_field_at_row(dh, builder, it->second.first, it->second.second,
                        it->first, i, diag_expr, suppress_warnings);
  }
  return Array<Data>{builder.finish()};
}

} // namespace

auto _::EvalRun::eval(const ast::index_expr& x, EvalFrame frame)
  -> Array<Data> {
  auto suppress_warnings = x.has_question_mark;
  if (auto const* index_constant = try_as<ast::constant>(x.index)) {
    if (auto const* name = try_as<std::string>(index_constant->value)) {
      auto field
        = ast::field_access{x.expr, x.expr.get_location(), x.has_question_mark,
                            ast::identifier{*name, x.index.get_location()}};
      return eval(field, std::move(frame));
    }
  }
  auto const& mask = frame.mask();
  auto subject = frame.eval(x.expr);
  return match(
    subject,
    [&](const Array<Record>& record) -> Array<Data> {
      auto index = frame.eval(x.index);
      auto index_strings = index.get_alternative<String>();
      if (not index_strings) {
        auto const length = index.length();
        auto null_alt = index.get_alternative<Null>();
        auto const is_null_mask
          = mask
            & (null_alt ? null_alt->present : storage::BitMap{length, false});
        if (is_null_mask.any() and not suppress_warnings) {
          diagnostic::warning("cannot use `null` as index")
            .primary(x.index)
            .emit(frame);
        }
        auto const bad_type_mask = mask.and_not(is_null_mask);
        if (bad_type_mask.any()) {
          diagnostic::warning(
            "cannot use a non-string value as index into `record`")
            .primary(x.index)
            .emit(frame);
        }
        return frame.null();
      }
      auto const combined_mask = mask & index_strings->present;
      return eval_record_index(frame, record, index_strings->data,
                               combined_mask, x, suppress_warnings);
    },
    [&](const UnionArray& u) -> Array<Data> {
      auto rec = u.get_alternative<Record>();
      if (mask.and_not(u.alternative_mask<Record>()).any()
          and not suppress_warnings) {
        diagnostic::warning("cannot access field of a non-record type")
          .primary(x.expr)
          .emit(frame);
      }
      if (not rec) {
        return frame.null();
      }
      auto const record_mask = mask & rec->present;
      auto index = frame.narrow(record_mask).eval(x.index);
      auto index_strings = index.get_alternative<String>();
      if (not index_strings) {
        if (record_mask.any() and not suppress_warnings) {
          diagnostic::warning("cannot use a non-string value as index into "
                              "`record`")
            .primary(x.index)
            .emit(frame);
        }
        return frame.null();
      }
      auto const combined_mask = record_mask & index_strings->present;
      return eval_record_index(frame, rec->data, index_strings->data,
                               combined_mask, x, suppress_warnings);
    },
    [&](const Array<Null>&) -> Array<Data> {
      if (not suppress_warnings) {
        diagnostic::warning("tried to access field of `null`")
          .primary(x)
          .emit(frame);
      }
      return frame.null();
    },
    [&](const auto&) -> Array<Data> {
      diagnostic::warning("cannot access field of a non-record type")
        .primary(x.expr)
        .emit(frame);
      return frame.null();
    });
}

} // namespace tenzir::nova
