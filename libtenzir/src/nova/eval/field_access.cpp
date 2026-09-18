#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/eval_internal.hpp"
#include "tenzir/nova/union_array.hpp"
#include "tenzir/tql2/ast.hpp"

namespace tenzir::nova {

auto _::EvalRun::eval(const ast::field_access& x, EvalFrame frame)
  -> Array<Data> {
  auto subject = frame.eval(x.left);
  const auto maybe_warn = [&x, &frame]() {
    if (not x.has_question_mark) {
      diagnostic::warning("record does not have field")
        .primary(x.left)
        .secondary(x.name, "field does not exist")
        .emit(frame);
    }
  };
  auto rec = subject.get_alternative<Record>();
  if (not rec) {
    maybe_warn();
    return frame.null();
  }
  auto const any_non_record = frame.mask().and_not(rec->present).any();
  if (any_non_record) {
    diagnostic::warning("cannot access field of non-record type")
      .primary(x.left, "not a record")
      .secondary(x.name)
      .emit(frame);
  }
  if (auto field = rec->data.field(x.name.name)) {
    field->present = std::move(field->present) & rec->present;
    auto needs_fill = frame.mask().and_not(field->present);
    if (needs_fill.any()) {
      maybe_warn();
    }
    return std::move(field->data).null_where(std::move(needs_fill));
  }
  maybe_warn();
  return frame.null();
}

} // namespace tenzir::nova
