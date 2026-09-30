#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/eval_internal.hpp"
#include "tenzir/nova/field_suggestions.hpp"
#include "tenzir/nova/union_array.hpp"
#include "tenzir/tql2/ast.hpp"

namespace tenzir::nova {

auto _::EvalRun::eval(const ast::field_access& x, EvalFrame frame)
  -> Array<Data> {
  auto subject = frame.eval(x.left);
  auto rec = subject.get_alternative<Record>();
  const auto maybe_warn = [&x, &frame, &rec]() {
    if (not x.has_question_mark) {
      diagnostic::warning("record does not have field")
        .primary(x.left)
        .secondary(x.name, "field does not exist")
        .compose([&](auto builder) {
          auto suggestion
            = rec ? _::suggest_field_name(x.name.name, rec->data,
                                          frame.mask() & rec->present)
                  : None{};
          return suggestion
                   ? std::move(builder).hint("did you mean `{}`?", *suggestion)
                   : std::move(builder);
        })
        .hint("append `?` to suppress this warning")
        .emit(frame);
    }
  };
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
