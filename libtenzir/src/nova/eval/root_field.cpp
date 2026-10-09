#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/eval_internal.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/field_suggestions.hpp"
#include "tenzir/tql2/ast.hpp"

namespace tenzir::nova {

auto _::EvalRun::eval(const ast::root_field& x, EvalFrame frame)
  -> Array<Data> {
  const auto maybe_warn = [this, &x, &frame]() {
    if (not x.has_question_mark and frame.mask().any()) {
      diagnostic::warning("event does not have field")
        .primary(x.get_location())
        .compose([&](auto builder) {
          auto suggestion
            = input_
                ? _::suggest_field_name(x.id.name, input_->data, frame.mask())
                : None{};
          return suggestion
                   ? std::move(builder).hint("did you mean `{}`?", *suggestion)
                   : std::move(builder);
        })
        .hint("append `?` to suppress this warning")
        .emit(frame);
    }
  };
  if (input_) {
    if (auto f = input_->data.field(x.id.name)) {
      // Only rows that are requested but absent from this row's shape need to
      // become `Null`; `null_where` leaves the array untouched when there are
      // none.
      auto needs_fill = frame.mask().and_not(f->present);
      if (needs_fill.any()) {
        maybe_warn();
      }
      return std::move(f->data).null_where(std::move(needs_fill));
    }
  }
  // Without input, only fields bound by an enclosing lambda exist. Anything
  // else is a reference to the (missing) input.
  if (not input_) {
    return fail_not_constant(x.get_location(), std::move(frame));
  }
  maybe_warn();
  return frame.null();
}

} // namespace tenzir::nova
