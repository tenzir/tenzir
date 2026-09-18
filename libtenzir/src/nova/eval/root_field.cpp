#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/eval_internal.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/tql2/ast.hpp"

namespace tenzir::nova {

auto _::EvalRun::eval(const ast::root_field& x, EvalFrame frame)
  -> Array<Data> {
  const auto maybe_warn = [&x, &frame]() {
    if (not x.has_question_mark) {
      diagnostic::warning("event does not have field")
        .primary(x.get_location())
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
