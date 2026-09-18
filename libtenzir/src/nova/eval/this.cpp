#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/eval_internal.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/tql2/ast.hpp"

namespace tenzir::nova {

auto _::EvalRun::eval(const ast::this_& x, EvalFrame frame) -> Array<Data> {
  if (not input_) {
    return fail_not_constant(x.get_location(), std::move(frame));
  }
  // A frame's mask is always a subset of the input's, so every requested row
  // is a live input row and the record needs no backfill.
  return Array<Data>{input_->data};
}

} // namespace tenzir::nova
