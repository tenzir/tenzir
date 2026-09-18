#include "tenzir/detail/assert.hpp"
#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/eval_internal.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/tql2/ast.hpp"

namespace tenzir::nova {

auto _::EvalRun::eval(ast::function_call const& x, EvalFrame frame)
  -> Array<Data> {
  if (not frame.mask().any()) {
    return frame.null();
  }
  return evaluator_->call_site(x).eval(std::move(frame));
}

} // namespace tenzir::nova
