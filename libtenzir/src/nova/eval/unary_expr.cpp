#include "tenzir/detail/overload.hpp"
#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/eval_internal.hpp"
#include "tenzir/nova/eval_kernel.hpp"
#include "tenzir/nova/type_system.hpp"
#include "tenzir/tql2/ast.hpp"

#include <limits>
#include <type_traits>
#include <utility>

namespace tenzir::nova {

// * _need_ to do type checking beforehand so we can reject entire arrays

auto _::EvalRun::eval(const ast::unary_expr& x, EvalFrame frame)
  -> Array<Data> {
  switch (x.op) {
    using enum ast::unary_op;
    case pos:
      return apply_kernel<1>(
        frame, "unary operator `+`", {x.expr}, x.get_location(),
        []<class T>(diagnostic_handler&, T v) -> Option<T>
          requires(std::same_as<T, Int> or std::same_as<T, UInt>
                   or std::same_as<T, Float>)
        {
          return +v;
        });
    case neg: {
      auto warn_int_overflow = WarnOnce{};
      auto warn_duration_overflow = WarnOnce{};
      return apply_kernel<1>(
        frame, "unary operator `-`", {x.expr}, x.get_location(),
        ::tenzir::detail::overload{
          [&x, &warn_int_overflow](diagnostic_handler& dh,
                                   Int v) -> Option<Int> {
            if (v == std::numeric_limits<Int>::min()) {
              warn_int_overflow(
                dh, diagnostic::warning("integer overflow").primary(x));
              return None{};
            }
            return -v;
          },
          [&x, &warn_int_overflow](diagnostic_handler& dh,
                                   UInt v) -> Option<Int> {
            constexpr auto limit
              = static_cast<UInt>(std::numeric_limits<Int>::max()) + 1;
            if (v > limit) {
              warn_int_overflow(
                dh, diagnostic::warning("integer overflow").primary(x));
              return None{};
            }
            if (v == limit) {
              return std::numeric_limits<Int>::min();
            }
            return -static_cast<Int>(v);
          },
          [](diagnostic_handler&, Float v) -> Option<Float> {
            return -v;
          },
          [&x, &warn_duration_overflow](diagnostic_handler& dh,
                                        Duration v) -> Option<Duration> {
            if (v.count() == std::numeric_limits<Duration::rep>::min()) {
              warn_duration_overflow(
                dh,
                diagnostic::warning("duration negation overflow").primary(x));
              return None{};
            }
            return Duration{-v.count()};
          },
        });
    }
    case not_:
      return apply_kernel<1>(
        frame, "unary operator `not`", {x.expr}, x.get_location(),
        // Constrained to exactly `Bool`: an unconstrained `Bool` parameter
        // would also accept `Int`/`UInt`/`Float` through implicit conversion
        // (see the note on `is_kernel_invocable_for`). `not null` is `null`,
        // silently, like in the legacy evaluator.
        ::tenzir::detail::overload{
          []<class T>(diagnostic_handler&, T v) -> Option<Bool>
            requires std::same_as<T, Bool>
          {
            return not v;
          },
          [](diagnostic_handler&, Null) -> Option<Bool> {
            return None{};
          },
          });
    case move:
      return frame.eval(x.expr);
  }
}

} // namespace tenzir::nova
