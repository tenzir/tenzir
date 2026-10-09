//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// The Luhn checksum (ISO/IEC 7812-1), which validates card numbers and other
// identification numbers:
//
// - `is_luhn_valid` tests whether a string of digits has a valid checksum.
// - `luhn_check_digit` computes the digit that makes a string of digits valid.

#include <tenzir/nova/eval_kernel.hpp>
#include <tenzir/nova/function_plugin.hpp>
#include <tenzir/nova/type_system.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <array>
#include <concepts>
#include <cstdint>
#include <string>
#include <string_view>

namespace tenzir::plugins::luhn {

namespace {

/// The Luhn sum of `digits`, which counts every other digit from the right
/// twice, subtracting 9 from doubled values above 9. The rightmost digit
/// counts twice iff `double_rightmost`. Returns `None` if `digits` is empty or
/// contains anything but the ASCII digits `0` to `9`. Strings hold at most
/// 2^31 bytes and each digit adds at most 9, so the sum cannot overflow.
auto luhn_sum(std::string_view digits, bool double_rightmost)
  -> Option<uint64_t> {
  if (digits.empty()) {
    return None{};
  }
  static constexpr auto weighted = std::array<std::array<uint8_t, 10>, 2>{{
    {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
    {0, 2, 4, 6, 8, 1, 3, 5, 7, 9},
  }};
  auto sum = uint64_t{0};
  auto doubled = double_rightmost ? size_t{1} : size_t{0};
  for (auto it = digits.rbegin(); it != digits.rend(); ++it) {
    // Bytes below `0` wrap around, so one comparison rejects every non-digit.
    auto const digit = static_cast<uint8_t>(static_cast<uint8_t>(*it) - '0');
    if (digit > 9) {
      return None{};
    }
    sum += weighted[doubled][digit];
    doubled ^= 1;
  }
  return sum;
}

struct LuhnArgs {
  nova::ValueArgument x;
  location call;
};

/// A predicate, so input that is not a string of digits is simply invalid.
class IsLuhnValidFunction {
public:
  static constexpr auto name = std::string_view{"is_luhn_valid"};

  static auto eval(LuhnArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    return nova::apply_kernel<1>(
      frame, name, std::array<nova::ValueArgument, 1>{args.x}, args.call,
      []<std::same_as<std::string_view> X>(diagnostic_handler&,
                                           X input) -> Option<nova::Bool> {
        auto const sum = luhn_sum(input, false);
        return sum and *sum % 10 == 0;
      });
  }
};

/// A calculator, so input that is not a string of digits has no result and
/// warns.
class LuhnCheckDigitFunction {
public:
  static constexpr auto name = std::string_view{"luhn_check_digit"};

  static auto eval(LuhnArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    // Rows only record what went wrong, so that invalid rows do not build
    // diagnostics that would be discarded after the first one.
    auto saw_empty = false;
    auto saw_non_digit = false;
    auto result = nova::apply_kernel<1>(
      frame, name, std::array<nova::ValueArgument, 1>{args.x}, args.call,
      [&]<std::same_as<std::string_view> X>(diagnostic_handler&,
                                            X input) -> Option<nova::Int> {
        // The check digit becomes the rightmost digit, so the rightmost digit
        // of the input counts twice.
        auto const sum = luhn_sum(input, true);
        if (sum) {
          return static_cast<int64_t>((10 - *sum % 10) % 10);
        }
        (input.empty() ? saw_empty : saw_non_digit) = true;
        return None{};
      });
    if (saw_empty) {
      diagnostic::warning("cannot compute the check digit of an empty string")
        .primary(args.x.source)
        .emit(frame);
    }
    if (saw_non_digit) {
      diagnostic::warning("expected only the digits `0` to `9`")
        .primary(args.x.source)
        .hint("remove separators such as spaces and dashes first")
        .emit(frame);
    }
    return result;
  }
};

template <class Function>
class LuhnPlugin final : public nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return std::string{Function::name};
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<LuhnArgs, Function>{};
    d.positional("x", &LuhnArgs::x, "string");
    d.call_location(&LuhnArgs::call);
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    diagnostic::error("`{}` requires `--nova`", name())
      .primary(inv.call)
      .emit(ctx);
    return failure::promise();
  }
};

} // namespace

} // namespace tenzir::plugins::luhn

TENZIR_REGISTER_PLUGIN(
  tenzir::plugins::luhn::LuhnPlugin<tenzir::plugins::luhn::IsLuhnValidFunction>)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::luhn::LuhnPlugin<
                       tenzir::plugins::luhn::LuhnCheckDigitFunction>)
