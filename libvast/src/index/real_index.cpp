/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/index/real_index.hpp"

#include "vast/detail/overload.hpp"
#include "vast/die.hpp"
#include "vast/index/container_lookup.hpp"
#include "vast/type.hpp"

#include <caf/deserializer.hpp>
#include <caf/serializer.hpp>
#include <caf/settings.hpp>

#include <algorithm>
#include <cmath>

namespace vast {

real_index::real_index(vast::type t, uint8_t integral_precision,
                       uint8_t fractional_precision)
  : value_index{std::move(t), caf::settings{}},
    integral_precision_{integral_precision},
    fractional_precision_{fractional_precision},
    integral_{base::uniform(10, integral_precision_)},
    fractional_{base::uniform(10, fractional_precision_)} {
} // namespace vast

caf::error real_index::serialize(caf::serializer& sink) const {
  return caf::error::eval([&] { return value_index::serialize(sink); },
                          [&] {
                            return sink(integral_precision_,
                                        fractional_precision_, sign_, zero_,
                                        nan_, inf_, integral_, fractional_);
                          });
}

caf::error real_index::deserialize(caf::deserializer& source) {
  return caf::error::eval([&] { return value_index::deserialize(source); },
                          [&] {
                            return source(integral_precision_,
                                          fractional_precision_, sign_, zero_,
                                          nan_, inf_, integral_, fractional_);
                          });
}

std::pair<uint64_t, uint64_t> real_index::decompose(real x) const {
  double lhs;
  double rhs = std::modf(double{std::abs(x)}, &lhs);
  rhs *= std::pow(10, fractional_precision_);
  auto integral = static_cast<uint64_t>(lhs);
  auto fractional = static_cast<uint64_t>(std::round(rhs));
  // Clamp values to maximum precision.
  uint64_t integral_max = std::pow(10, integral_precision_);
  uint64_t fractional_max = std::pow(10, fractional_precision_);
  integral = std::min(integral, integral_max);
  fractional = std::min(fractional, fractional_max);
  return {integral, fractional};
}

bool real_index::append_impl(data_view x, id pos) {
  auto r = caf::get_if<view<real>>(&x);
  if (!r)
    return false;
  switch (std::fpclassify(*r)) {
    default:
      die("missing std::fpclassify() case");
    case FP_NAN: {
      nan_.append_bits(0, pos - nan_.size());
      nan_.append_bit(true);
      break;
    }
    case FP_ZERO: {
      // No signed zero, i.e., -0.0. and +0.0 are equal.
      zero_.append_bits(0, pos - zero_.size());
      zero_.append_bit(true);
      break;
    }
    case FP_INFINITE: {
      sign_.append_bits(0, pos - sign_.size());
      sign_.append_bit(std::signbit(*r));
      inf_.append_bits(0, pos - inf_.size());
      inf_.append_bit(true);
      break;
    }
    case FP_SUBNORMAL:
    case FP_NORMAL: {
      sign_.append_bits(0, pos - sign_.size());
      sign_.append_bit(std::signbit(*r));
      auto [integral, fractional] = decompose(*r);
      integral_.skip(pos - integral_.size());
      integral_.append(integral);
      fractional_.skip(pos - fractional_.size());
      fractional_.append(fractional);
      break;
    }
  }
  return true;
}

caf::expected<ids>
real_index::lookup_impl(relational_operator op, data_view d) const {
  auto f = detail::overload{
    [&](auto x) -> caf::expected<ids> {
      return make_error(ec::type_clash, materialize(x));
    },
    [&](view<real> x) -> caf::expected<ids> {
      if (op == in || op == not_in)
        return make_error(ec::unsupported_operator, op);
      auto sign
        = [&] { return std::signbit(x) ? sign_ : flip(sign_, offset()); };
      switch (std::fpclassify(x)) {
        default:
          die("missing std::fpclassify() case");
        case FP_NAN:
          switch (op) {
            default:
              return ec::unsupported_operator;
            case equal:
              return nan_;
            case not_equal:
              return flip(nan_, offset());
          }
        case FP_ZERO:
          switch (op) {
            default:
              return ec::unsupported_operator;
            case equal:
              return zero_;
            case not_equal:
              return flip(zero_, offset());
            case less:
              return sign_;
            case less_equal:
              return sign_ | zero_;
            case greater:
              return flip(sign_, offset()) - nan_ - zero_;
            case greater_equal:
              return (flip(sign_, offset()) - nan_) | zero_;
          }
        case FP_INFINITE:
          switch (op) {
            default:
              return ec::unsupported_operator;
            case equal:
              return inf_ & sign();
            case not_equal:
              return ~(inf_ & sign());
            case less:
              if (x < 0.0)
                return ids{};
              return (inf_ & sign_) | (flip(inf_, offset()) - nan_);
            case less_equal:
              return x < 0.0 ? inf_ & sign_ : flip(nan_, offset());
            case greater: {
              if (x > 0.0)
                return ids{};
              return (inf_ - sign_) | (flip(inf_, offset()) - nan_);
            }
            case greater_equal:
              return x < 0.0 ? flip(nan_, offset()) : inf_ - sign_;
          }
        case FP_SUBNORMAL:
        case FP_NORMAL: {
          auto [integral, fractional] = decompose(x);
          switch (op) {
            default:
              return ec::unsupported_operator;
            case equal: {
              auto result = sign();
              result &= integral_.lookup(equal, integral);
              result &= fractional_.lookup(equal, fractional);
              return result;
            }
            case not_equal: {
              auto result = sign();
              result &= integral_.lookup(equal, integral);
              result &= fractional_.lookup(equal, fractional);
              result = flip(result, offset());
              result |= zero_;
              result |= nan_;
              result |= inf_;
              return result;
            }
            case less:
            case less_equal: {
              // Example:
              // - x < 5.2   <=>  i < 5.0 || (i == 5.0 && f < 0.2)
              // - x <= 5.2  <=>  i < 5.0 || (i == 5.0 && f <= 0.2)
              auto result = sign();
              result &= integral_.lookup(equal, integral);
              result &= fractional_.lookup(op, fractional);
              if (integral == 0)
                result |= sign();
              else
                result |= integral_.lookup(less, integral);
              result |= inf_ & sign_;
              return result;
            }
            case greater:
            case greater_equal: {
              // Example:
              // - x > 5.2   <=>  i > 6.0 || (i == 5.0 && f > 0.2)
              // - x >= 5.2  <=>  i > 6.0 || (i == 5.0 && f >= 0.2)
              auto result = sign();
              result &= integral_.lookup(equal, integral);
              result &= fractional_.lookup(op, fractional);
              if (integral == 0)
                result |= ~sign();
              else
                result |= integral_.lookup(greater, integral + 1);
              result |= inf_ - sign_;
              return result;
            }
          }
        }
      }
      return make_error(ec::logic_error, "should not be reached");
    },
    [&](view<list> xs) { return detail::container_lookup(*this, op, xs); },
  };
  return caf::visit(f, d);
}

} // namespace vast
