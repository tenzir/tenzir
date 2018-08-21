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

#pragma once

namespace vast {
namespace si_literals {

constexpr unsigned long long operator""_k(unsigned long long x) {
  return x * 1'000;
}

constexpr unsigned long long operator""_M(unsigned long long x) {
  return x * 1'000'000;
}

constexpr unsigned long long operator""_G(unsigned long long x) {
  return x * 1'000'000'000;
}

constexpr unsigned long long operator""_T(unsigned long long x) {
  return x * 1'000'000'000'000;
}

constexpr unsigned long long operator""_P(unsigned long long x) {
  return x * 1'000'000'000'000'000;
}

constexpr unsigned long long operator""_E(unsigned long long x) {
  return x * 1'000'000'000'000'000'000;
}

constexpr unsigned long long operator""_Ki(unsigned long long x) {
  return x << 10;
}

constexpr unsigned long long operator""_Mi(unsigned long long x) {
  return x << 20;
}

constexpr unsigned long long operator""_Gi(unsigned long long x) {
  return x << 30;
}

constexpr unsigned long long operator""_Ti(unsigned long long x) {
  return x << 40;
}

constexpr unsigned long long operator""_Pi(unsigned long long x) {
  return x << 50;
}

constexpr unsigned long long operator""_Ei(unsigned long long x) {
  return x << 60;
}

} // namespace si_literals

namespace decimal_byte_literals {

constexpr unsigned long long operator""_kB(unsigned long long x) {
  using namespace si_literals;
  return x * 1_k;
}

constexpr unsigned long long operator""_MB(unsigned long long x) {
  using namespace si_literals;
  return x * 1_M;
}

constexpr unsigned long long operator""_GB(unsigned long long x) {
  using namespace si_literals;
  return x * 1_G;
}

constexpr unsigned long long operator""_TB(unsigned long long x) {
  using namespace si_literals;
  return x * 1_T;
}

constexpr unsigned long long operator""_PB(unsigned long long x) {
  using namespace si_literals;
  return x * 1_P;
}

constexpr unsigned long long operator""_EB(unsigned long long x) {
  using namespace si_literals;
  return x * 1_E;
}

} // namespace decimal_byte_literals

namespace binary_byte_literals {

constexpr unsigned long long operator""_KiB(unsigned long long x) {
  using namespace si_literals;
  return x * 1_Ki;
}

constexpr unsigned long long operator""_MiB(unsigned long long x) {
  using namespace si_literals;
  return x * 1_Mi;
}

constexpr unsigned long long operator""_GiB(unsigned long long x) {
  using namespace si_literals;
  return x * 1_Gi;
}

constexpr unsigned long long operator""_TiB(unsigned long long x) {
  using namespace si_literals;
  return x * 1_Ti;
}

constexpr unsigned long long operator""_PiB(unsigned long long x) {
  using namespace si_literals;
  return x * 1_Pi;
}

constexpr unsigned long long operator""_EiB(unsigned long long x) {
  using namespace si_literals;
  return x * 1_Ei;
}

} // namespace byte_literals
} // namespace vast
