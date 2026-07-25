//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/type_list.hpp"

#include "tenzir/test/test.hpp"

#include <type_traits>
#include <variant>

namespace {

using tenzir::TypeList;

// -- size ---------------------------------------------------------------

static_assert(TypeList<>::size == 0);
static_assert(TypeList<int, double, float>::size == 3);

// -- contains -------------------------------------------------------------

static_assert(TypeList<int, double>::contains<int>);
static_assert(not TypeList<int, double>::contains<float>);
static_assert(not TypeList<>::contains<int>);

// -- unique_index_of --------------------------------------------------------

static_assert(TypeList<int, double, float>::unique_index_of<int> == 0);
static_assert(TypeList<int, double, float>::unique_index_of<double> == 1);
static_assert(TypeList<int, double, float>::unique_index_of<float> == 2);

// -- at -----------------------------------------------------------------

static_assert(std::is_same_v<TypeList<int, double, float>::at<0>, int>);
static_assert(std::is_same_v<TypeList<int, double, float>::at<1>, double>);
static_assert(std::is_same_v<TypeList<int, double, float>::at<2>, float>);

// -- apply ----------------------------------------------------------------

template <typename... Ts>
struct count_types : std::integral_constant<std::size_t, sizeof...(Ts)> {};

static_assert(TypeList<int, double, float>::apply<count_types>::value == 3);
static_assert(std::is_same_v<TypeList<int, double>::apply<std::variant>,
                             std::variant<int, double>>);

// -- wrap -----------------------------------------------------------------

static_assert(std::is_same_v<TypeList<int, double>::wrap<std::add_pointer_t>,
                             TypeList<int*, double*>>);

// -- transform ------------------------------------------------------------

template <typename T>
struct add_const_helper : std::type_identity<const T> {};

static_assert(std::is_same_v<TypeList<int, double>::transform<add_const_helper>,
                             TypeList<const int, const double>>);

// -- all_of -----------------------------------------------------------------

struct not_arithmetic {};

static_assert(TypeList<int, double>::all_of<std::is_arithmetic>);
static_assert(not TypeList<int, not_arithmetic>::all_of<std::is_arithmetic>);
static_assert(TypeList<>::all_of<std::is_arithmetic>);

// -- join -------------------------------------------------------------------

static_assert(std::is_same_v<TypeList<int>::join<double, float>,
                             TypeList<int, double, float>>);
static_assert(std::is_same_v<TypeList<int>::join<TypeList<double, float>>,
                             TypeList<int, double, float>>);
static_assert(std::is_same_v<TypeList<>::join<TypeList<>>, TypeList<>>);

// -- index_sequence -----------------------------------------------------------

static_assert(
  std::is_same_v<
    std::remove_const_t<decltype(TypeList<int, double, float>::index_sequence)>,
    std::index_sequence<0, 1, 2>>);
static_assert(
  std::is_same_v<std::remove_const_t<decltype(TypeList<>::index_sequence)>,
                 std::index_sequence<>>);

} // namespace

TEST("TypeList dummy") {
  // Empty test suites are not allowed.
}
