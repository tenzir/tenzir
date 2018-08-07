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

#include <type_traits>

namespace vast {

/// Wrapper to encapsulate the implementation of concepts requiring access to
/// private state.
struct access {
  template <class, class = void>
  struct state;

  template <class, class = void>
  struct parser;

  template <class, class = void>
  struct printer;

  template <class, class = void>
  struct converter;
};

namespace detail {

struct has_access_state {
  template <class T>
  static auto test(T* x) -> decltype(access::state<T>{}, std::true_type());

  template <class>
  static auto test(...) -> std::false_type;
};

struct has_access_parser {
  template <class T>
  static auto test(T* x) -> decltype(access::parser<T>{}, std::true_type());

  template <class>
  static auto test(...) -> std::false_type;
};

struct has_access_printer {
  template <class T>
  static auto test(T* x) -> decltype(access::printer<T>{}, std::true_type());

  template <class>
  static auto test(...) -> std::false_type;
};

struct has_access_converter {
  template <class T>
  static auto test(T* x) -> decltype(access::converter<T>{}, std::true_type());

  template <class>
  static auto test(...) -> std::false_type;
};

} // namespace detail

template <class T>
constexpr bool has_access_state_v
  = decltype(detail::has_access_state::test<T>(0))::value;

template <class T>
constexpr bool has_access_parser_v
  = decltype(detail::has_access_parser::test<T>(0))::value;

template <class T>
constexpr bool has_access_printer_v
  = decltype(detail::has_access_printer::test<T>(0))::value;

template <class T>
constexpr bool has_access_converter_v
  = decltype(detail::has_access_converter::test<T>(0))::value;

} // namespace vast

