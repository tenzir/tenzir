#ifndef VAST_ACCESS_H
#define VAST_ACCESS_H

namespace vast {

/// Wrapper to encapsulate the implementation of concepts requiring access to
/// private state.
struct access
{
  template <typename, typename = void>
  struct state;

  template <typename, typename = void>
  struct parser;

  template <typename, typename = void>
  struct printer;

  template <typename, typename = void>
  struct converter;
};

namespace detail {

struct has_access_state
{
  template <typename T>
  static auto test(T* x) -> decltype(access::state<T>{}, std::true_type());

  template <typename>
  static auto test(...) -> std::false_type;
};

struct has_access_parser
{
  template <typename T>
  static auto test(T* x) -> decltype(access::parser<T>{}, std::true_type());

  template <typename>
  static auto test(...) -> std::false_type;
};

struct has_access_printer
{
  template <typename T>
  static auto test(T* x) -> decltype(access::printer<T>{}, std::true_type());

  template <typename>
  static auto test(...) -> std::false_type;
};

struct has_access_converter
{
  template <typename T>
  static auto test(T* x) -> decltype(access::converter<T>{}, std::true_type());

  template <typename>
  static auto test(...) -> std::false_type;
};

} // namespace detail

template <typename T>
struct has_access_state : decltype(detail::has_access_state::test<T>(0)) {};

template <typename T>
struct has_access_parser : decltype(detail::has_access_parser::test<T>(0)) {};

template <typename T>
struct has_access_printer : decltype(detail::has_access_printer::test<T>(0)) {};

template <typename T>
struct has_access_converter : decltype(detail::has_access_converter::test<T>(0)) {};

} // namespace vast

#endif
