#ifndef VAST_DETAIL_TYPE_TRAITS_HPP
#define VAST_DETAIL_TYPE_TRAITS_HPP

#include <type_traits>
#include <vector>
#include <streambuf>
#include <string>

#include <caf/detail/type_traits.hpp>

namespace vast {
namespace detail {

using caf::detail::conjunction;
using caf::detail::disjunction;

// Computes the sum of its arguments.
template <size_t ...>
struct sum;

template <size_t S0, size_t ...SN>
struct sum<S0, SN...>
  : std::integral_constant<size_t, S0 + sum<SN...>{}> {};

template <>
struct sum<> : std::integral_constant<size_t, 0> {};

/// Checks whether a type derives from `basic_streambuf<Char>`.
template <class T, class U = void>
struct is_streambuf : std::false_type {};

template <class T>
struct is_streambuf<
  T,
  std::enable_if_t<
    std::is_base_of<std::basic_streambuf<typename T::char_type>, T>::value
  >
> : std::true_type {};

/// Checks whether a type is container which consists of contiguous bytes.
template <class T, class U = void>
struct is_contiguous_byte_container : std::false_type {};

template <class T>
struct is_contiguous_byte_container<
  T,
  std::enable_if_t<
    std::is_same<T, std::string>::value
      || std::is_same<T, std::vector<char>>::value
      || std::is_same<T, std::vector<unsigned char>>::value
  >
> : std::true_type {};

} // namespace detail
} // namespace vast

#endif

