#ifndef VAST_CONCEPT_VERSION_H
#define VAST_CONCEPT_VERSION_H

#include <type_traits>

namespace vast {

/// Types modeling the *Version* concept shall specify this class with a
/// `static constexpr uint32_t` member named `serial` which identifies the
/// current version of the type.
template <typename T>
struct version;

namespace detail {

struct is_versionized {
  template <typename T>
  static auto test(T*) -> decltype(version<T>::serial(), std::true_type());

  template <typename>
  static auto test(...) -> std::false_type;
};

// Trait to check whether a type has a version.
template <typename T>
struct is_versionized : decltype(detail::is_versionized::test<T>(0)) {};

} // namespace detail
} // namespace vast

#endif
