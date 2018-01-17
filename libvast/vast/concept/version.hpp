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

#ifndef VAST_CONCEPT_VERSION_HPP
#define VAST_CONCEPT_VERSION_HPP

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
