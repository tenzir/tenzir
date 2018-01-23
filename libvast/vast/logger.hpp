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

#ifndef VAST_LOGGER_HPP
#define VAST_LOGGER_HPP

#include <sstream>
#include <type_traits>

#include <caf/logger.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/typed_actor.hpp>

#include "vast/config.hpp"
#include "vast/concept/printable/print.hpp"
#include "vast/detail/pp.hpp"

namespace vast::detail {

struct formatter {
  template <class Stream, class T>
  struct is_streamable {
    template <class S, class U>
    static auto test(U const* x)
    -> decltype(std::declval<S&>() << *x, std::true_type());

    template <class, class>
    static auto test(...) -> std::false_type;

    using type = decltype(test<Stream, T>(0));
    static constexpr auto value = type::value;
  };

  template <class T>
  formatter& operator<<(const T& x) {
    if constexpr (is_streamable<std::ostringstream, T>::value) {
      message << x;
      return *this;
    } else if constexpr (vast::is_printable<std::ostreambuf_iterator<char>,
                                            T>::value) {
      using vast::print;
      if (!print(std::ostreambuf_iterator<char>{message}, x))
        message.setstate(std::ios_base::failbit);
      return *this;
    } else {
      static_assert(!std::is_same_v<T, T>,
                    "T is neither streamable nor printable");
    }
  }

  template <class T, class... Ts>
  formatter& operator<<(caf::stateful_actor<T, Ts...>* a) {
    message << a->name();
    return *this;
  }

  template <class... Ts>
  formatter& operator<<(const caf::typed_actor<Ts...>& a) {
    return *this << a->address();
  }

  formatter& operator<<(const caf::actor& a) {
    return *this << a->address();
  }

  formatter& operator<<(const caf::actor_addr& a) {
    message << a.id();
    return *this;
  }

  // E.g., self->current_sender()
  formatter& operator<<(const caf::strong_actor_ptr& a) {
    if (a)
      message << a->id();
    else
      message << "invalid";
    return *this;
  }

  std::ostringstream message;
};

} // namespace vast::detail

#if defined(CAF_LOG_LEVEL)
  #define VAST_LOG_IMPL(lvl, msg)                                              \
    do {                                                                       \
      vast::detail::formatter __vast_fmt;                                      \
      __vast_fmt << msg;                                                       \
      CAF_LOG_IMPL("vast", lvl, __vast_fmt.message.str());                     \
    } while (false)

  #define VAST_LOG_2(lvl, m1) VAST_LOG_IMPL(lvl, m1)
  #define VAST_LOG_3(lvl, m1, m2) VAST_LOG_2(lvl, m1 << ' ' << m2)
  #define VAST_LOG_4(lvl, m1, m2, m3) VAST_LOG_3(lvl, m1, m2 << ' ' << m3)
  #define VAST_LOG_5(lvl, m1, m2, m3, m4)                                      \
    VAST_LOG_4(lvl, m1, m2, m3 << ' ' << m4)
  #define VAST_LOG_6(lvl, m1, m2, m3, m4, m5)                                  \
    VAST_LOG_5(lvl, m1, m2, m3, m4 << ' ' << m5)
  #define VAST_LOG_7(lvl, m1, m2, m3, m4, m5, m6)                              \
    VAST_LOG_6(lvl, m1, m2, m3, m4, m5 << ' ' << m6)
  #define VAST_LOG_8(lvl, m1, m2, m3, m4, m5, m6, m7)                          \
    VAST_LOG_7(lvl, m1, m2, m3, m4, m5, m6 << ' ' << m7)
  #define VAST_LOG_9(lvl, m1, m2, m3, m4, m5, m6, m7, m8)                      \
    VAST_LOG_8(lvl, m1, m2, m3, m4, m5, m6, m7 << ' ' << m8)
  #define VAST_LOG_10(lvl, m1, m2, m3, m4, m5, m6, m7, m8, m9)                 \
    VAST_LOG_9(lvl, m1, m2, m3, m4, m5, m6, m7, m8 << ' ' << m9)
  #define VAST_LOG(...)                                                        \
    VAST_PP_OVERLOAD(VAST_LOG_, __VA_ARGS__)(__VA_ARGS__)

  #if VAST_LOG_LEVEL >= CAF_LOG_LEVEL_ERROR
    #define VAST_ERROR(...) VAST_LOG(CAF_LOG_LEVEL_ERROR, __VA_ARGS__)
  #else
    #define VAST_ERROR(...) CAF_VOID_STMT
  #endif

  #if VAST_LOG_LEVEL >= CAF_LOG_LEVEL_WARNING
    #define VAST_WARNING(...) VAST_LOG(CAF_LOG_LEVEL_WARNING, __VA_ARGS__)
  #else
    #define VAST_WARNING(...) CAF_VOID_STMT
  #endif

  #if VAST_LOG_LEVEL >= CAF_LOG_LEVEL_INFO
    #define VAST_INFO(...) VAST_LOG(CAF_LOG_LEVEL_INFO, __VA_ARGS__)
  #else
    #define VAST_INFO(...) CAF_VOID_STMT
  #endif

  #if VAST_LOG_LEVEL >= CAF_LOG_LEVEL_DEBUG
    #define VAST_DEBUG(...) VAST_LOG(CAF_LOG_LEVEL_DEBUG, __VA_ARGS__)
  #else
    #define VAST_DEBUG(...) CAF_VOID_STMT
  #endif

  #if VAST_LOG_LEVEL >= CAF_LOG_LEVEL_TRACE
    #define VAST_TRACE(...) VAST_LOG(CAF_LOG_LEVEL_TRACE, __VA_ARGS__)
  #else
    #define VAST_TRACE(...) CAF_VOID_STMT
  #endif
#else
  #define VAST_ERROR(...) CAF_VOID_STMT
  #define VAST_WARNING(...) CAF_VOID_STMT
  #define VAST_INFO(...) CAF_VOID_STMT
  #define VAST_DEBUG(...) CAF_VOID_STMT
  #define VAST_TRACE(...) CAF_VOID_STMT
#endif

#endif
