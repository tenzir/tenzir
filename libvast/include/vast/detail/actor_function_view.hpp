//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <caf/function_view.hpp>

namespace vast::detail {

// A variant of function view that uses an existing scoped_actor.
// Useful if the communication partner wants to monitor or send
// additional messages.
template <class Actor>
class actor_function_view {
public:
  using type = Actor;

  /// Sends a request message to the assigned actor and returns the result.
  template <class... Ts>
  auto operator()(Ts&&... xs) {
    static_assert(sizeof...(Ts) > 0, "no message to send");
    using trait = caf::response_type<typename type::signatures,
                                     caf::detail::strip_and_convert_t<Ts>...>;
    static_assert(trait::valid, "receiver does not accept given message");
    using tuple_type = typename trait::tuple_type;
    using value_type = caf::function_view_flattened_result_t<tuple_type>;
    using result_type = caf::expected<value_type>;
    if (!dest)
      return result_type{caf::sec::bad_function_call};
    caf::error err;
    caf::function_view_result<value_type> result;
    self->request(dest, timeout, std::forward<Ts>(xs)...)
      .receive(
        [&](caf::error& x) {
          err = std::move(x);
        },
        typename caf::function_view_storage<value_type>::type{result.value});
    if (err)
      return result_type{err};
    return result_type{flatten(result.value)};
  }

  template <class T>
  T&& flatten(T& x) {
    return std::move(x);
  }

  template <class T>
  T&& flatten(std::tuple<T>& x) {
    return std::move(get<0>(x));
  }

  caf::scoped_actor& self;
  type dest;
  caf::duration timeout = caf::infinite;
};

template <class T>
auto make_actor_function_view(caf::scoped_actor& self, const T& x,
                              caf::duration t = caf::infinite) {
  return actor_function_view<T>{self, x, t};
}

} // namespace vast::detail
