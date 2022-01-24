//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/concepts.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/overload.hpp"
#include "vast/die.hpp"

#include <caf/detail/type_list.hpp>
#include <caf/response_handle.hpp>
#include <caf/typed_response_promise.hpp>

#include <exception>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <variant>

#if __has_include(<coroutine>)

#  include <coroutine>
namespace stdcoro = std;

#else

#  include <experimental/coroutine>
namespace stdcoro = std::experimental;

#endif

namespace vast {

// -- forward declarations ----------------------------------------------------

template <class T = void>
class [[nodiscard]] co_result;

namespace detail {

template <class T>
struct [[nodiscard]] co_result_promise;

} // namespace detail

// -- co_lift -----------------------------------------------------------------

namespace detail {

template <class Function>
struct co_lift_helper_impl {
  static decltype(auto) make(auto*, auto&& fun) noexcept {
    return std::forward<decltype(fun)>(fun);
  }
};

/// A traits class for inferring the signature of the `co_result<T>` returning
/// function object we want to lift into a `caf::result<T>` returning function
/// object.
template <class Function>
struct co_lift_helper : co_lift_helper_impl<decltype(&Function::operator())> {};

template <class T, class Lambda, class... Args>
struct co_lift_helper_impl<co_result<T> (Lambda::*)(Args...)> {
  static auto make(auto*, auto&& fun) noexcept {
    static_assert(detail::always_false_v<decltype(fun)>,
                  "lifted co_result coroutines must be marked noexcept");
  }
};

template <class T, class Lambda, class... Args>
struct co_lift_helper_impl<co_result<T> (Lambda::*)(Args...) const> {
  static auto make(auto*, auto&& fun) noexcept {
    static_assert(detail::always_false_v<decltype(fun)>,
                  "lifted co_result coroutines must be marked noexcept");
  }
};

template <class T, class Lambda, class... Args>
struct co_lift_helper_impl<co_result<T> (Lambda::*)(Args...) noexcept> {
  static auto make([[maybe_unused]] auto* self, auto&& fun) noexcept {
    // TODO: When upgrading to CAF 0.18, allow for passing the parameters by
    // reference, and instead keep a reference to
    // `self->current_mailbox_element()->payload` in the co_result_promise.
    static_assert(!std::disjunction_v<std::is_reference<Args>...>,
                  "lifted arguments must be passed by value to stay alive for "
                  "the duration of the coroutine frame");
    // TODO: When upgrading to CAF 0.18, add noexcept to the returned lambda.
    // Right now, `caf::dmfou` is not specialized for noexcept-specified member
    // function objects, which means that it cannot be used in an actor
    // behavior.
    constexpr static auto is_state_memfn = requires {
      requires std::is_same_v<Lambda,
                              std::remove_cvref_t<decltype(self->state)>>;
    };
    if constexpr (is_state_memfn) {
      return [self, fun = std::forward<decltype(fun)>(fun)](
               Args&... args) -> caf::result<T> {
        return std::invoke(fun, self->state, std::move(args)...);
      };
    }
    else {
      return [fun = std::forward<decltype(fun)>(fun)](
               Args&... args) mutable -> caf::result<T> {
        return fun(std::move(args)...);
      };
    }
  }
};

template <class T, class Lambda, class... Args>
struct co_lift_helper_impl<co_result<T> (Lambda::*)(Args...) const noexcept> {
  static auto make([[maybe_unused]] auto* self, auto&& fun) noexcept {
    // TODO: When upgrading to CAF 0.18, allow for passing the parameters by
    // reference, and instead keep a reference to
    // `self->current_mailbox_element()->payload` in the co_result_promise.
    static_assert(!std::disjunction_v<std::is_reference<Args>...>,
                  "lifted arguments must be passed by value to stay alive for "
                  "the duration of the coroutine frame");
    // TODO: When upgrading to CAF 0.18, add noexcept to the returned lambda.
    // Right now, `caf::dmfou` is not specialized for noexcept-specified member
    // function objects, which means that it cannot be used in an actor
    // behavior.
    constexpr static auto is_state_memfn = requires {
      requires std::is_same_v<Lambda,
                              std::remove_cvref_t<decltype(self->state)>>;
    };
    if constexpr (is_state_memfn) {
      return [self, fun = std::forward<decltype(fun)>(fun)](
               Args&... args) -> caf::result<T> {
        return std::invoke(fun, self->state, std::move(args)...);
      };
    }
    else {
      return [fun = std::forward<decltype(fun)>(fun)](
               Args&... args) -> caf::result<T> {
        return std::invoke(fun, std::move(args)...);
      };
    }
  }
};

} // namespace detail

/// Creates a lifted behavior, i.e., a behavior in which every `co_result<T>`
/// returning function object is automatically lifted via `co_lift<T>`.
template <class... Sigs>
auto co_lift_behavior(caf::typed_event_based_actor<Sigs...>* self,
                      auto&&... sigs) noexcept -> caf::typed_behavior<Sigs...> {
  return {detail::co_lift_helper<std::remove_cvref_t<decltype(sigs)>>::make(
    self, std::forward<decltype(sigs)>(sigs))...};
}

/// Creates a lifted behavior, i/ returning function object is automatically
/// lifted via `co_lift<T>`.
template <class State, class... Sigs>
auto co_lift_behavior(
  caf::stateful_actor<State, caf::typed_event_based_actor<Sigs...>>* self,
  auto&&... sigs) noexcept -> caf::typed_behavior<Sigs...> {
  return {detail::co_lift_helper<std::remove_cvref_t<decltype(sigs)>>::make(
    self, std::forward<decltype(sigs)>(sigs))...};
}

// -- co_request_then / co_request_await / co_request_receive -----------------

namespace detail {

/// The action of the `co_request_awaiter`.
/// TODO: When adding support for CAF 0.18, add support for `fan_out_request`
/// automatically by supporting calling `co_request_*` with a vector of
/// destination handles, and allowing users to specify the select policy as the
/// first template argument.
enum class co_request_action {
  then,    ///< Await the response asynchronously in FIFO order via
           ///< `caf::response_handle::then`.
  await,   ///< Await the response asynchronously in LIFO order via
           ///< `caf::response_handle::await`.
  receive, ///< Await the response synchronously via
           ///< `caf::response_handle::receive`.
};

/// A trait for determining the response type and function signature of response
/// handlers from their output type list.
template <class Output>
struct response_handler_traits;

template <class... Args>
struct response_handler_traits<caf::detail::type_list<Args...>> {
  /// The argument type used in the response callback of `co_request_awaiter`.
  using argument_type = std::tuple<Args&...>;

  /// An utility for lifting the response callback from a `argument_type`
  /// receiving version to a `Ts&...` receiving version.
  template <class Function>
  struct wrap_helper : wrap_helper<decltype(&Function::operator())> {};

  template <class Lambda>
  struct wrap_helper<void (Lambda::*)(argument_type) noexcept> {
    static auto make(auto&& fun) noexcept {
      return [fun = std::forward<decltype(fun)>(fun)](
               Args&... args) mutable noexcept {
        fun(argument_type{args...});
      };
    }
  };

  template <class Lambda>
  struct wrap_helper<void (Lambda::*)(argument_type) const noexcept> {
    static auto make(auto&& fun) noexcept {
      return [fun = std::forward<decltype(fun)>(fun)](Args&... args) noexcept {
        fun(argument_type{args...});
      };
    }
  };

  /// Wraps a callback lambda into a lambda that receives the argument 1-by-1 to
  /// allow for using `caf::result<Ts...>`, which takes the `Ts...`
  /// individually. This essentially mirrors the result-type inferring logic of
  /// `caf::function_view` with a bit more flexibility.
  template <class Function>
  static auto wrap(Function&& fun) noexcept
    -> decltype(wrap_helper<std::remove_cvref_t<Function>>::make(
      std::forward<Function>(fun))) {
    return wrap_helper<std::remove_cvref_t<Function>>::make(
      std::forward<Function>(fun));
  }

  /// Undoes the wrapping of the response argument type into a lambda if required.
  static auto unwrap(argument_type&& args) noexcept {
    if constexpr (sizeof...(Args) == 0)
      return caf::unit;
    else if constexpr (sizeof...(Args) == 1)
      return std::get<0>(std::move(args));
    else
      return std::move(args);
  }

  /// Returns the tuple-unwrapped version of the argument type.
  using response_type = decltype(unwrap(std::declval<argument_type&&>()));
};

/// A trait for determining the template parameters of a response handle from a
/// response handle.
template <class ResponseHandle>
struct response_handle_traits;

template <concepts::actor_handle Self, class Output, bool IsBlocking>
struct response_handle_traits<caf::response_handle<Self, Output, IsBlocking>>
  : response_handler_traits<Output> {
  /// The type of the self actor pointer.
  using self_type = Self;

  /// A type list for the parameters of the response handler in the success case.
  using output_type = Output;

  /// Indicates whether the response is awaited synchronously.
  static constexpr bool is_blocking = IsBlocking;
};

/// The awaiter object returned by `co_request_*` functions. When this is first
/// to suspend the parent `co_result` coroutine it allocates a response promise.
template <co_request_action Action, class ResponseHandle>
struct [[nodiscard]] co_request_awaiter {
  using traits = response_handle_traits<ResponseHandle>;

  /// Construct the awaiter type.
  explicit co_request_awaiter(ResponseHandle&& response_handle)
    : response_handle_{std::move(response_handle)}, response_{} {
    // nop
  }

  /// For non-blocking requests we must always suspend the coroutine as we only
  /// set the continuation in the `await_suspend` handler.
  [[nodiscard]] constexpr bool await_ready() const noexcept
    requires(!traits::is_blocking) {
    return false;
  }

  /// For blocking requests we must never suspend the coroutine as we can get
  /// the result immediately.
  [[nodiscard]] constexpr bool await_ready() noexcept
    requires(traits::is_blocking) {
    if constexpr (Action == co_request_action::receive) {
      auto handle_value
        = traits::wrap([this](typename traits::argument_type value) noexcept {
            response_ = traits::unwrap(std::move(value));
          });
      auto handle_error = [this](caf::error& err) noexcept {
        response_ = std::move(err);
      };
      std::move(response_handle_).receive(handle_value, handle_error);
      VAST_ASSERT(response_);
      return true;
    }
    die("unhandled blocking co_request_action");
  }

  /// Unconditionally transfer execution back to the calling context; there's no
  /// reason for the executing context to be this awaiter as receiving the
  /// result happens in the CAF runtime.
  template <class T>
  void await_suspend(
    stdcoro::coroutine_handle<co_result_promise<T>> coroutine) noexcept {
    if constexpr (traits::is_blocking)
      die("co_request_awaiter must only be suspended for non-blocking "
          "requests");
    auto handle_value = traits::wrap(
      [=, this](typename traits::argument_type value) mutable noexcept {
        response_ = traits::unwrap(std::move(value));
        VAST_ASSERT(coroutine);
        coroutine.resume();
      });
    auto handle_error = [=, this](caf::error& err) mutable noexcept {
      response_ = std::move(err);
      VAST_ASSERT(coroutine);
      coroutine.resume();
    };
    if constexpr (Action == co_request_action::then) {
      std::move(response_handle_).then(handle_value, handle_error);
      return;
    } else if constexpr (Action == co_request_action::await) {
      std::move(response_handle_).await(handle_value, handle_error);
      return;
    }
    die("unhandled non-blocking co_request_action");
  }

  /// Provide the calling context a `caf::expected<response_type>` signaling
  /// a received value or error.
  [[nodiscard]] caf::expected<typename traits::response_type>
  await_resume() noexcept {
    VAST_ASSERT(response_);
    return std::move(*response_);
  }

  /// A mechanism for the surrounding co_result coroutine to create a CAF
  /// response promise.
  [[nodiscard]] typename traits::self_type* self() noexcept {
    return response_handle_.self();
  }

private:
  /// The response handle returned from a call to `self->request(...)`.
  ResponseHandle response_handle_;

  /// The response returned from the request.
  std::optional<caf::expected<typename traits::response_type>> response_;
};

} // namespace detail

/// The analog to `self.request(dest, timeout, args...).then(...)` for
/// `co_result<T>` coroutines.
///
/// When awaited suspends the coroutine, returns an expected value similar to
/// `caf::function_view`, and continues the coroutine execution in the
/// response handlers CAF runtime context.
template <caf::message_priority Priority = caf::message_priority::normal>
auto co_request_then(concepts::non_blocking_actor_handle auto& self,
                     const concepts::statically_typed_actor_handle auto& dest,
                     const auto& timeout, auto&&... args) noexcept {
  auto handle = self.template request<Priority>(
    dest, timeout, std::forward<decltype(args)>(args)...);
  return detail::co_request_awaiter<detail::co_request_action::then,
                                    decltype(handle)>{std::move(handle)};
}

/// The analog to `self.request(dest, timeout, args...).await(...)` for
/// `co_result<T>` coroutines.
///
/// When awaited suspends the coroutine, returns an expected value similar to
/// `caf::function_view`, and continues the coroutine execution in the
/// response handlers CAF runtime context.
template <caf::message_priority Priority = caf::message_priority::normal>
auto co_request_await(concepts::non_blocking_actor_handle auto& self,
                      const concepts::statically_typed_actor_handle auto& dest,
                      const auto& timeout, auto&&... args) noexcept {
  auto handle = self.template request<Priority>(
    dest, timeout, std::forward<decltype(args)>(args)...);
  return detail::co_request_awaiter<detail::co_request_action::await,
                                    decltype(handle)>{std::move(handle)};
}

/// The analog to `self.request(dest, timeout, args...).receive(...)` for
/// `co_result<T>` coroutines.
///
/// When awaited does *not* suspend the coroutine, returns an expected value
/// similar to `caf::function_view`, and continues the coroutine execution in
/// the the current CAF runtime context immediately.
template <caf::message_priority Priority = caf::message_priority::normal>
auto co_request_receive(concepts::blocking_actor_handle auto& self,
                        const concepts::statically_typed_actor_handle auto& dest,
                        const auto& timeout, auto&&... args) noexcept {
  auto handle = self.template request<Priority>(
    dest, timeout, std::forward<decltype(args)>(args)...);
  return detail::co_request_awaiter<detail::co_request_action::receive,
                                    decltype(handle)>{std::move(handle)};
}

// -- co_deliver --------------------------------------------------------------

namespace detail {

/// The awaiter object returned by `co_deliver`. Never suspends.
template <class T>
struct [[nodiscard]] co_deliver_awaiter : stdcoro::suspend_never {
  /// The co_deliver_awaiter's value type. CAF uses the tag type `caf::unit_t`
  /// in places where void would semantically be correct, but a non-void type is
  /// required, so we do that here as well.
  using value_type = std::conditional_t<std::is_void_v<T>, caf::unit_t, T>;

  /// Construct the awaiter type.
  explicit co_deliver_awaiter(caf::expected<value_type> value)
    : value{std::move(value)} {
    // nop
  }

  /// Provide the calling context a `caf::delegated<T>` signaling that a result
  /// was already delivered.
  [[nodiscard]] auto await_resume() noexcept {
    return caf::delegated<value_type>{};
  }

  /// The returned value_type.
  caf::expected<value_type> value;
};

} // namespace detail

/// The analog to returning a value from an actor behavior for `co_result<T>`
/// coroutines. The value is returned to the CAF runtime on the next suspend.
///
/// When awaited does *not* suspend the coroutine, returns an empty
/// tag value that *must* later be returned from the coroutine, and continues
/// the coroutine execution in the the current CAF runtime context immediately.
///
/// This exists only to solve a corner case in which the return value of the
/// coroutine is known before suspending it via `co_await co_request_*`. Use
/// with caution.
template <class T>
auto co_deliver(caf::expected<T> value) noexcept {
  return detail::co_deliver_awaiter<T>{std::move(value)};
}

template <class T>
auto co_deliver(T value) noexcept {
  return detail::co_deliver_awaiter<T>{std::move(value)};
}

template <class T>
auto co_deliver(caf::error err) noexcept {
  return detail::co_deliver_awaiter<T>{std::move(err)};
}

// -- co_delegate -------------------------------------------------------------

namespace detail {

/// The awaiter object returned by `co_delegate`. Never suspends.
template <caf::message_priority Priority, concepts::actor_handle Self,
          concepts::statically_typed_actor_handle Handle, class... Args>
struct [[nodiscard]] co_delegate_awaiter : stdcoro::suspend_never {
  /// The co_delegate_awaiter's value type. CAF uses the tag type `caf::unit_t`
  /// in places where void would semantically be correct, but a non-void type is
  /// required, so we do that here as well.
  using delegated_type = decltype(std::declval<Self*>()->delegate(
    std::declval<const Handle&>(), std::declval<Args&&>()...));

  /// Construct the awaiter type.
  co_delegate_awaiter(Self& self,
                      std::tuple<const Handle&, Args...> delegate_args)
    : self{self}, delegate_args{std::move(delegate_args)} {
    // nop
  }

  /// Provide the calling context a `caf::delegated<T>` signaling that a result
  /// was already delivered.
  [[nodiscard]] auto await_resume() noexcept {
    return delegated_type{};
  }

  /// The self actor handle.
  Self& self;

  /// The remaining arguments.
  std::tuple<const Handle&, Args...> delegate_args;
};

} // namespace detail

/// The analog to `self.delegate(dest, args...)` for `co_result<T>` coroutines.
///
/// When awaited does *not* suspend the coroutine, returns an empty tag value
/// that *must* later be returned from the coroutine, and continues the
/// coroutine execution in the the current CAF runtime context immediately.
template <caf::message_priority Priority = caf::message_priority::normal,
          concepts::actor_handle Self,
          concepts::statically_typed_actor_handle Handle, class... Args>
auto co_delegate(Self& self, const Handle& dest, Args&&... args) noexcept {
  return detail::co_delegate_awaiter<Priority, Self, Handle, Args&&...>{
    self, {dest, std::forward<Args>(args)...}};
}

// -- co_make_response_promise ------------------------------------------------

namespace detail {

/// The awaiter object returned by `co_make_response_promise`. Never suspends.
template <class T, concepts::non_blocking_actor_handle Self>
struct [[nodiscard]] co_make_response_promise_awaiter : stdcoro::suspend_never {
  /// The coroutine's value type. CAF uses the tag type `caf::unit_t` in places
  /// where void would semantically be correct, but a non-void type is required,
  /// so we do that here as well.
  using value_type = std::conditional_t<std::is_void_v<T>, caf::unit_t, T>;

  explicit co_make_response_promise_awaiter(Self& self)
    : self{self}, rp{nullptr} {
    // nop
  }

  /// Never suspend the coroutine. Checks if the `co_result_promise<T>` already
  /// has a `caf::resppnse_promise<value_type>`, and if so, extracts it.
  bool await_suspend(
    stdcoro::coroutine_handle<co_result_promise<T>> coroutine) noexcept {
    rp = coroutine.promise().unsafe_try_get_response_promise();
    return false;
  }

  /// Provide the calling context the response promise, which is either
  /// extracted from the `co_result_promise<T>` if that was suspended at least
  /// once, or created adhoc from the self actor handle.
  [[nodiscard]] auto await_resume() noexcept {
    return std::pair{rp ? *rp
                        : self.template make_response_promise<value_type>(),
                     caf::delegated<T>{}};
  }

  /// The self actor handle.
  Self& self;

  /// The pre-existing response promise, if it exists.
  caf::typed_response_promise<value_type>* rp;
};

} // namespace detail

/// The analog to `self.make_response_promise<T>()` for `co_result<T>`
/// coroutines.
///
/// When awaited does *not* suspend the coroutine, returns the
/// response promise and an empty tag value that *must* later be returned from
/// the coroutine, and continues the coroutine execution in the the current CAF
/// runtime context immediately.
///
/// It is the caller's responsiblity to fulfill the returned response promise
/// from the same actor context.
template <class T, concepts::non_blocking_actor_handle Self>
auto co_make_response_promise(Self& self) noexcept {
  return detail::co_make_response_promise_awaiter<T, Self>{self};
}

// -- co_result ---------------------------------------------------------------

namespace detail {

/// The promise type of a `co_result<T>` coroutine, which does the heavy lifting
/// and handling of various awaitable types.
template <class T>
struct co_result_promise final {
  /// The coroutine's value type. CAF uses the tag type `caf::unit_t` in places
  /// where void would semantically be correct, but a non-void type is required,
  /// so we do that here as well.
  using value_type = std::conditional_t<std::is_void_v<T>, caf::unit_t, T>;

  /// Constructs a `co_result_promise`.
  co_result_promise() noexcept = default;

  ~co_result_promise() noexcept {
    if (auto coroutine
        = stdcoro::coroutine_handle<co_result_promise<T>>::from_promise(*this))
      coroutine.destroy();
  }

  /// The `co_result_promise` is non-copyable by design because it is
  /// responsible for destroying the coroutine frame.
  co_result_promise(const co_result_promise&) = delete;
  co_result_promise& operator=(const co_result_promise&) = delete;

  /// Move-constructs / move-assigns a `co_result_promise`.
  co_result_promise(co_result_promise&&) noexcept = default;
  co_result_promise& operator=(co_result_promise&&) noexcept = default;

  /// We always want execution of the coroutine to start immediately, not just
  /// when first suspending it.
  auto initial_suspend() const noexcept {
    return stdcoro::suspend_never{};
  }

  /// Execution of the coroutine must always be suspended at the end; we have no
  /// notion of continuation for co_result as that is taken care of by the CAF
  /// runtime.
  auto final_suspend() const noexcept {
    return stdcoro::suspend_always{};
  }

  /// Callback for awaiting a `co_request_awaiter`, i.e., gets called when using
  /// `co_await co_request_{then,await,receive}(...)` in a `co_result<T>`
  /// coroutine.
  template <detail::co_request_action Action, class ResponseHandle>
  auto await_transform(
    detail::co_request_awaiter<Action, ResponseHandle>&& awaiter) noexcept {
    auto f = detail::overload{
      [&](std::monostate) noexcept {
        // We're suspending the coroutine because we're awaiting a
        auto rp = awaiter.self()->template make_response_promise<value_type>();
        VAST_ASSERT(rp.pending());
        storage_.template emplace<caf::typed_response_promise<value_type>>(
          std::move(rp));
      },
      [&](caf::typed_response_promise<value_type>& rp) noexcept {
        // If the storage already holds a response promise then we're awaiting a
        // second time; no need to create a new response promise. Instead, we
        // sanity-check that the response promise is valid and was not yet
        // fulfilled.
        VAST_ASSERT(rp.pending());
      },
      [](const value_type&) noexcept {
        die("co_result_promise cannot await a co_request_awaiter after "
            "returning a value");
      },
      [](const caf::error&) noexcept {
        die("co_result_promise cannot await a co_request_awaiter after "
            "returning an error");
      },
      [](caf::delegated<value_type>) noexcept {
        // If the storage already holds a delegated then we've already got our
        // result, and there's no need to create a promise here.
      },
    };
    std::visit(f, storage_);
    return std::move(awaiter);
  }

  /// Callback for awaiting a `co_deliver_awaiter`, i.e., gets called when using
  /// `co_await co_deliver(...)` in a `co_result<T>` coroutine.
  auto
  await_transform(detail::co_deliver_awaiter<value_type>&& awaiter) noexcept {
    auto f = detail::overload{
      [&](std::monostate) noexcept {
        if (awaiter.value)
          storage_.template emplace<value_type>(std::move(*awaiter.value));
        else
          storage_.template emplace<caf::error>(
            std::move(awaiter.value.error()));
      },
      [&](caf::typed_response_promise<value_type>& rp) noexcept {
        VAST_ASSERT(rp.pending());
        if (awaiter.value)
          rp.deliver(std::move(*awaiter.value));
        else
          rp.deliver(std::move(awaiter.value.error()));
        storage_.template emplace<caf::delegated<T>>();
      },
      [](const value_type&) noexcept {
        die("co_result_promise cannot await a co_deliver_awaiter after "
            "returning a value");
      },
      [](const caf::error&) noexcept {
        die("co_result_promise cannot await a co_deliver_awaiter after "
            "returning an error");
      },
      [](caf::delegated<value_type>) noexcept {
        die("co_result_promise cannot await a co_deliver_awaiter after "
            "delegating a value");
      },
    };
    std::visit(f, storage_);
    return std::move(awaiter);
  }

  /// Callback for awaiting a `co_delegate_awaiter`, i.e., gets called when
  /// using `co_await co_delegate(...)` in a `co_result<T>` coroutine.
  template <caf::message_priority Priority, concepts::actor_handle Self,
            concepts::statically_typed_actor_handle Handle, class... Args>
  auto
  await_transform(detail::co_delegate_awaiter<Priority, Self, Handle, Args...>&&
                    awaiter) noexcept {
    auto f = detail::overload{
      [&](std::monostate) noexcept {
        storage_.template emplace<caf::delegated<value_type>>(
          std::apply(&Self::template delegate<Priority, Handle, Args...>,
                     std::tuple_cat(std::tuple<Self&>{awaiter.self},
                                    std::move(awaiter.delegate_args))));
      },
      [&](caf::typed_response_promise<value_type>& rp) noexcept {
        VAST_ASSERT(rp.pending());
        std::apply(&caf::typed_response_promise<value_type>::template delegate<
                     Priority, Handle, Args...>,
                   std::tuple_cat(
                     std::tuple<caf::typed_response_promise<value_type>&>{rp},
                     std::move(awaiter.delegate_args)));
        storage_.template emplace<caf::delegated<value_type>>();
      },
      [](const value_type&) noexcept {
        die("co_result_promise cannot await a co_delegate_awaiter after "
            "returning a value");
      },
      [](const caf::error&) noexcept {
        die("co_result_promise cannot await a co_delegate_awaiter after "
            "returning an error");
      },
      [](caf::delegated<value_type>) noexcept {
        die("co_result_promise cannot await a co_delegate_awaiter after "
            "delegating a value");
      },
    };
    std::visit(f, storage_);
    return std::move(awaiter);
  }

  /// Callback for awaiting a `co_make_response_promise_awaiter`, i.e., gets
  /// called when using `co_await co_make_response_promise<T>(...)` in a
  /// `co_result<T>` coroutine.
  template <concepts::non_blocking_actor_handle Self>
  auto await_transform(
    detail::co_make_response_promise_awaiter<T, Self>&& awaiter) noexcept {
    auto f = detail::overload{
      [&](std::monostate) noexcept {
        storage_.template emplace<caf::delegated<value_type>>();
      },
      [&](const caf::typed_response_promise<value_type>&) noexcept {
        storage_.template emplace<caf::delegated<value_type>>();
      },
      [](const value_type&) noexcept {
        die("co_result_promise cannot await a co_make_response_promise_awaiter "
            "after returning a value");
      },
      [](const caf::error&) noexcept {
        die("co_result_promise cannot await a co_make_response_promise_awaiter "
            "after returning an error");
      },
      [](caf::delegated<value_type>) noexcept {
        die("co_result_promise cannot await a co_make_response_promise_awaiter "
            "after delegating a value");
      },
    };
    std::visit(f, storage_);
    return std::move(awaiter);
  }

  /// Forbid using awaitables not tailor-made for `co_result<T>`.
  auto await_transform(auto&&) = delete;

  /// Creates the `co_result<T>` return object of the coroutine frame; this
  /// is required by the C++ standard, and we don't do anything custom here.
  co_result<T> get_return_object() noexcept {
    return co_result<T>{
      stdcoro::coroutine_handle<co_result_promise<T>>::from_promise(*this)};
  }

  /// Callback for storing an exception that occured in the coroutine.
  void unhandled_exception() noexcept {
    // We intentionally rethrow the current exception because `co_result<T>`
    // coroutines must be noexcept-qualfiied, which is enforced via `co_lift`.
    std::rethrow_exception(std::current_exception());
  }

  /// A `co_result_promise<T> &&` is implicitly convertible to a
  /// `caf::result<T>`; this does one of three things to this promise:
  /// - Return a response promise if we arrive at this point because the
  ///   coroutine is suspended and awaiting a value or an error.
  /// - Return a value or an error if we arrived at this point without ever
  ///   suspending the coroutine.
  /// - Return a delegation tag value that indicates that an external source is
  ///   responsible for fulfilling the promise or the value was already delegated.
  caf::result<T> result() noexcept {
    auto f = detail::overload{
      [](std::monostate) -> caf::result<T> {
        die("co_result_promise must not return empty");
      },
      [](const caf::typed_response_promise<value_type>& rp) noexcept
      -> caf::result<T> {
        return {rp};
      },
      [](value_type& value) noexcept -> caf::result<T> {
        return {std::move(value)};
      },
      [](caf::error& err) noexcept -> caf::result<T> {
        return {std::move(err)};
      },
      [](caf::delegated<value_type> tag) noexcept -> caf::result<T> {
        return {tag};
      },
    };
    return std::visit(f, storage_);
  }

  /// Callback for returning a value via `co_return`.
  template <class U>
    requires(std::is_constructible_v<value_type, U&&>)
  void return_value(U&& value) noexcept(
    std::is_nothrow_constructible_v<value_type, U&&>) {
    auto f = detail::overload{
      [&, this](std::monostate) noexcept {
        // If the storage holds nothing we must store the value for later
        // return. We cannot use a response promise because we can only create
        // that once we received the self actor handle from a suspending call to
        // await_transform.
        this->storage_.template emplace<value_type>(std::forward<U>(value));
      },
      [&](caf::typed_response_promise<value_type>& rp) noexcept(
        std::is_nothrow_constructible_v<value_type, U&&>) {
        // If the storage holds a response promise we have suspended the
        // coroutine at least once via co_await, which means that we need to
        // deliver the resulting value here instead of returning it.
        VAST_ASSERT(rp.pending());
        rp.deliver(value_type{std::forward<U>(value)});
      },
      [](const value_type&) noexcept {
        die("co_result_promise cannot return a second value");
      },
      [](const caf::error&) noexcept {
        die("co_result_promise cannot return a value after previously "
            "returning an error");
      },
      [](caf::delegated<value_type>) noexcept {
        die("co_result_promise cannot return a value after previously "
            "delegating a value");
      },
    };
    std::visit(f, storage_);
  }

  /// Callback for returning an error via `co_return`.
  template <class U>
    requires(std::is_constructible_v<caf::error, U&&>)
  void return_value(U&& value) noexcept(
    std::is_nothrow_constructible_v<caf::error, U&&>) {
    auto f = detail::overload{
      [&](std::monostate) noexcept(
        std::is_nothrow_constructible_v<caf::error, U&&>) {
        // If the storage holds nothing we must store the error for later
        // return. We cannot use a response promise because we can only create
        // that once we received the self actor handle from a suspending call to
        // await_transform.
        storage_.template emplace<caf::error>(std::forward<U>(value));
      },
      [&](caf::typed_response_promise<value_type>& rp) noexcept(
        std::is_nothrow_constructible_v<caf::error, U&&>) {
        // If the storage holds a response promise we have suspended the
        // coroutine at least once via co_await, which means that we need to
        // deliver the resulting value here instead of returning it.
        VAST_ASSERT(rp.pending());
        rp.deliver(caf::error{std::forward<U>(value)});
      },
      [](const value_type&) noexcept {
        die("co_result_promise cannot return an error after previously "
            "returning a value");
      },
      [](const caf::error&) noexcept {
        die("co_result_promise cannot return a second error");
      },
      [](caf::delegated<value_type>) noexcept {
        die("co_result_promise cannot return an error after previously "
            "delegating a value");
      },
    };
    std::visit(f, storage_);
  }

  /// Callback for returning a delegation tag via `co_return`.
  void return_value(caf::delegated<value_type>) noexcept {
    auto f = detail::overload{
      [](std::monostate) noexcept {
        die("co_result_promise cannot delegate without previously awaiting a "
            "co_deliver_awaiter, co_delegate_awaiter, or "
            "co_make_response_promise_awaiter");
      },
      [](const caf::typed_response_promise<value_type>&) noexcept {
        die("co_result_promise cannot delegate after previously being "
            "suspended");
      },
      [](const value_type&) noexcept {
        die("co_result_promise cannot delegate after previously returning a "
            "value");
      },
      [](const caf::error&) noexcept {
        die("co_result_promise cannot delegate after previously returning an "
            "error");
      },
      [](caf::delegated<value_type>) noexcept {
        // nop
      },
    };
    std::visit(f, storage_);
  }

  /// This is a convenience function that allows for returning a
  /// `caf::expected<value_type>` in a `co_result<T>` coroutine, similar to how
  /// `caf::result<T>` is constructible from an expected value.
  void return_value(caf::expected<value_type> value) noexcept {
    if (value)
      return_value(std::move(*value));
    else
      return_value(std::move(value.error()));
  }

  /// Returns a pointer to the underlying typed response promise, or nullptr if
  /// that does not yet exist. Use with caution.
  caf::typed_response_promise<value_type>*
  unsafe_try_get_response_promise() noexcept {
    return std::get_if<caf::typed_response_promise<value_type>>(&storage_);
  }

private:
  /// An internal storage container for the various possible result types.
  /// - `std::monostate` indicates that no value was returned yet.
  /// - `caf::typed_response_promise<value_type>` indicates that the coroutine
  ///   was suspended, leading to the result being made available later.
  /// - `value_type` indicates that a we returned a value.
  /// - `caf::error` indicates that a we returned an error.
  /// - `caf::delegated<value_type>` indicates that we delivered a result early,
  ///   or moved responsibility for delivering to an external context.
  std::variant<std::monostate, caf::typed_response_promise<value_type>,
               value_type, caf::error, caf::delegated<value_type>>
    storage_;
};

} // namespace detail

/// A coroutine-enabling wrapper around `caf::result<T>`.
///
/// This is best explained using a tony table:
///
/// BEFORE
///
///     [self](T&...) -> caf::result<R> {
///       A();
///       self->request(d1, t, xs...).then(
///         [](response_type& value) {
///           B(value);
///         },
///         [](caf::error& err) {
///           C(err);
///         });
///       D();
///       return r;
///     }
///
/// AFTER
///
///     co_lift([self](T...) -> co_result<R> {
///       A();
///       auto task = co_request_then(*self, d1, t, xs...);
///       D();
///       auto tag = co_await co_deliver(r);
///       if (auto value = co_await task)
///         B(*value);
///       else
///         C(value.error());
///       co_return tag;
///     })
///
/// The main benefit is immediately obvious: We've got rid of callback hell.
///
/// There exist a few pitfalls with this that might be easy to run into:
/// - Using `self->request`, `self->make_response_promise`, and `self->delegate`
///   within a `co_result` coroutine leads to undefined behavior and is
///   inherently unsafe. Use `co_request_then(self, ...)`,
///   `co_request_await(self, ...)`, `co_make_response_promise<T>(self)`, and
///   `co_delegate(self, ...)` instead.
/// - After awaiting a response handle, `self->current_sender()` might return an
///   unexpected value because the resumed coroutine behaves as if it was called
///   from the response handle continuation.
/// - To deliver values early, i.e., on first suspend, use `co_deliver(...)`,
///   which returns a tag type that later must be returned from the coroutine.
/// - CAF supports catching exceptions via `set_exception_handler`. Throwing
///   exceptions from a `co_result<T>` coroutine will always bypass CAF's
///   exception handler and call `std::terminate()`.
///
/// @related co_lift_behavior TODO: => lift_behavior
/// @related co_request_then => request_then
/// @related co_request_await => request_await
/// @related co_request_receive => request_receive
/// @related co_deliver  => deliver
/// @related co_delegate => delegate
/// @related co_make_response_promise => make_response_promise
template <class T>
class [[nodiscard]] co_result {
public:
  /// The underlying promsie type that takes care of most of the magic.
  using promise_type = detail::co_result_promise<T>;

  /// Construct a `co_result` from its coroutine handle.
  explicit co_result(stdcoro::coroutine_handle<promise_type> coroutine) noexcept(
    std::is_nothrow_move_constructible_v<stdcoro::coroutine_handle<promise_type>>)
    : coroutine_{std::move(coroutine)} {
    // nop
  }

  /// Destroy the coroutine.
  /// NOTE: Unlike for most coroutine types, it is the promise type that is
  /// responsible for cleaning up the coroutine frame rather than the coroutine
  /// type. The destructor only destroys the coroutine if ownership of the
  /// handle was not previously transferred to its underlying promise.
  ~co_result() noexcept {
    if (coroutine_)
      coroutine_.destroy();
  }

  /// A `co_result` is non-copyable by design, as it otherwise would not be able
  /// to transfer ownership of the coroutine to its underlying promise.
  co_result(const co_result&) = delete;
  co_result& operator=(const co_result&) = delete;

  /// Move-construct the coroutine.
  co_result(co_result&& other) noexcept
    : coroutine_(std::exchange(other.coroutine_, {})) {
  }

  /// Move-assign the coroutine.
  co_result& operator=(co_result&& other) noexcept {
    if (std::addressof(other) != this)
      coroutine_ = std::exchange(other.coroutine_, {});
    return *this;
  }

  /// Transform a `co_result<T>` into a `caf::result<T>` on first suspend of the
  /// coroutine, moving the ownership of the coroutine into the promise.
  explicit(false) operator caf::result<T>() && noexcept {
    return std::exchange(coroutine_, {}).promise().result();
  }

private:
  /// The coroutine handle.
  stdcoro::coroutine_handle<promise_type> coroutine_ = {};
};

} // namespace vast
