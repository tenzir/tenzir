//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/transformer.hpp"

#include <caf/detail/pretty_type_name.hpp>

#include <type_traits>
#include <unordered_map>

#define VAST_FWD(x) std::forward<decltype(x)>(x)

namespace vast {

/// # Usage
/// Define some of these function functions as `operator()`:
/// - Source:    `() -> generator<Output>`
/// - Stateless: `Input -> Output`
/// - Stateful:  `generator<Input> -> generator<Output>`
/// The `transformer_control&` can also be appended as an argument. The result
/// can optionally be wrapped in `caf::expected`, and `dynamic_output` can be
/// used in place of `generator<Output>`.
///
///
/// # Uncertain
/// - `type -> table_slice -> Output`
/// - `() -> type -> table_slice -> Output`
/// - `() -> type -> generator<table_slice> -> generator<Output>
/// - `() -> (type -> S, generator<table_slice, S> -> generator<Output>)`
/// - `() -> (S, generator<table_slice, S> -> generator<Output>)`
template <class Self>
class crtp_transformer : public transformer {
public:
  auto instantiate(dynamic_input input, transformer_control& control) const
    -> caf::expected<dynamic_output> override {
    auto f = detail::overload{
      [&](std::monostate) -> caf::expected<dynamic_output> {
        if constexpr (std::is_invocable_v<const Self&>) {
          return self()();
        } else {
          return caf::make_error(ec::type_clash, "todo (not invocable)");
        }
      },
      [&]<class Input>(
        generator<Input> input) -> caf::expected<dynamic_output> {
        if constexpr (std::is_invocable_v<const Self&, Input>) {
          return std::invoke(
            [this](generator<Input> input)
              -> generator<std::invoke_result_t<const Self&, Input>> {
              for (auto&& x : input) {
                co_yield self()(std::move(x));
              }
            },
            std::move(input));
        } else if constexpr (std::is_invocable_v<const Self&, generator<Input>>) {
          return self()(std::move(input));
        } else if constexpr (std::is_invocable_v<const Self&, generator<Input>,
                                                 transformer_control&>) {
          return self()(std::move(input), control);
        } else {
          return caf::make_error(ec::type_clash, "todo (not invocable)");
        }
      },
    };
    return std::visit(f, std::move(input));
  }

  auto clone() const -> std::unique_ptr<transformer> override {
    return std::make_unique<Self>(self());
  }

private:
  auto self() const -> const Self& {
    return static_cast<const Self&>(*this);
  }
};

// --------------------------------------------------------------------

/// # Usage
/// Override `initialize` and `process`, and perhaps `finish`.
template <class State_, class Output_ = table_slice>
class schematic_transformer : public transformer {
public:
  using State = State_;
  using Output = Output_;

  virtual auto initialize(const type& schema) const -> caf::expected<State> = 0;

  virtual auto process(table_slice slice, State& state) const -> Output = 0;

  virtual auto finish(std::unordered_map<type, State> states) const
    -> generator<Output> {
    (void)states;
    return {};
  }

  auto instantiate(dynamic_input input, transformer_control& control) const
    -> caf::expected<dynamic_output> final {
    auto f = detail::overload{
      [&](auto) -> caf::expected<dynamic_output> {
        return caf::make_error(ec::type_clash, "this transformer only accepts "
                                               "'generator<table_slice>'");
      },
      [&](generator<table_slice> input) -> caf::expected<dynamic_output> {
        return std::invoke(
          [this, &control](generator<table_slice> input) -> generator<Output> {
            auto states = std::unordered_map<type, State>{};
            for (auto&& slice : input) {
              auto it = states.find(slice.schema());
              if (it == states.end()) {
                auto state = initialize(slice.schema());
                if (!state) {
                  control.abort(state.error());
                  break;
                }
                it = states.try_emplace(it, slice.schema(), *state);
              }
              co_yield process(std::move(slice), it->second);
            }
            for (auto&& output : finish(std::move(states))) {
              co_yield output;
            }
          },
          std::move(input));
      },
    };
    return std::visit(f, std::move(input));
  }
};

// --------------------------------------------------------------------

/// # Usage
/// Override `initialize` and `process`, and perhaps `finish`.
template <class State_, class Output_ = table_slice>
class schematic_transformer2 : public transformer {
public:
  using State = State_;
  using Output = Output_;
  using Foo = generator<std::pair<table_slice, std::reference_wrapper<State>>>;

  // virtual auto initialize(const type& schema) const -> caf::expected<State> =
  // 0;

  // virtual auto process(table_slice slice, State& state) const -> Output = 0;

  // virtual auto finish(std::unordered_map<type, State> states) const
  //   -> generator<Output> {
  //   (void)states;
  //   return {};
  // }

  virtual auto foo(generator<table_slice> input, transformer_control&) const
    -> generator<Output> {
    return caf::make_error(ec::type_clash, "");
  }

  virtual auto initialize(const type& schema) const -> caf::expected<State> = 0;

  auto instantiate(dynamic_input input, transformer_control& control) const
    -> caf::expected<dynamic_output> final {
    auto f = detail::overload{
      [&](auto) -> caf::expected<dynamic_output> {
        return caf::make_error(ec::type_clash, "this transformer only accepts "
                                               "'generator<table_slice>'");
      },
      [&](generator<table_slice> input) -> caf::expected<dynamic_output> {
        return std::invoke(
          [this, &control](generator<table_slice> input) -> generator<Output> {
            auto states = std::unordered_map<type, State>{};
            for (auto&& slice : input) {
              auto it = states.find(slice.schema());
              if (it == states.end()) {
                auto state = initialize(slice.schema());
                if (!state) {
                  control.abort(state.error());
                  break;
                }
                it = states.try_emplace(it, slice.schema(), *state);
              }
              co_yield process(std::move(slice), it->second);
            }
            for (auto&& output : finish(std::move(states))) {
              co_yield output;
            }
          },
          std::move(input));
      },
    };
    return std::visit(f, std::move(input));
  }
};

// --------------------------------------------------------------------

// template <class Transformer, class... Args>
// concept instantiable
//   = requires(Transformer& t) { t.instantiate(std::declval<Args>()...); };

// template <class Transformer, class... Args>
// auto instantiate_or_error(Transformer& t, transformer_control& control,
//                           Args&&... args) -> caf::expected<dynamic_output> {
//   if constexpr (instantiable<Transformer, Args..., transformer_control&>) {
//     return t.instantiate(VAST_FWD(args)..., control);
//   } else if constexpr (instantiable<Transformer, Args...>) {
//     return t.instantiate(VAST_FWD(args)...);
//   } else {
//     auto arg_names = std::initializer_list<std::string>{
//       caf::detail::pretty_type_name(typeid(Args))...};
//     return caf::make_error(ec::type_clash,
//                            fmt::format("the transformer {} cannot be "
//                                        "instantiated with {}",
//                                        caf::detail::pretty_type_name(typeid(t)),
//                                        fmt::join(arg_names, ", ")));
//   }
// }

// /// # Usage
// /// Define some `instantiate` functions with the following signatures:
// /// - `() -> R`
// /// - `(transformer_control&) -> R`
// /// - `(generator<T>) -> R`
// /// - `(generator<T>, transformer_control&) -> R`
// /// where `R` is either `generator<U>` or `caf::expected<generator<U>>`.
// template <class Self>
// class transformer : public dynamic_transformer {
// public:
//   auto
//   instantiate(dynamic_input input, transformer_control& control) const
//     -> caf::expected<dynamic_output> override {
//     auto f = detail::overload{
//       [&](std::monostate) -> caf::expected<dynamic_output> {
//         return instantiate_or_error(self(), control);
//       },
//       [&]<class Input>(
//         generator<Input> input) -> caf::expected<dynamic_output> {
//         return instantiate_or_error(self(), control, std::move(input));
//       },
//     };
//     return std::visit(f, std::move(input));
//   }

//   // auto clone() -> std::unique_ptr<dynamic_transformer> override {
//   //   return std::make_unique<Parent>(self());
//   // }

// private:
//   auto self() const -> const Self& {
//     // static_assert(std::is_final_v<Parent>, "TODO");
//     return static_cast<const Self&>(*this);
//   }
// };

// ---------------------------------------------------------------

// template <class Self, class... Args>
// concept can_process
//   = requires(Self const& self) { self.process(std::declval<Args>()...); };

// template <class Self, class... Args>
// using process_result
//   = decltype(std::declval<Self const&>().process(std::declval<Args>()...));

// /// # Usage
// /// Define some functions `auto process(T) -> U`.
// template <class Self>
// class stateless_transformer : public dynamic_transformer {
// public:
//   auto instantiate(dynamic_input input, transformer_control&) const
//     -> caf::expected<dynamic_output> override {
//     auto f = detail::overload{
//       [&](std::monostate) -> caf::expected<dynamic_output> {
//         return caf::make_error(ec::type_clash, "nope");
//       },
//       [&]<class T>(generator<T> input) -> caf::expected<dynamic_output> {
//         if constexpr (can_process<Self, T>) {
//           return std::invoke(
//             [this](generator<T> input) -> generator<process_result<Self, T>> {
//               for (auto&& x : input) {
//                 co_yield self().process(std::move(x));
//               }
//             },
//             std::move(input));
//         } else {
//           return caf::make_error(ec::type_clash, "nope");
//         }
//       },
//     };
//     return std::visit(f, std::move(input));
//   }

// private:
//   auto self() const -> Self const& {
//     return static_cast<Self const&>(*this);
//   }
// };

} // namespace vast
