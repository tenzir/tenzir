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

#include <unordered_map>
#include <utility>
#include <type_traits>

#include "vast/detail/assert.hpp"
#include "vast/detail/type_traits.hpp"

namespace vast {

/// Traits to be specialized by classes that want to be constructed through a
/// factory.
/// @tparam Type The base class to define a factory for.
/// @relates factory
// The type ins this trait class serve as an example only. Users must
// specialize this class as they see fit.
template <class Type>
struct factory_traits {
  // -- mandatory types --------------------------------------------------------

  // The type that the factory should produce.
  using result_type = double;

  // The key to register a factory with.
  using key_type = int;

  // The function type to construct a polymorphic type.
  using signature = result_type (*)(int, int);

  // -- optional convenience functions for simpler factory registration --------

  // Produces a factory key for a concrete type.
  template <class T>
  static key_type key();

  // A mandatory function to construct a type for a given key. All arguments
  // after the key are forwarded to the registered factory.
  static result_type make(key_type key, int, int);

  // -- optional overloads for construction ------------------------------------

  // A convenience function to construct a concrete type T.
  template <class T>
  static result_type make(int x, int y);

  // An optional overload that differs from the factory signature.
  template <class T>
  static result_type make(int x);

  // An optional overload without template parameter.
  static result_type make(char x);
};

/// An extensible factory to construct polymorphic objects.
/// @tparam Type The base class for type type-erased instances.
template <class Type>
struct factory {
  using traits = factory_traits<Type>;
  using key_type = typename traits::key_type;
  using signature = typename traits::signature;
  using result_type = typename traits::result_type;

  // Sanity checks.
  static_assert(std::is_polymorphic_v<Type>,
                "factory only supports polymorphic types");
  static_assert(
    std::is_same_v<Type, std::decay_t<decltype(*std::declval<result_type>())>>);

  // -- registration ----------------------------------------------------------

  /// Registers a new factory with manually specified function.
  /// @param key The key to register a factory with.
  /// @param factory The function to register with *key*.
  /// @returns `true` iff the factory was registered successfully.
  /// @pre `factory != nullptr`
  template <class Key>
  static bool add(Key&& key, signature factory) {
    VAST_ASSERT(factory != nullptr);
    if constexpr (std::is_convertible_v<std::decay_t<Key>, key_type>)
      return factories().emplace(std::forward<Key>(key), factory).second;
    else
      return add(traits::key(std::forward<Key>(key)), factory);
  }

  /// Registers a (key, factory) pair.
  /// @tparam T The concrete subtype of `Type` to register.
  /// @returns `true` iff the factory was registered successfully.
  template <class T, class Key>
  static bool add(Key&& key) {
    return add(std::forward<Key>(key), traits::template make<T>);
  }

  /// Registers a (key, factory) pair.
  /// @tparam Key The key type to convert in to a key instance.
  /// @tparam T The concrete subtype to register.
  /// @returns `true` iff the factory was registered successfully.
  template <class Key, class T>
  static bool add() {
    return add<T>(traits::template key<Key>());
  }

  /// Registers a new factory with a key and function from the traits.
  /// @tparam T The concrete subtype of `Type` to register.
  /// @returns `true` iff the traits factory was successfully registered.
  template <class T>
  static bool add() {
    return add<T, T>();
  }

  /// Retrieves a factory for a given key.
  /// @param key The key to retrieve a factory for.
  /// @returns The factory associated with *key* or `nullptr` if *key* does not
  ///          map to a registered factory function.
  template <class Key>
  static signature get(Key&& key) {
    auto i = factories().find(make_key(std::forward<Key>(key)));
    return i == factories().end() ? nullptr : i->second;
  }

  /// Automatically retrieves a factory for a given key.
  /// @tparam T The concrete subtype of `Type`.
  /// @returns The factory associated with `Type` or `nullptr` if `Type` does
  ///          not map to a registered factory function.
  template <class T>
  static signature get() {
    return get(traits::template key<T>());
  }

  /// Removes all entries from the factory.
  static void clear() {
    factories().clear();
  }

  /// Registered pre-defined types of the factory.
  static void initialize() {
    traits::initialize();
  }

  // -- construction ----------------------------------------------------------

  /// Constructs a concrete type via a registered factory.
  /// @param key The key of the factory to retrieve.
  /// @param args The arguments to pass to the factory function.
  /// @returns An instance of `result_type` or `nullptr` on failure.
  template <class Key, class... Args>
  static result_type make(Key&& key, Args&&... args) {
    if (auto f = get(std::forward<Key>(key)))
      return invoke(f, std::forward<Key>(key), std::forward<Args>(args)...);
    return nullptr;
  }

  /// Constructs a concrete type via a registered factory.
  template <class T, class... Args>
  static result_type make(Args&&... args) {
    return make(traits::template key<T>(), std::forward<Args>(args)...);
  }

private:
  template <class Traits, class T>
  using has_key_function = decltype(Traits::key(std::declval<T>()));

  template <class T>
  static decltype(auto) make_key(T&& x) {
    if constexpr (detail::is_detected_v<has_key_function, traits, T>)
      return traits::key(std::forward<T>(x)); // always try to normalize keys
    else
      return std::forward<T>(x);
  }


  template <class Key, class... Args>
  static auto invoke(signature factory, Key&& key, Args&&... args) {
    // Only forward the key if the factory signature has it as first argument.
    if constexpr (std::is_invocable_v<signature, Key, Args...>)
      return factory(std::forward<Key>(key), std::forward<Args>(args)...);
    else if constexpr (std::is_invocable_v<signature, Args...>)
      return factory(std::forward<Args>(args)...);
  }

  static auto& factories() {
    // TODO: use detail::flat_map instead.
    static std::unordered_map<key_type, signature> factories;
    return factories;
  }
};

} // namespace vast
