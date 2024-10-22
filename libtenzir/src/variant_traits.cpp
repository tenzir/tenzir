#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/type.hpp"

#include <caf/variant.hpp>

#include <variant>

namespace tenzir {

template <class T>
class variant_traits;

template <class... Ts>
class variant_traits<std::variant<Ts...>> {
public:
  static constexpr auto count = sizeof...(Ts);

  constexpr static auto index(const std::variant<Ts...>& x) -> size_t {
    return x.index();
  }

  template <size_t I>
  constexpr static auto get(const std::variant<Ts...>& x)
    -> const std::variant_alternative_t<I, std::variant<Ts...>>& {
    return *std::get_if<I>(&x);
  }
};

template <class... Ts>
class variant_traits<caf::variant<Ts...>> {
public:
  static constexpr auto count = sizeof...(Ts);

  static auto index(const caf::variant<Ts...>& x) -> size_t {
    return x.index();
  }

  template <size_t I>
  static auto get(const caf::variant<Ts...>& x)
    -> const std::variant_alternative_t<I, std::variant<Ts...>>& {
    using T = std::tuple_element_t<I, std::tuple<Ts...>>;
    return *caf::sum_type_access<caf::variant<Ts...>>::get_if(
      &x, caf::sum_type_token<T, I>{});
  }
};

template <>
class variant_traits<type> {
public:
  static constexpr auto count = caf::detail::tl_size<concrete_types>::value;

  static auto index(const type& x) -> size_t {
    // TODO: Is this assumption correct? Probably not!!!
    return x.type_index();
  }

  template <size_t I>
  static auto get(const type& x)
    -> const caf::detail::tl_at_t<concrete_types, I>& {
    using Type = caf::detail::tl_at_t<concrete_types, I>;
    // TODO: This breaks.
    // static_assert(Type::type_index == I);
    if constexpr (basic_type<Type>) {
      static constexpr auto instance = Type{};
      return instance;
    } else {
      // TODO: Is this allowed?
      return static_cast<const Type&>(
        static_cast<const stateful_type_base&>(x));
    }
  }
};

template <>
class variant_traits<arrow::Array> {
public:
  using types = caf::detail::tl_map_t<concrete_types, type_to_arrow_array>;

  static constexpr auto count = caf::detail::tl_size<types>::value;

  static auto index(const arrow::Array& x) -> size_t {
    // 1. Check this:
    (void)x.type_id();
    arrow::BooleanArray::TypeClass::type_id;
    // 2. For extension type:
    auto name
      = static_cast<const arrow::ExtensionType&>(*x.type()).extension_name();
    auto other = ip_type::arrow_type::name;
    if (other == name) {
      // Ok
    }
  }

  template <size_t I>
  static auto get(const arrow::Array& x) -> decltype(auto) {
    return static_cast<const caf::detail::tl_at_t<types, I>&>(x);
  }
};

// TODO: Check this.
template <class T, class U>
constexpr auto transfer_const_ref(const U& x) -> decltype(auto) {
  using TNoRef = std::remove_reference_t<T>;
  using UNoCvRef = std::remove_cvref_t<U>;
  if constexpr (std::is_lvalue_reference_v<T>) {
    if constexpr (std::is_const_v<TNoRef>) {
      return static_cast<std::add_const_t<UNoCvRef>&>(x);
    } else {
      return const_cast<std::remove_const_t<UNoCvRef>&>(x);
    }
  } else {
    if constexpr (std::is_const_v<TNoRef>) {
      return static_cast<std::add_const_t<UNoCvRef>&&>(x);
    } else {
      return const_cast<std::remove_const_t<UNoCvRef>&&>(x);
    }
  }
}

template <size_t I, class V>
constexpr auto get_impl(V&& v) -> decltype(auto) {
  // TODO: Assume here that we can propagate non-const?
  static_assert(
    std::is_reference_v<
      decltype(variant_traits<std::remove_cvref_t<V>>::template get<I>(v))>);
  return transfer_const_ref<V>(
    variant_traits<std::remove_cvref_t<V>>::template get<I>(v));
}

template <class V, class T>
constexpr auto type_to_variant_index = std::invoke(
  []<size_t... Is>(std::index_sequence<Is...>) {
    auto result = 0;
    auto found
      = (std::invoke([&] {
           if (std::same_as<
                 std::decay_t<decltype(variant_traits<V>::template get<Is>(
                   std::declval<V>()))>,
                 T>) {
             result = Is;
             return 1;
           }
           return 0;
         })
         + ... + 0);
    if (found == 0) {
      throw std::runtime_error{"type was not found in variant"};
    }
    if (found > 1) {
      throw std::runtime_error{"type was found multiple times in variant"};
    }
    return result;
  },
  std::make_index_sequence<variant_traits<V>::count>());

template <class V, class... Fs>
constexpr auto match_one(V&& v, Fs&&... fs) -> decltype(auto) {
  using Traits = variant_traits<std::remove_cvref_t<V>>;
  // TODO: Could be better by ref?
  auto visitor = detail::overload{std::forward<Fs>(fs)...};
  using Visitor = decltype(visitor);
  auto index = Traits::index(std::as_const(v));
  static_assert(std::same_as<decltype(index), size_t>);
  using Result = decltype(visitor(get_impl<0>(v)));
  // TODO: static?
  // TODO: A switch/if-style dispatch is probably more performant.
  constexpr auto table = std::invoke(
    []<size_t... Is>(std::index_sequence<Is...>) {
      return std::array{
        +[](Visitor&& visitor, V&& v) -> Result {
          // TODO: refs?
          // TODO: void?
          using Ret = decltype(std::move(visitor)(get_impl<Is>(v)));
          static_assert(std::same_as<Ret, Result>,
                        "return types must be equal");
          return std::move(visitor)(get_impl<Is>(v));
          // if constexpr (std::same_as<Result, void>) {
          // }
          // decltype(auto) result
          //   = visitor(Traits::template get<Is>(std::forward<T>(x)));
          // static_assert(std::same_as<decltype(result), Result>,
          //               "return types must be equal");
          // return result;
        }...,
      };
    },
    std::make_index_sequence<Traits::count>());
  static_assert(table.size() == Traits::count);
  TENZIR_ASSERT(index < Traits::count);
  return table[index](std::move(visitor), std::forward<V>(v)); // NOLINT
}

template <class... Xs>
constexpr auto match_tuple(std::tuple<Xs...> xs, auto&& f) {
  // TODO: Match zero.
  if constexpr (sizeof...(Xs) == 0) {
    return f();
  } else {
    auto&& x = std::get<0>(xs);
    // TODO: Forward like?
    return match_one(x, [&]<class X>(X&& x) {
      return match_tuple(std::apply(
                           []<class... Ys>(auto&&, Ys&&... ys) {
                             return std::tuple<Ys&&...>{ys...};
                           },
                           xs),
                         [&]<class... Ys>(Ys&&... ys) {
                           f(std::forward<X>(x), std::forward<Ys>(ys)...);
                         });
    });
  }
}

template <class V, class... Fs>
constexpr auto match(V&& v, Fs&&... fs) -> decltype(auto) {
  if constexpr (caf::detail::is_specialization<std::tuple,
                                               std::remove_cvref_t<V>>::value) {
    return match_tuple(std::forward<V>(v),
                       detail::overload{std::forward<Fs>(fs)...});
  } else {
    return match_one(std::forward<V>(v), std::forward<Fs>(fs)...);
  }
}

// We would need to disable ADL, but then we cannot overload index/type...

// Disable ADL!
// template <size_t I>
// constexpr auto get = []<class V>(V&& v) -> decltype(auto) {
//   TENZIR_ASSERT(variant_traits<V>::index(v) == I);
//   return get_impl<I>(std::forward<V>(v));
// };
template <class T>
constexpr auto get = []<class V>(V&& v) -> decltype(auto) {
  constexpr auto index = type_to_variant_index<std::remove_cvref_t<V>, T>;
  TENZIR_ASSERT(variant_traits<std::remove_cvref_t<V>>::index(v) == index);
  return get_impl<index>(std::forward<V>(v));
};

template <class T>
constexpr auto get_if = []<class V>(V* v) -> T* {
  constexpr auto index = type_to_variant_index<V, T>;
  if (not v) {
    return nullptr;
  }
  if (variant_traits<std::remove_const_t<V>>::index(*v) != index) {
    return nullptr;
  }
  return &get_impl<index>(*v);
};

// template <class T, class... Fs>
// auto match2(T&& x) {
//   return [](auto...) {
//     return 42;
//   };
// }

// template <class... Fs>
// auto match3(Fs&&...) {
//   return [](auto...) {
//     return 42;
//   };
// }

// template <class... Fs>
// class [[nodiscard]] match4 : Fs... {
// public:
//   explicit match4(Fs&&... fs) : Fs{std::forward<Fs>(fs)}... {
//   }

//   template <class... Variants>
//   auto operator()(Variants&&... variants) -> decltype(auto) {
//     return 42;
//   }
// };

void test() {
  auto ty = type{};
  match(
    ty, []<concrete_type T>(T& x) {}, []<basic_type T>(T& x) {});

  auto std_var = std::variant<int, double>{};
  auto caf_var = caf::variant<int, double>{};

  constexpr auto constexpr_test = std::invoke([] {
    auto var = std::variant<int, double>{5.0};
    match(
      var,
      [](int x) {

      },
      [](double x) {

      });
    return 9.5;
  });
  auto got = get<double>(std_var);
  auto ptr = get_if<double>(&std_var);

  static_assert(constexpr_test == 9.5);

  auto xyz1 = match(
    caf_var,
    [](int x) {
      static_assert(true);
      return 42;
    },
    [](double x) {
      static_assert(true);
      return 43;
    },
    [](std::monostate x) {
      static_assert(true);
      return 44;
    });

  // auto xyz2 = match2(var)(
  //   [](int x) {
  //     static_assert(true);
  //   },
  //   [](double x) {},
  //   [](std::monostate x) {
  //     static_assert(true);
  //   });

  // auto xyz3 = match3(
  //   [](int x) {
  //     static_assert(true);
  //   },
  //   [](double x) {},
  //   [](std::monostate x) {
  //     static_assert(true);
  //   })(var);

  // auto xyz = match4{
  //   [](int x) {
  //     static_assert(true);
  //   },
  //   [](double x) {},
  //   [](std::monostate x) {
  //     static_assert(true);
  //   },
  // }(var);
}

} // namespace tenzir
