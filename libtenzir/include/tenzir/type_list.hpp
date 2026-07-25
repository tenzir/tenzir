#pragma once

#include "tenzir/concepts.hpp"

#include <optional>

namespace tenzir {
template <typename... Ts>
struct TypeList;

namespace detail {
template <concepts::instantiation_of<TypeList> L, typename... Rs>
struct JoinLists;

template <typename... L, typename... R>
struct JoinLists<TypeList<L...>, R...>
  : std::type_identity<TypeList<L..., R...>> {};

template <typename... L, typename... R>
struct JoinLists<TypeList<L...>, TypeList<R...>>
  : std::type_identity<TypeList<L..., R...>> {};

template <typename T, typename... Ts>
struct tuple_index;

template <typename T, typename... Ts>
consteval auto unique_index_of() noexcept -> std::size_t {
  constexpr static std::array found = {std::same_as<T, Ts>...};
  constexpr static auto size = std::size(found);
  constexpr auto index = []() -> std::optional<std::size_t> {
    auto last = std::optional<std::size_t>{};
    for (std::size_t i = 0; i < size; ++i) {
      if (found[i]) {
        if (last) {
          return std::nullopt;
        }
        last = i;
      }
    }
    return last;
  }();
  static_assert(index, "T must be unique in type list");
  return *index;
}
} // namespace detail

template <typename... Ts>
struct TypeList {
  constexpr static auto size = sizeof...(Ts);

  template <typename T>
  constexpr static bool contains = concepts::one_of<T, Ts...>;

  template <typename T>
  constexpr static auto unique_index_of = detail::unique_index_of<T, Ts...>();

  template <size_t I>
    requires(I < size)
  using at = std::tuple_element_t<I, std::tuple<Ts...>>;

  template <template <typename...> class C>
  using apply = C<Ts...>;

  template <template <typename> class T>
  using wrap = TypeList<T<Ts>...>;

  template <template <typename> class T>
  using transform = TypeList<typename T<Ts>::type...>;

  template <template <typename> class T>
  constexpr static bool all_of = (T<Ts>::value && ...);

  template <typename... Rs>
  using join = typename detail::JoinLists<TypeList<Ts...>, Rs...>::type;

  constexpr static auto index_sequence = std::index_sequence_for<Ts...>{};
};
} // namespace tenzir
