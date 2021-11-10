//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/detail/narrow.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_row.hpp"
#include "vast/type.hpp"

#include <iterator>

namespace vast {

namespace detail {

// Returns a tuple of addresses for the element at the given index in each of
// the given tuples.
template <std::size_t Index, class... Tuples>
constexpr auto tuple_zip_addressof(Tuples&&... tuples) {
  return std::make_tuple(std::addressof(std::get<Index>(tuples))...);
}

// Helper utility of `map_tuple_elements` for use with index sequences.
template <class Invocable, class... Tuples, std::size_t... Indices>
constexpr auto
tuple_zip_and_map_impl(std::index_sequence<Indices...>, Invocable& invocable,
                       Tuples&&... tuples) {
  return std::make_tuple(std::apply(
    [&](auto&&... args) {
      return std::invoke(invocable, *std::forward<decltype(args)>(args)...);
    },
    tuple_zip_addressof<Indices>(tuples...))...);
}

// Invoke function for each of the given tuples, capturing the result in a
// tuple itself.
template <class Invocable, class Tuple, class... Tuples>
constexpr auto
tuple_zip_and_map(Invocable& invocable, Tuple&& tuple, Tuples&&... tuples) {
  constexpr auto size = std::tuple_size_v<std::decay_t<Tuple>>;
  static_assert(((size == std::tuple_size_v<std::decay_t<Tuples>>)&&...),
                "tuple sizes must match exactly");
  return tuple_zip_and_map_impl(std::make_index_sequence<size>{}, invocable,
                                std::forward<Tuple>(tuple),
                                std::forward<Tuples>(tuples)...);
}

} // namespace detail

/// A typed view on a given set of columns of a table slice.
template <class... Types>
class projection final {
public:
  // -- member types -----------------------------------------------------------

  /// A column-wise iterator over selected columns in a table slice.
  // TODO: Consider making this a virtual base class that has multiple
  // implementations depending on the table slice type.
  struct iterator final {
    // -- iterator facade ------------------------------------------------------

    template <concrete_type Type>
    using view_for_type
      = view<std::conditional_t<std::is_same_v<Type, none_type>, data,
                                type_to_data_t<Type>>>;

    // TODO: Consider making this a random access iterator.
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using reference = std::tuple<std::optional<view_for_type<Types>>...>;

    reference operator*() const noexcept {
      auto get
        = [&]<concrete_type Type>(
            table_slice::size_type column,
            const Type& type) noexcept -> std::optional<view_for_type<Type>> {
        if constexpr (std::is_same_v<Type, none_type>) {
          // TODO: Wrapping a data_view inside an optional is kind of
          // non-sensical; we should consider offering an explicit operator bool
          // to data_view that checks whether it holds a none_t, and drop the
          // wrapping optional. As is, the ergonomics on the call site differ
          // too much when using unspecified types.
          auto data = proj_.slice_.at(row_, column);
          if (caf::holds_alternative<caf::none_t>(data))
            return std::nullopt;
          return data;
        } else {
          auto data = proj_.slice_.at(row_, column, type);
          if (caf::holds_alternative<caf::none_t>(data))
            return std::nullopt;
          return caf::get<view<type_to_data_t<Type>>>(data);
        }
      };
      return detail::tuple_zip_and_map(get, proj_.indices_, proj_.types_);
    }

    iterator& operator++() noexcept {
      ++row_;
      return *this;
    }

    iterator operator++(int) & noexcept {
      auto result = *this;
      ++*this;
      return result;
    }

    friend bool operator==(const iterator& lhs, const iterator& rhs) noexcept {
      return &lhs.proj_ == &rhs.proj_ && lhs.row_ == rhs.row_;
    };

    friend bool operator!=(const iterator& lhs, const iterator& rhs) noexcept {
      return !(lhs == rhs);
    };

    // -- constructors, destructors, and assignment operators ------------------

    /// Construct a table slice projection iterator at a given row.
    iterator(const projection& proj, table_slice::size_type row) noexcept
      : proj_{proj}, row_{row} {
      // nop
    }

    // -- concepts -------------------------------------------------------------

    /// Support CAF's type inspection.
    template <class Inspector>
    friend auto inspect(Inspector& f, iterator& x) ->
      typename Inspector::result_type {
      return f(caf::meta::type_name("vast.projection.iterator"), x.proj_,
               x.row_);
    }

    // -- utility functions ----------------------------------------------------

    /// Access a view to the whole table slice row that the iterator is working
    /// on instead of just the selected columns.
    [[nodiscard]] table_slice_row row() const noexcept {
      return {proj_.slice(), row_};
    }

    // -- implementation details -----------------------------------------------

  private:
    const projection& proj_ = {};
    table_slice::size_type row_ = 0;
  };

  // -- utility functions ------------------------------------------------------

  /// Check for validity of the projection. Returns true if all indices are
  /// valid.
  [[nodiscard]] explicit operator bool() const noexcept {
    return std::all_of(
      indices_.begin(), indices_.end(),
      [columns = slice_.columns()](table_slice::size_type index) noexcept {
        return index < columns;
      });
  }

  /// Returns an error that helps debug wrong indices.
  [[nodiscard]] caf::error error() const noexcept {
    if (*this)
      return caf::none;
    return caf::make_error(
      ec::invalid_argument,
      fmt::format("cannot project invalid indices: at least one of "
                  "the given indices is outside the valid range [0, "
                  "{}): {}",
                  slice_.columns(), indices_));
  }

  /// Return the underlying table slice.
  [[nodiscard]] const table_slice& slice() const noexcept {
    return slice_;
  }

  // -- container facade -------------------------------------------------------

  [[nodiscard]] table_slice::size_type size() const noexcept {
    return slice_.rows();
  }

  [[nodiscard]] iterator begin() const noexcept {
    if (*this)
      return {*this, 0};
    return end();
  }

  [[nodiscard]] iterator end() const noexcept {
    return {*this, size()};
  }

  // -- constructors, destructors, and assignment operators --------------------

  /// Construct a table slice projection for a given set of indices (columns).
  projection(
    table_slice slice, std::tuple<Types...> types,
    std::array<table_slice::size_type, sizeof...(Types)> indices) noexcept
    : slice_{std::move(slice)}, types_{std::move(types)}, indices_{indices} {
    // nop
  }

  // -- concepts ---------------------------------------------------------------

  /// Support CAF's type inspection.
  template <class Inspector>
  friend auto inspect(Inspector& f, projection& x) ->
    typename Inspector::result_type {
    return f(caf::meta::type_name("vast.projection"), x.slice_, x.types_,
             x.indices_);
  }

  // -- implementation details -------------------------------------------------

private:
  const table_slice slice_ = {};
  const std::tuple<Types...> types_ = {};
  const std::array<table_slice::size_type, sizeof...(Types)> indices_;
};

/// Creates a typed view on a given set of columns of a table slice.
/// @relates projection
/// @param slice The table slice to project.
/// @param hints... The hints of the columns, specified as alternating pairs of
/// type and one of flat column index, offset, or suffix-matched column name.
template <class... Hints>
auto project(table_slice slice, Hints&&... hints) {
  auto do_project = [&]<concrete_type... Types, class... Indices>(
                      std::tuple<Types, Indices> && ... hints)
                      ->projection<Types...> {
    static_assert(sizeof...(Types) == sizeof...(Indices),
                  "project requires an equal number of types and hints");
    const auto layout = slice.layout().type;
    auto find_flat_index_for_hint
      = [&]<concrete_type Type>(
          auto&& self, const Type& type,
          const auto& index) noexcept -> table_slice::size_type {
      if constexpr (std::is_convertible_v<decltype(index), offset>) {
        // If the index is an offset, we can just use it directly.
        const auto field = layout.field(index);
        if (std::is_same_v<Type, none_type> || congruent(field.type, type))
          return layout.flat_index(index);
      } else if constexpr (std::is_constructible_v<std::string_view,
                                                   decltype(index)>) {
        // If the index is a string, we need to resolve it to an offset first.
        const auto offsets = layout.resolve_key_suffix(index);
        // TODO: Should we instead check whether we have exactly one match, or
        // prefix-match rather than suffix-match?
        if (!offsets.empty())
          return self(self, type, offsets[0]);
      } else if constexpr (std::is_convertible_v<decltype(index),
                                                 table_slice::size_type>) {
        for (table_slice::size_type flat_index = 0;
             const auto& [field, _] : layout.leaves()) {
          if (flat_index
              == detail::narrow_cast<table_slice::size_type>(index)) {
            if (std::is_same_v<Type, none_type> || congruent(field.type, type))
              return flat_index;
            break;
          }
          ++flat_index;
        }
      } else {
        static_assert(detail::always_false_v<decltype(index)>,
                      "projection index must be convertible to 'offset', "
                      "'table_slice::size_type', or 'std::string_view'");
      }
      return static_cast<table_slice::size_type>(-1);
    };
    auto flat_indices = std::array{find_flat_index_for_hint(
      find_flat_index_for_hint, std::get<0>(hints), std::get<1>(hints))...};
    return {
      slice,
      std::tuple{std::forward<Types>(std::get<0>(hints))...},
      std::move(flat_indices),
    };
  };
  auto pack_type_and_index = []<concrete_type Type, class Index>(
                               auto&& self, const Type& type,
                               const Index& index, const auto&... remainder) {
    if constexpr (sizeof...(remainder) == 0) {
      return std::tuple<std::tuple<Type, decltype(+index)>>{
        std::tuple{type, index}};
    } else {
      return std::tuple_cat(self(self, type, index), self(self, remainder...));
    }
  };
  static_assert(sizeof...(hints) >= 2, "projection hints must be supplied");
  static_assert(sizeof...(hints) % 2 == 0, "projection hints must be given as "
                                           "alternating concrete types and "
                                           "projection indices");
  return std::apply(do_project,
                    pack_type_and_index(pack_type_and_index, hints...));
}

} // namespace vast
