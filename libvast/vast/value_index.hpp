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

#include <algorithm>
#include <memory>
#include <type_traits>

#include <caf/deserializer.hpp>
#include <caf/error.hpp>
#include <caf/serializer.hpp>

#include "vast/ewah_bitmap.hpp"
#include "vast/ids.hpp"
#include "vast/bitmap_index.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/concept/printable/vast/operator.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/overload.hpp"
#include "vast/die.hpp"
#include "vast/error.hpp"
#include "vast/expected.hpp"
#include "vast/type.hpp"
#include "vast/value_index_factory.hpp"
#include "vast/view.hpp"

namespace vast {

using value_index_ptr = std::unique_ptr<value_index>;

/// An index for a ::value that supports appending and looking up values.
/// @warning A lookup result does *not include* `nil` values, regardless of the
/// relational operator. Include them requires performing an OR of the result
/// and an explit query for nil, e.g., `x != 42 || x == nil`.
class value_index {
public:
  value_index(vast::type x);

  virtual ~value_index();

  using size_type = typename ids::size_type;

  /// Appends a data value.
  /// @param x The data to append to the index.
  /// @returns `true` if appending succeeded.
  expected<void> append(data_view x);

  /// Appends a data value.
  /// @param x The data to append to the index.
  /// @param pos The positional identifier of *x*.
  /// @returns `true` if appending succeeded.
  expected<void> append(data_view x, id pos);

  /// Looks up data under a relational operator. If the value to look up is
  /// `nil`, only `==` and `!=` are valid operations. The concrete index
  /// type determines validity of other values.
  /// @param op The relation operator.
  /// @param x The value to lookup.
  /// @returns The result of the lookup or an error upon failure.
  expected<ids> lookup(relational_operator op, data_view x) const;

  /// Merges another value index with this one.
  /// @param other The value index to merge.
  /// @returns `true` on success.
  //bool merge(const value_index& other);

  /// Retrieves the ID of the last append operation.
  /// @returns The largest ID in the index.
  size_type offset() const;

  /// @returns the type of the value index.
  const vast::type& type() const;

  // -- persistence -----------------------------------------------------------

  virtual caf::error serialize(caf::serializer& sink) const;

  virtual caf::error deserialize(caf::deserializer& source);

private:
  virtual bool append_impl(data_view x, id pos) = 0;

  virtual expected<ids>
  lookup_impl(relational_operator op, data_view x) const = 0;

  ewah_bitmap mask_;
  ewah_bitmap none_;
  const vast::type type_;
};

/// @relates value_index
caf::error inspect(caf::serializer& sink, const value_index& x);

/// @relates value_index
caf::error inspect(caf::deserializer& source, value_index& x);

/// @relates value_index
caf::error inspect(caf::serializer& sink, const value_index_ptr& x);

/// @relates value_index
caf::error inspect(caf::deserializer& source, value_index_ptr& x);

namespace detail {

template <class Index, class Sequence>
expected<ids> container_lookup_impl(const Index& idx, relational_operator op,
                               const Sequence& xs) {
  ids result;
  if (op == in) {
    result = bitmap{idx.offset(), false};
    for (auto x : xs) {
      auto r = idx.lookup(equal, x);
      if (r)
        result |= *r;
      else
        return r;
      if (all<1>(result)) // short-circuit
        return result;
    }
  } else if (op == not_in) {
    result = bitmap{idx.offset(), true};
    for (auto x : xs) {
      auto r = idx.lookup(equal, x);
      if (r)
        result -= *r;
      else
        return r;
      if (all<0>(result)) // short-circuit
        return result;
    }
  } else {
    return make_error(ec::unsupported_operator, op);
  }
  return result;
}

template <class Index>
expected<ids> container_lookup(const Index& idx, relational_operator op,
                               view<vector> xs) {
  VAST_ASSERT(xs);
  return container_lookup_impl(idx, op, *xs);
}

template <class Index>
expected<ids> container_lookup(const Index& idx, relational_operator op,
                               view<set> xs) {
  VAST_ASSERT(xs);
  return container_lookup_impl(idx, op, *xs);
}

} // namespace detail

/// An index for arithmetic values.
template <class T, class Binner = void>
class arithmetic_index : public value_index {
public:
  using value_type =
    std::conditional_t<
      detail::is_any_v<T, timestamp, timespan>,
      timespan::rep,
      std::conditional_t<
        detail::is_any_v<T, boolean, integer, count, real>,
        T,
        std::false_type
      >
    >;

  static_assert(!std::is_same_v<value_type, std::false_type>,
                "invalid type T for arithmetic_index");

  using coder_type =
    std::conditional_t<
      std::is_same_v<T, boolean>,
      singleton_coder<ids>,
      multi_level_coder<range_coder<ids>>
    >;

  using binner_type =
    std::conditional_t<
      std::is_void_v<Binner>,
      // Choose a space-efficient binner if none specified.
      std::conditional_t<
        detail::is_any_v<T, timestamp, timespan>,
      decimal_binner<9>, // nanoseconds -> seconds
        std::conditional_t<
          std::is_same_v<T, real>,
          precision_binner<10>, // no fractional part
          identity_binner
        >
      >,
      Binner
    >;

  using bitmap_index_type = bitmap_index<value_type, coder_type, binner_type>;

  template <
    class... Ts,
    class = std::enable_if_t<std::is_constructible<bitmap_index_type, Ts...>{}>
  >
  explicit arithmetic_index(vast::type t, Ts&&... xs)
    : value_index{std::move(t)},
      bmi_{std::forward<Ts>(xs)...} {
    // nop
  }

  caf::error serialize(caf::serializer& sink) const override {
    return caf::error::eval([&] { return value_index::serialize(sink); },
                            [&] { return sink(bmi_); });
  }

  caf::error deserialize(caf::deserializer& source) override {
    return caf::error::eval([&] { return value_index::deserialize(source); },
                            [&] { return source(bmi_); });
  }

private:
  bool append_impl(data_view d, id pos) override {
    auto append = [&](auto x) {
      bmi_.skip(pos - bmi_.size());
      bmi_.append(x);
      return true;
    };
    return caf::visit(detail::overload(
      [&](auto&&) { return false; },
      [&](view<boolean> x) { return append(x); },
      [&](view<integer> x) { return append(x); },
      [&](view<count> x) { return append(x); },
      [&](view<real> x) { return append(x); },
      [&](view<timespan> x) { return append(x.count()); },
      [&](view<timestamp> x) { return append(x.time_since_epoch().count()); }
    ), d);
  }

  expected<ids>
  lookup_impl(relational_operator op, data_view d) const override {
    return caf::visit(detail::overload(
      [&](auto x) -> expected<ids> {
        return make_error(ec::type_clash, value_type{}, materialize(x));
      },
      [&](view<boolean> x) -> expected<ids> { return bmi_.lookup(op, x); },
      [&](view<integer> x) -> expected<ids> { return bmi_.lookup(op, x); },
      [&](view<count> x) -> expected<ids> { return bmi_.lookup(op, x); },
      [&](view<real> x) -> expected<ids> { return bmi_.lookup(op, x); },
      [&](view<timespan> x) -> expected<ids> {
        return bmi_.lookup(op, x.count());
      },
      [&](view<timestamp> x) -> expected<ids> {
        return bmi_.lookup(op, x.time_since_epoch().count());
      },
      [&](view<vector> xs) { return detail::container_lookup(*this, op, xs); },
      [&](view<set> xs) { return detail::container_lookup(*this, op, xs); }
    ), d);
  };

  bitmap_index_type bmi_;
};

/// An index for strings.
class string_index : public value_index {
public:
  /// Constructs a string index.
  /// @param t An instance of `string_type`.
  /// @param max_length The maximum string length to support. Longer strings
  ///                   will be chopped to this size.
  explicit string_index(vast::type t, size_t max_length = 1024);

  caf::error serialize(caf::serializer& sink) const override;

  caf::error deserialize(caf::deserializer& source) override;

private:
  /// The index which holds each character.
  using char_bitmap_index = bitmap_index<uint8_t, bitslice_coder<ewah_bitmap>>;

  /// The index which holds the string length.
  using length_bitmap_index =
    bitmap_index<uint32_t, multi_level_coder<range_coder<ids>>>;

  bool append_impl(data_view x, id pos) override;

  expected<ids>
  lookup_impl(relational_operator op, data_view x) const override;

  size_t max_length_;
  length_bitmap_index length_;
  std::vector<char_bitmap_index> chars_;
};

/// An index for IP addresses.
class address_index : public value_index {
public:
  using byte_index = bitmap_index<uint8_t, bitslice_coder<ewah_bitmap>>;
  using type_index = bitmap_index<bool, singleton_coder<ewah_bitmap>>;

  explicit address_index(vast::type t);

  caf::error serialize(caf::serializer& sink) const override;

  caf::error deserialize(caf::deserializer& source) override;

private:
  bool append_impl(data_view x, id pos) override;

  expected<ids>
  lookup_impl(relational_operator op, data_view x) const override;

  std::array<byte_index, 16> bytes_;
  type_index v4_;
};

/// An index for subnets.
class subnet_index : public value_index {
public:
  using prefix_index = bitmap_index<uint8_t, equality_coder<ewah_bitmap>>;

  explicit subnet_index(vast::type t);

  caf::error serialize(caf::serializer& sink) const override;

  caf::error deserialize(caf::deserializer& source) override;

private:
  bool append_impl(data_view x, id pos) override;

  expected<ids>
  lookup_impl(relational_operator op, data_view x) const override;

  address_index network_;
  prefix_index length_;
};

/// An index for ports.
class port_index : public value_index {
public:
  using number_index =
    bitmap_index<
      port::number_type,
      multi_level_coder<range_coder<ewah_bitmap>>
    >;

  using protocol_index =
    bitmap_index<
      std::underlying_type<port::port_type>::type,
      equality_coder<ewah_bitmap>
    >;

  explicit port_index(vast::type t);

  caf::error serialize(caf::serializer& sink) const override;

  caf::error deserialize(caf::deserializer& source) override;

private:
  bool append_impl(data_view x, id pos) override;

  expected<ids>
  lookup_impl(relational_operator op, data_view x) const override;

  number_index num_;
  protocol_index proto_;
};

/// An index for vectors and sets.
class sequence_index : public value_index {
public:
  /// Constructs a sequence index of a given type.
  /// @param t The sequence type.
  /// @param max_size The maximum number of elements permitted per sequence.
  ///                 Longer sequences will be trimmed at the end.
  explicit sequence_index(vast::type t, size_t max_size = 128);

  /// The bitmap index holding the sequence size.
  using size_bitmap_index =
    bitmap_index<uint32_t, multi_level_coder<range_coder<ids>>>;

  caf::error serialize(caf::serializer& sink) const override;

  caf::error deserialize(caf::deserializer& source) override;

private:
  bool append_impl(data_view x, id pos) override;

  expected<ids>
  lookup_impl(relational_operator op, data_view x) const override;

  std::vector<value_index_ptr> elements_;
  size_t max_size_;
  size_bitmap_index size_;
  vast::type value_type_;
};

} // namespace vast
