//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "caf/send.hpp"
#include "vast/detail/assert.hpp"
#include "vast/logger.hpp"
#include "vast/system/actors.hpp"
#include "vast/table_slice.hpp"

#include <caf/actor_addr.hpp>
#include <caf/intrusive_ptr.hpp>
#include <caf/make_counted.hpp>
#include <caf/typed_actor.hpp>
#include <caf/variant.hpp>

#include <mutex>
#include <unordered_map>

namespace vast::system {

struct end_of_stream_marker_t {};
static constexpr auto end_of_stream_marker = end_of_stream_marker_t{};

struct flush_guard final : caf::ref_counted {
  explicit flush_guard(flush_listener_actor flush_listener);
  ~flush_guard() noexcept override;

  flush_guard() = default;
  flush_guard(const flush_guard&) = default;
  flush_guard(flush_guard&&) = default;
  flush_guard& operator=(const flush_guard&) = default;
  flush_guard& operator=(flush_guard&&) = default;

  template <class Inspector>
  friend auto inspect(Inspector& f, caf::intrusive_ptr<flush_guard>& x) {
    auto flush_listener = x ? x->flush_listener_ : flush_listener_actor{};
    return f(flush_listener,
             caf::meta::load_callback([&]() noexcept -> caf::error {
               if (flush_listener)
                 x = caf::make_counted<flush_guard>(std::move(flush_listener));
               return caf::none;
             }));
  }

private:
  flush_listener_actor flush_listener_ = {};
  std::atomic<int>* counter_ = {};
};

void intrusive_ptr_add_ref(const flush_guard*);
void intrusive_ptr_release(const flush_guard*);

template <class T>
class stream_controlled : public caf::variant<T, end_of_stream_marker_t> {
public:
  friend struct caf::sum_type_access<stream_controlled<T>>;
  using variant_type = caf::variant<T, end_of_stream_marker_t>;

  stream_controlled() = default;
  stream_controlled(const stream_controlled& x) = default;
  stream_controlled(stream_controlled&& x) noexcept = default;
  stream_controlled& operator=(const stream_controlled&) = default;
  stream_controlled& operator=(stream_controlled&&) noexcept = default;
  ~stream_controlled() noexcept = default;

  stream_controlled(const variant_type& x) : variant_(x) {
    // nop
  }

  stream_controlled(variant_type&& x) noexcept : variant_(std::move(x)) {
    // nop
  }

  stream_controlled& operator=(const variant_type& x) {
    variant_ = x;
    return *this;
  }

  stream_controlled& operator=(variant_type&& x) noexcept {
    variant_ = std::move(x);
    return *this;
  }

  void subscribe(caf::intrusive_ptr<flush_guard> flush_guard) {
    VAST_ASSERT(flush_guard);
    VAST_ASSERT(!flush_guard_);
    flush_guard_ = std::move(flush_guard);
  }

  void subscribe(flush_listener_actor flush_listener) {
    subscribe(caf::make_counted<flush_guard>(std::move(flush_listener)));
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, stream_controlled& x) ->
    typename Inspector::result_type {
    return f(x.variant_, x.flush_guard_);
  }

private:
  variant_type variant_ = {};
  caf::intrusive_ptr<flush_guard> flush_guard_ = {};
};

} // namespace vast::system

namespace caf {

template <class T>
struct sum_type_access<vast::system::stream_controlled<T>> {
  using element_type = vast::system::stream_controlled<T>;
  using super = sum_type_access<typename element_type::variant_type>;

  using types = typename super::types;
  using type0 = typename super::type0;

  static constexpr bool specialized = true;

  template <class U, int Pos>
  static bool is(const element_type& x, sum_type_token<U, Pos> token) {
    return super::template is<U, Pos>(x.variant_, token);
  }

  template <class U, int Pos>
  static U& get(element_type& x, sum_type_token<U, Pos> token) {
    return super::template get<U, Pos>(x.variant_, token);
  }

  template <class U, int Pos>
  static const U& get(const element_type& x, sum_type_token<U, Pos> token) {
    return super::template get<U, Pos>(x.variant_, token);
  }

  template <class U, int Pos>
  static U* get_if(element_type* x, sum_type_token<U, Pos> token) {
    return super::template get_if<U, Pos>(x, token);
  }

  template <class U, int Pos>
  static const U* get_if(const element_type* x, sum_type_token<U, Pos> token) {
    return super::template get_if<U, Pos>(x, token);
  }

  template <class Result, class Visitor, class... Ts>
  static Result apply(element_type& x, Visitor&& visitor, Ts&&... xs) {
    return super::template apply<Result, Visitor, Ts...>(
      x, std::forward<Visitor>(visitor), std::forward<Ts>(xs)...);
  }

  template <class Result, class Visitor, class... Ts>
  static Result apply(const element_type& x, Visitor&& visitor, Ts&&... xs) {
    return super::template apply<Result, Visitor, Ts...>(
      x, std::forward<Visitor>(visitor), std::forward<Ts>(xs)...);
  }
};

} // namespace caf
