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

// -- v1 includes --------------------------------------------------------------

#include "vast/table_slice_builder.hpp"

#include "vast/data.hpp"
#include "vast/detail/overload.hpp"
#include "vast/fbs/table_slice.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/table_slice.hpp"

// -- v0 includes --------------------------------------------------------------

#include "vast/data.hpp"
#include "vast/detail/overload.hpp"
#include "vast/table_slice_builder.hpp"

#include <algorithm>

namespace vast {

namespace v1 {

// -- constructors, destructors, and assignment operators ----------------------

table_slice_builder::table_slice_builder(record_type layout)
  : fbb_{}, layout_{std::move(layout)} {
  // nop
}

table_slice_builder::~table_slice_builder() = default;

// -- properties ---------------------------------------------------------------

bool table_slice_builder::recursive_add(const data& x, const type& t) {
  return caf::visit(
    detail::overload{
      [&](const list& xs, const record_type& rt) {
        for (size_t i = 0; i < xs.size(); ++i) {
          if (!recursive_add(xs[i], rt.fields[i].type))
            return false;
        }
        return true;
      },
      [&](const auto&, const auto&) { return add(make_view(x)); },
    },
    x, t);
}

table_slice table_slice_builder::finish() {
  auto result = finish_impl();
  fbb_.Clear();
  return result;
}

size_t table_slice_builder::rows() const noexcept {
  return 0;
}

table_slice_encoding table_slice_builder::encoding() const noexcept {
  return implementation_id;
}

void table_slice_builder::reserve(size_t) {
  // nop
}

const record_type& table_slice_builder::layout() const noexcept {
  return layout_;
}

size_t table_slice_builder::columns() const noexcept {
  return layout().fields.size();
}

// -- implementation details ---------------------------------------------------

bool table_slice_builder::add_impl(data_view) {
  return false;
}

table_slice table_slice_builder::finish_impl() {
  // Create an invalid table slice.
  fbs::CreateTableSlice(fbb_);
  return table_slice{fbs::release(fbb_)};
}

// -- intrusive_ptr facade -----------------------------------------------------

void intrusive_ptr_add_ref(const table_slice_builder* ptr) {
  intrusive_ptr_add_ref(static_cast<const caf::ref_counted*>(ptr));
}

void intrusive_ptr_release(const table_slice_builder* ptr) {
  intrusive_ptr_release(static_cast<const caf::ref_counted*>(ptr));
}

} // namespace v1

inline namespace v0 {

table_slice_builder::table_slice_builder(record_type layout)
  : layout_(std::move(layout)) { // nop
}

table_slice_builder::~table_slice_builder() {
  // nop
}

bool table_slice_builder::recursive_add(const data& x, const type& t) {
  return caf::visit(
    detail::overload{
      [&](const list& xs, const record_type& rt) {
        for (size_t i = 0; i < xs.size(); ++i) {
          if (!recursive_add(xs[i], rt.fields[i].type))
            return false;
        }
        return true;
      },
      [&](const auto&, const auto&) { return add(make_view(x)); },
    },
    x, t);
}

void table_slice_builder::reserve(size_t) {
  // nop
}

size_t table_slice_builder::columns() const noexcept {
  return layout_.fields.size();
}

void intrusive_ptr_add_ref(const table_slice_builder* ptr) {
  intrusive_ptr_add_ref(static_cast<const caf::ref_counted*>(ptr));
}

void intrusive_ptr_release(const table_slice_builder* ptr) {
  intrusive_ptr_release(static_cast<const caf::ref_counted*>(ptr));
}

} // namespace v0

} // namespace vast
