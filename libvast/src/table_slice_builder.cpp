//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/table_slice_builder.hpp"

#include "vast/data.hpp"
#include "vast/detail/overload.hpp"
#include "vast/table_slice.hpp"

#include <caf/make_counted.hpp>

#include <algorithm>

namespace vast {

// -- constructors, destructors, and assignment operators ----------------------

table_slice_builder::table_slice_builder(legacy_record_type layout) noexcept
  : layout_(std::move(layout)) {
  // nop
}

table_slice_builder::~table_slice_builder() noexcept {
  // nop
}

// -- properties ---------------------------------------------------------------

bool table_slice_builder::recursive_add(const data& x, const type& t) {
  auto f = detail::overload{
    [&](const list& xs, const legacy_record_type& rt) {
      for (size_t i = 0; i < xs.size(); ++i) {
        if (!recursive_add(xs[i], rt.fields[i].type))
          return false;
      }
      return true;
    },
    [&](const auto&, const auto&) {
      return add(make_view(x));
    },
  };
  return caf::visit(f, x, t);
}

size_t table_slice_builder::columns() const noexcept {
  return layout_.num_leaves();
}

const legacy_record_type& table_slice_builder::layout() const noexcept {
  return layout_;
}

void table_slice_builder::reserve([[maybe_unused]] size_t num_rows) {
  // nop
}

// -- intrusive_ptr facade -----------------------------------------------------

void intrusive_ptr_add_ref(const table_slice_builder* ptr) {
  intrusive_ptr_add_ref(static_cast<const caf::ref_counted*>(ptr));
}

void intrusive_ptr_release(const table_slice_builder* ptr) {
  intrusive_ptr_release(static_cast<const caf::ref_counted*>(ptr));
}

} // namespace vast
