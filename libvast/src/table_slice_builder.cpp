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
#include "vast/type.hpp"

#include <caf/make_counted.hpp>

#include <algorithm>

namespace vast {

// -- constructors, destructors, and assignment operators ----------------------

table_slice_builder::table_slice_builder(record_type layout) noexcept
  : layout_(std::move(layout)) {
  // nop
}

table_slice_builder::~table_slice_builder() noexcept {
  // nop
}

// -- properties ---------------------------------------------------------------

bool table_slice_builder::recursive_add(const data& x, const type& t) {
  auto f = detail::overload{
    [&](const record& xs, const record_type& rt) {
      if (xs.size() != rt.num_fields())
        return false;
      for (const auto& field : rt.fields()) {
        const auto it = xs.find(field.name);
        if (it == xs.end())
          return false;
        if (!recursive_add(it->second, field.type))
          return false;
      }
      return true;
    },
    [&](const list& xs, const record_type& rt) {
      size_t col = 0;
      for (const auto& field : rt.fields()) {
        VAST_ASSERT(col < xs.size());
        if (!recursive_add(xs[col], field.type))
          return false;
        ++col;
      }
      return col == xs.size();
    },
    [&](const list& xs, const list_type& lt) {
      // table_slice_builder::recursive_add's purpose is to add a whole row at
      // once that is represented as a `vast::data`. The internal data
      // representations for both Arrow and MsgPack encododings are flattened,
      // except for record types that exist in lists, which we need to unflatten
      // from lists of values into records here.
      // A way better solution would be to rethink the table_slice_builder API
      // from the ground up once we stop flattening data, and removing the need
      // for this recursive_add function.
      // TODO: In the meantime we should try to replace all use cases for this
      // function with inline add calls, traversing views on the parsed data
      // rather than converting events into lists first.
      auto unwrap_nested = [](auto&& self, data x, const type& t) -> data {
        // (1) Try to unwrap list into lists by applying unwrap_nested
        // recursively.
        if (const auto* lt = caf::get_if<list_type>(&t)) {
          auto* l = caf::get_if<list>(&x);
          VAST_ASSERT(l);
          auto result = list{};
          result.reserve(l->size());
          for (size_t i = 0; i < l->size(); ++i)
            result.emplace_back(self(self, (*l)[i], lt->value_type()));
          return result;
        }
        // (2) Try to unwrap list into records.
        if (const auto* rt = caf::get_if<record_type>(&t)) {
          auto* l = caf::get_if<list>(&x);
          VAST_ASSERT(l);
          VAST_ASSERT(l->size() == rt->num_fields());
          auto result = record{};
          result.reserve(l->size());
          for (size_t i = 0; i < l->size(); ++i) {
            const auto field = rt->field(i);
            result.emplace(std::string{field.name},
                           self(self, (*l)[i], field.type));
          }
          return result;
        }
        // (3) We're done unwrapping.
        return x;
      };
      return add(unwrap_nested(unwrap_nested, xs, lt));
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

const record_type& table_slice_builder::layout() const noexcept {
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
