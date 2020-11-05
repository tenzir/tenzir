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

#include "vast/concept/parseable/vast/json.hpp"

#include "vast/concept/parseable/vast/address.hpp"
#include "vast/concept/parseable/vast/port.hpp"
#include "vast/concept/parseable/vast/subnet.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/concept/printable/vast/json.hpp"
#include "vast/data.hpp"
#include "vast/format/json.hpp"
#include "vast/logger.hpp"
#include "vast/policy/include_field_names.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/type.hpp"
#include "vast/view.hpp"

#include <caf/detail/pretty_type_name.hpp>
#include <caf/expected.hpp>
#include <caf/none.hpp>

namespace vast::format::json {
namespace {

struct convert {
  template <class T>
  using expected = caf::expected<T>;
  using json = vast::json;

  caf::expected<data> operator()(json::boolean b, const bool_type&) const {
    return b;
  }

  caf::expected<data> operator()(json::number n, const integer_type&) const {
    return detail::narrow_cast<integer>(n);
  }

  caf::expected<data> operator()(json::number n, const count_type&) const {
    return detail::narrow_cast<count>(n);
  }

  caf::expected<data> operator()(json::number n, const real_type&) const {
    return detail::narrow_cast<real>(n);
  }

  caf::expected<data> operator()(json::number n, const port_type&) const {
    return port{detail::narrow_cast<port::number_type>(n)};
  }

  caf::expected<data> operator()(json::number s, const time_type&) const {
    auto secs = std::chrono::duration<json::number>(s);
    auto since_epoch = std::chrono::duration_cast<duration>(secs);
    return time{since_epoch};
  }

  caf::expected<data> operator()(json::number s, const duration_type&) const {
    auto secs = std::chrono::duration<json::number>(s);
    return std::chrono::duration_cast<duration>(secs);
  }

  caf::expected<data> operator()(json::string s, const string_type&) const {
    return s;
  }

  template <class T,
            typename std::enable_if_t<has_parser_v<type_to_data<T>>, int> = 0>
  caf::expected<data> operator()(const json::string& s, const T&) const {
    using value_type = type_to_data<T>;
    value_type x;
    if (!make_parser<value_type>{}(s, x))
      return make_error(ec::parse_error, "unable to parse",
                        caf::detail::pretty_type_name(typeid(value_type)), ":",
                        s);
    return x;
  }

  caf::expected<data>
  operator()(const json::string& s, const enumeration_type& e) const {
    auto i = std::find(e.fields.begin(), e.fields.end(), s);
    if (i == e.fields.end())
      return make_error(ec::parse_error, "invalid:", s);
    return detail::narrow_cast<enumeration>(std::distance(e.fields.begin(), i));
  }

  caf::expected<data>
  operator()(const json::array& a, const list_type& v) const {
    list xs;
    xs.reserve(a.size());
    for (auto& x : a) {
      if (auto elem = caf::visit(*this, x, v.value_type))
        xs.push_back(*std::move(elem));
      else
        return elem;
    }
    return xs;
  }

  caf::expected<data>
  operator()(const json::object& o, const map_type& m) const {
    map xs;
    xs.reserve(o.size());
    for (auto& [k, v] : o) {
      // TODO: Properly unwrap the key type instead of wrapping is in json.
      auto key = caf::visit(*this, json{k}, m.key_type);
      if (!key)
        return key.error();
      auto val = caf::visit(*this, v, m.value_type);
      if (!val)
        return val.error();
      xs[*key] = *val;
    }
    return xs;
  }

  caf::expected<data>
  operator()(const json::string& str, const bool_type&) const {
    if (bool x; parsers::json_boolean(str, x))
      return x;
    return make_error(ec::convert_error, "cannot convert from", str, "to bool");
  }

  caf::expected<data>
  operator()(const json::string& str, const real_type&) const {
    if (real x; parsers::json_number(str, x))
      return x;
    return make_error(ec::convert_error, "cannot convert from", str, "to real");
  }

  caf::expected<data>
  operator()(const json::string& str, const integer_type&) const {
    if (integer x; parsers::json_int(str, x))
      return x;
    if (real x; parsers::json_number(str, x)) {
      VAST_WARNING_ANON("json-reader narrowed", str, "to type int");
      return detail::narrow_cast<integer>(x);
    }
    return make_error(ec::convert_error, "cannot convert from", str, "to int");
  }

  caf::expected<data>
  operator()(const json::string& str, const count_type&) const {
    if (count x; parsers::json_count(str, x))
      return x;
    if (real x; parsers::json_number(str, x)) {
      VAST_WARNING_ANON("json-reader narrowed", str, "to type count");
      return detail::narrow_cast<count>(x);
    }
    return make_error(ec::convert_error, "cannot convert from", str,
                      "to count");
  }

  caf::expected<data> operator()(json::string str, const port_type&) const {
    if (port x; parsers::port(str, x))
      return x;
    if (port::number_type x; parsers::u16(str, x))
      return port{x};
    return make_error(ec::convert_error, "cannot convert from", str, "to port");
  }

  template <class T, class U>
  caf::expected<data> operator()(T, U) const {
    if constexpr (std::is_same_v<std::decay_t<T>, caf::none_t>) {
      // Iff there is no specific conversion available, but the LHS is JSON
      // `null`, we always want to return VAST `nil`.
      return caf::none;
    } else {
      VAST_ERROR_ANON("json-reader cannot convert from",
                      caf::detail::pretty_type_name(typeid(T)), "to",
                      caf::detail::pretty_type_name(typeid(U)));
      return make_error(ec::syntax_error, "invalid json type");
    }
  }
};

const vast::json* lookup(std::string_view field, const vast::json::object& xs) {
  VAST_ASSERT(!field.empty());
  auto lookup_flat = [&]() {
    auto r = xs.find(field);
    return r == xs.end() ? nullptr : &r->second;
  };
  auto i = field.find('.');
  if (i == std::string_view::npos)
    return lookup_flat();
  // We have to deal with a nested field name in a potentially nested JSON
  // object.
  auto r = xs.find(field.substr(0, i));
  if (r == xs.end())
    // Attempt to access JSON field with flattened name.
    return lookup_flat();
  auto obj = caf::get_if<vast::json::object>(&r->second);
  if (obj == nullptr)
    return nullptr;
  field.remove_prefix(i + 1);
  return lookup(field, *obj);
}

} // namespace

caf::error writer::write(const table_slice_ptr& x) {
  json_printer<policy::oneline> printer;
  return print<policy::include_field_names>(printer, x, "{", ", ", "}");
}

const char* writer::name() const {
  return "json-writer";
}

caf::error add(table_slice_builder& builder, const vast::json::object& xs,
               const record_type& layout) {
  for (auto& field : layout.fields) {
    auto i = lookup(field.name, xs);
    // Non-existing fields are treated as empty (unset).
    if (!i) {
      if (!builder.add(make_data_view(caf::none)))
        return make_error(ec::unspecified, "failed to add caf::none to table "
                                           "slice builder");
      continue;
    }
    auto x = caf::visit(convert{}, *i, field.type);
    if (!x)
      return make_error(ec::convert_error, x.error().context(),
                        "could not convert", field.name, ":", to_string(*i));
    if (!builder.add(make_data_view(*x)))
      return make_error(ec::type_clash, "unexpected type", field.name, ":",
                        to_string(*i));
  }
  return caf::none;
}

} // namespace vast::format::json
