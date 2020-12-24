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

#include "vast/format/simdjson.hpp"

#include "vast/concept/parseable/vast/address.hpp"
#include "vast/concept/parseable/vast/subnet.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/concept/printable/vast/json.hpp"
#include "vast/data.hpp"
#include "vast/logger.hpp"
#include "vast/policy/include_field_names.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/type.hpp"
#include "vast/view.hpp"

#include <caf/detail/pretty_type_name.hpp>
#include <caf/expected.hpp>
#include <caf/none.hpp>

namespace vast::format::simdjson {
namespace {

caf::expected<data> convert(const ::simdjson::dom::element & e,
                            const record_field & f);

template <typename JsonType>
struct adjusted_json_param
{
  using type = std::conditional_t<std::is_arithmetic_v<JsonType> ||
                                    std::is_same_v<JsonType, std::string_view>,
                                  JsonType,
                                  const JsonType&>;
};

template <typename JsonType>
using adjusted_json_param_t = typename adjusted_json_param<JsonType>::type;

/// A default implementation ot conversion from JSON type
/// (known as one of simdjson element_type) to an internal data type.
template< typename JsonType, int ConcreteTypeIndex>
caf::expected<data> type_biassed_convert_impl(adjusted_json_param_t<JsonType>,
                                              const record_field & f ) {
  // ngrodzitski: Once fmt is available message can be improved.
  // as simdjson has a `ostream<<element_type` which makes it
  // "serializable" to fmt.
  VAST_ERROR_ANON("json-reader cannot convert field '",
                  f.name, "' to a propper type");

  return make_error(ec::syntax_error, "conversion not implemented");
}

// BOOL Convertions.
template<>
caf::expected<data> type_biassed_convert_impl< bool, type_id<bool_type>() >(
  bool b, const record_field & ) {
  return b;
}

template<>
caf::expected<data> type_biassed_convert_impl< std::string_view, type_id<bool_type>() >(
  std::string_view s, const record_field & ) {
    if(s == "true")
      return true;
    else if(s == "false")
      return false;

    return make_error(ec::convert_error, "cannot convert from", std::string{s}, "to bool");
}

// INTEGER Conversions
template<>
caf::expected<data> type_biassed_convert_impl< integer, type_id<integer_type>() >(
  integer n, const record_field & ) {
  return n;
}

template<>
caf::expected<data> type_biassed_convert_impl<std::string_view, type_id<integer_type>() >(
  std::string_view s, const record_field & f) {
  // Reparse string as JSON element.
  // ngrodzitski: that approach kind of bad in cases like:
  // "\"\"\"\"\"42\"\"\"\"\"", which are unlikely, but can be handled later.
  ::simdjson::dom::parser parser;
  auto r = parser.parse( s );
  if(r.error())
    make_error(ec::convert_error, "cannot convert from", std::string{s}, "to int");

  return convert( r.value(), f );
}

// STRING Conversions
template<>
caf::expected<data> type_biassed_convert_impl< std::string_view, type_id<string_type>() >(
  std::string_view s, const record_field & ) {
  return std::string{ s };
}

/// A converter from a given JSON type to data.
/// Relies on a specification of type_biassed_convert_impl template function.
/// If no specialiation from a given JSON type to data is provided
/// the the default implementation is used (which returns error).
template <typename JsonType >
struct from_json_x_converter
{
  /// Converter callback.
  /// This is an alias for function which takes an instance of JsonType
  /// and returns data innstance or an error.
  using converter_callback = caf::expected<data> (*)(adjusted_json_param_t<JsonType>,
                                                     const record_field & f );

  /// A list of types which are possible destination types
  /// in the scope of conversion from JSON
  using dest_types_list = concrete_types;

  /// A total number of types possible with data.
  /// This is also exactly the nubmer of possible conversion cases.
  static constexpr std::size_t dest_types_count =
    type_id< caf::detail::tl_back_t<dest_types_list> >() + 1UL;

  using callbacks_array = std::array<converter_callback, dest_types_count>;

  template <std::size_t... ConcreteTypeIndex>
  static constexpr callbacks_array make_callbacks_array_impl(
    std::index_sequence<ConcreteTypeIndex...> ) noexcept {
      callbacks_array result = { type_biassed_convert_impl< JsonType, ConcreteTypeIndex >... };
      return result;
  }

  template <typename ConcreteTypeIndexSeq = std::make_index_sequence<dest_types_count>>
  static constexpr callbacks_array make_callbacks_array() noexcept {
      return make_callbacks_array_impl( ConcreteTypeIndexSeq{} );
  }

  static constexpr callbacks_array callbacks = make_callbacks_array();
};

using converter_callback = caf::expected<data> (*)( const ::simdjson::dom::element &,
                                                    const record_field & f );


template <typename T>
caf::expected<data> convert_from(::simdjson::simdjson_result<T> r,
                                 const record_field & f){
  VAST_ASSERT(r.error() == ::simdjson::SUCCESS);

  const auto type_index = f.type->index();

  using converter = from_json_x_converter< T >;

  if( 0L <= type_index && static_cast<int>(converter::dest_types_count) > type_index )
  {
    return converter::callbacks[type_index]( r.value(), f );
  }

  return make_error(ec::syntax_error, "invalid field type");
}

caf::expected<data> convert(const ::simdjson::dom::element & e,
                            const record_field & f) {
  switch( e.type() )
  {
    case ::simdjson::dom::element_type::ARRAY:
      return convert_from( e.get_array(), f );
    case ::simdjson::dom::element_type::OBJECT:
      return convert_from( e.get_object(), f );
    case ::simdjson::dom::element_type::INT64:
      return convert_from( e.get_int64(), f );
    case ::simdjson::dom::element_type::UINT64:
      return convert_from( e.get_uint64(), f );
    case ::simdjson::dom::element_type::DOUBLE:
      return convert_from( e.get_double(), f );
    case ::simdjson::dom::element_type::STRING:
      return convert_from( e.get_string(), f );
    case ::simdjson::dom::element_type::BOOL:
      return convert_from( e.get_bool(), f );
    case ::simdjson::dom::element_type::NULL_VALUE:
      return caf::none;
  }

  return make_error(ec::syntax_error, "invalid json type");
}


// struct convert {
//   template <class T>
//   using expected = caf::expected<T>;
//   using json = vast::json;

//   caf::expected<data> operator()(json::boolean b, const bool_type&) const {
//     return b;
//   }

//   caf::expected<data> operator()(json::number n, const integer_type&) const {
//     return detail::narrow_cast<integer>(n);
//   }

//   caf::expected<data> operator()(json::number n, const count_type&) const {
//     return detail::narrow_cast<count>(n);
//   }

//   caf::expected<data> operator()(json::number n, const real_type&) const {
//     return detail::narrow_cast<real>(n);
//   }

//   caf::expected<data> operator()(json::number s, const time_type&) const {
//     auto secs = std::chrono::duration<json::number>(s);
//     auto since_epoch = std::chrono::duration_cast<duration>(secs);
//     return time{since_epoch};
//   }

//   caf::expected<data> operator()(json::number s, const duration_type&) const {
//     auto secs = std::chrono::duration<json::number>(s);
//     return std::chrono::duration_cast<duration>(secs);
//   }

//   caf::expected<data> operator()(json::string s, const string_type&) const {
//     return s;
//   }

//   template <class T,
//             typename std::enable_if_t<has_parser_v<type_to_data<T>>, int> = 0>
//   caf::expected<data> operator()(const json::string& s, const T&) const {
//     using value_type = type_to_data<T>;
//     value_type x;
//     if (!make_parser<value_type>{}(s, x))
//       return make_error(ec::parse_error, "unable to parse",
//                         caf::detail::pretty_type_name(typeid(value_type)), ":",
//                         s);
//     return x;
//   }

//   caf::expected<data>
//   operator()(const json::string& s, const enumeration_type& e) const {
//     auto i = std::find(e.fields.begin(), e.fields.end(), s);
//     if (i == e.fields.end())
//       return make_error(ec::parse_error, "invalid:", s);
//     return detail::narrow_cast<enumeration>(std::distance(e.fields.begin(), i));
//   }

//   caf::expected<data>
//   operator()(const json::array& a, const list_type& v) const {
//     list xs;
//     xs.reserve(a.size());
//     for (auto& x : a) {
//       if (auto elem = caf::visit(*this, x, v.value_type))
//         xs.push_back(*std::move(elem));
//       else
//         return elem;
//     }
//     return xs;
//   }

//   caf::expected<data>
//   operator()(const json::object& o, const map_type& m) const {
//     map xs;
//     xs.reserve(o.size());
//     for (auto& [k, v] : o) {
//       // TODO: Properly unwrap the key type instead of wrapping is in json.
//       auto key = caf::visit(*this, json{k}, m.key_type);
//       if (!key)
//         return key.error();
//       auto val = caf::visit(*this, v, m.value_type);
//       if (!val)
//         return val.error();
//       xs[*key] = *val;
//     }
//     return xs;
//   }

//   caf::expected<data>
//   operator()(const json::string& str, const bool_type&) const {
//     if (bool x; parsers::json_boolean(str, x))
//       return x;
//     return make_error(ec::convert_error, "cannot convert from", str, "to bool");
//   }

//   caf::expected<data>
//   operator()(const json::string& str, const real_type&) const {
//     if (real x; parsers::json_number(str, x))
//       return x;
//     return make_error(ec::convert_error, "cannot convert from", str, "to real");
//   }

//   caf::expected<data>
//   operator()(const json::string& str, const integer_type&) const {
//     if (integer x; parsers::json_int(str, x))
//       return x;
//     if (real x; parsers::json_number(str, x)) {
//       VAST_WARNING_ANON("json-reader narrowed", str, "to type int");
//       return detail::narrow_cast<integer>(x);
//     }
//     return make_error(ec::convert_error, "cannot convert from", str, "to int");
//   }

//   caf::expected<data>
//   operator()(const json::string& str, const count_type&) const {
//     if (count x; parsers::json_count(str, x))
//       return x;
//     if (real x; parsers::json_number(str, x)) {
//       VAST_WARNING_ANON("json-reader narrowed", str, "to type count");
//       return detail::narrow_cast<count>(x);
//     }
//     return make_error(ec::convert_error, "cannot convert from", str,
//                       "to count");
//   }

//   template <class T, class U>
//   caf::expected<data> operator()(T, U) const {
//     if constexpr (std::is_same_v<std::decay_t<T>, caf::none_t>) {
//       // Iff there is no specific conversion available, but the LHS is JSON
//       // `null`, we always want to return VAST `nil`.
//       return caf::none;
//     } else {
//       VAST_ERROR_ANON("json-reader cannot convert from",
//                       caf::detail::pretty_type_name(typeid(T)), "to",
//                       caf::detail::pretty_type_name(typeid(U)));
//       return make_error(ec::syntax_error, "invalid json type");
//     }
//   }
// };

::simdjson::simdjson_result< ::simdjson::dom::element >
lookup(std::string_view field, const ::simdjson::dom::object& xs) {
  VAST_ASSERT(!field.empty());
  auto i = field.find('.');
  if (i == std::string_view::npos)
    return xs.at_key(field);

  // We have to deal with a nested field name in a potentially nested JSON
  // object.
  const auto [r, key_er] = xs.at_key(field.substr(0, i));

  if (key_er != ::simdjson::error_code::SUCCESS)
    // Attempt to access JSON field with flattened name.
    return xs.at_key(field);

  const auto [obj,to_object_er] = r.get_object();

  if (to_object_er != ::simdjson::SUCCESS)
    return ::simdjson::error_code::INCORRECT_TYPE;

  field.remove_prefix(i + 1);
  return lookup(field, obj);
}

} // namespace

// caf::error writer::write(const table_slice& x) {
//   json_printer<policy::oneline> printer;
//   return print<policy::include_field_names>(printer, x, "{", ", ", "}");
// }

// const char* writer::name() const {
//   return "json-writer";
// }

caf::error add(table_slice_builder& builder, const ::simdjson::dom::object& xs,
               const record_type& layout) {
  for (auto& field : layout.fields) {
    auto [el, er] = lookup(field.name, xs);
    // Non-existing fields are treated as empty (unset).
    if (er != ::simdjson::SUCCESS) {
      if (!builder.add(make_data_view(caf::none)))
        return make_error(ec::unspecified, "failed to add caf::none to table "
                                           "slice builder");
      continue;
    }
    auto x = convert(el, field);
    if (!x)
      return make_error(ec::convert_error, x.error().context(),
                        "could not convert", field.name);

    if (!builder.add(make_data_view(*x)))
      return make_error(ec::type_clash, "unexpected type", field.name);
  }
  return caf::none;
}

} // namespace vast::format::simdjson
