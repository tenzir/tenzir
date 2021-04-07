//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/address.hpp"
#include "vast/aliases.hpp"
#include "vast/concept/hashable/uhash.hpp"
#include "vast/concept/hashable/xxhash.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/operators.hpp"
#include "vast/detail/overload.hpp"
#include "vast/fmt_integration.hpp"
#include "vast/offset.hpp"
#include "vast/pattern.hpp"
#include "vast/subnet.hpp"
#include "vast/time.hpp"
#include "vast/type.hpp"

#include <caf/default_sum_type_access.hpp>
#include <caf/detail/type_list.hpp>
#include <caf/expected.hpp>
#include <caf/fwd.hpp>
#include <caf/none.hpp>
#include <caf/optional.hpp>
#include <caf/variant.hpp>

#include <chrono>
#include <filesystem>
#include <string>
#include <tuple>
#include <type_traits>

#include <yaml-cpp/yaml.h>

namespace vast {

class data;

namespace detail {

// clang-format off
template <class T>
using to_data_type = std::conditional_t<
  std::is_floating_point_v<T>,
  real,
  std::conditional_t<
    std::is_same_v<T, bool>,
    bool,
    std::conditional_t<
      std::is_unsigned_v<T>,
      std::conditional_t<
        // TODO (ch7585): Define enumeration and count as strong typedefs to
        //                avoid error-prone heuristics like this one.
        sizeof(T) == 1,
        enumeration,
        count
      >,
      std::conditional_t<
        std::is_signed_v<T>,
        integer,
        std::conditional_t<
          std::is_convertible_v<T, std::string>,
          std::string,
          std::conditional_t<
               std::is_same_v<T, caf::none_t>
            || std::is_same_v<T, duration>
            || std::is_same_v<T, time>
            || std::is_same_v<T, pattern>
            || std::is_same_v<T, address>
            || std::is_same_v<T, subnet>
            || std::is_same_v<T, list>
            || std::is_same_v<T, map>
            || std::is_same_v<T, record>,
            T,
            std::false_type
          >
        >
      >
    >
  >
>;
// clang-format on

} // namespace detail

/// Converts a C++ type to the corresponding VAST data type.
/// @relates data
template <class T>
using to_data_type = detail::to_data_type<std::decay_t<T>>;

/// A type-erased represenation of various types of data.
/// @note Be careful when constructing a `vector<data>` from a single `list`.
/// Brace-initialization may behave unexpectedly, creating a nested vector
/// instead of copying the elements of the list for gcc only. This happens for
/// two reasons:
/// - `data` can be implicitly constructed from `list`, which is exactly
///   `vector<data>`. This is not easily fixable, because `data` is a
///   `variant<list, ...>`, which is implicitly constructible from `list` by
///   design.
/// - Core Working Group (CWG) issue #2137 has not been fixed in clang yet.
///   c.f. https://wg21.cmeerw.net/cwg/issue2137, causing brace-initialization
///   to erroneously behave like other means of construction.
class data : detail::totally_ordered<data>, detail::addable<data> {
public:
  // clang-format off
  using types = caf::detail::type_list<
    caf::none_t,
    bool,
    integer,
    count,
    real,
    duration,
    time,
    std::string,
    pattern,
    address,
    subnet,
    enumeration,
    list,
    map,
    record
  >;
  // clang-format on

  /// The sum type of all possible JSON types.
  using variant = caf::detail::tl_apply_t<types, caf::variant>;

  /// Default-constructs empty data.
  data() = default;

  data(const data&) = default;
  data& operator=(const data&) = default;
  data(data&&) noexcept = default;
  data& operator=(data&&) noexcept = default;
  ~data() noexcept = default;

  /// Constructs data from optional data.
  /// @param x The optional data instance.
  template <class T>
  data(caf::optional<T> x) : data{x ? std::move(*x) : data{}} {
    // nop
  }

  /// Constructs data from a `std::chrono::duration`.
  /// @param x The duration to construct data from.
  template <class Rep, class Period>
  data(std::chrono::duration<Rep, Period> x) : data_{duration{x}} {
    // nop
  }

  /// Constructs data.
  /// @param x The instance to construct data from.
  template <class T, class = std::enable_if_t<std::negation_v<
                       std::is_same<to_data_type<T>, std::false_type>>>>
  data(T&& x) : data_{to_data_type<T>(std::forward<T>(x))} {
    // nop
  }

  friend bool operator==(const data& lhs, const data& rhs);
  friend bool operator<(const data& lhs, const data& rhs);

  // These operators need to be templates so they're instantiated at a later
  // point in time, because there'd be a cyclic dependency otherwise.
  // caf::variant<Ts...> is just a placeholder for vast::data_view here.

  template <class... Ts>
  friend bool operator==(const data& lhs, const caf::variant<Ts...>& rhs) {
    return is_equal(lhs, rhs);
  }

  template <class... Ts>
  friend bool operator==(const caf::variant<Ts...>& lhs, const data& rhs) {
    return is_equal(lhs, rhs);
  }

  template <class... Ts>
  friend bool operator!=(const data& lhs, const caf::variant<Ts...>& rhs) {
    return !is_equal(lhs, rhs);
  }

  template <class... Ts>
  friend bool operator!=(const caf::variant<Ts...>& lhs, const data& rhs) {
    return !is_equal(lhs, rhs);
  }

  /// @cond PRIVATE

  [[nodiscard]] variant& get_data() {
    return data_;
  }

  [[nodiscard]] const variant& get_data() const {
    return data_;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, data& x) {
    return f(x.data_);
  }

  /// @endcond

private:
  variant data_;
};

std::string to_string(const data& d);

// -- helpers -----------------------------------------------------------------

/// Maps a concrete data type to a corresponding @ref type.
/// @relates data type
template <class>
struct data_traits {
  using type = std::false_type;
};

// NOLINTNEXTLINE
#define VAST_DATA_TRAIT(name)                                                  \
  template <>                                                                  \
  struct data_traits<name> {                                                   \
    using type = name##_type;                                                  \
  }

VAST_DATA_TRAIT(bool);
VAST_DATA_TRAIT(integer);
VAST_DATA_TRAIT(count);
VAST_DATA_TRAIT(real);
VAST_DATA_TRAIT(duration);
VAST_DATA_TRAIT(time);
VAST_DATA_TRAIT(pattern);
VAST_DATA_TRAIT(address);
VAST_DATA_TRAIT(subnet);
VAST_DATA_TRAIT(enumeration);
VAST_DATA_TRAIT(list);
VAST_DATA_TRAIT(map);
VAST_DATA_TRAIT(record);

#undef VAST_DATA_TRAIT

template <>
struct data_traits<caf::none_t> {
  using type = none_type;
};

template <>
struct data_traits<std::string> {
  using type = string_type;
};

template <>
struct data_traits<data> {
  using type = vast::type;
};

/// @relates data type
template <class T>
using data_to_type = typename data_traits<T>::type;

/// @returns `true` if *x is a *basic* data.
/// @relates data
bool is_basic(const data& x);

/// @returns `true` if *x is a *complex* data.
/// @relates data
bool is_complex(const data& x);

/// @returns `true` if *x is a *recursive* data.
/// @relates data
bool is_recursive(const data& x);

/// @returns `true` if *x is a *container* data.
/// @relates data
bool is_container(const data& x);

/// @returns The maximum nesting depth any field in the record `r`.
size_t depth(const record& r);

/// Creates a record instance for a given record type. The number of data
/// instances must correspond to the number of fields in the flattened version
/// of the record.
/// @param rt The record type
/// @param xs The record fields.
/// @returns A record according to the fields as defined in *rt*.
caf::optional<record>
make_record(const record_type& rt, std::vector<data>&& xs);

/// Flattens a record recursively.
record flatten(const record& r);

/// Flattens a record recursively according to a record type such
/// that only nested records are lifted into parent list.
/// @param r The record to flatten.
/// @param rt The record type according to which *r* should be flattened.
/// @returns The flattened record if the nested structure of *r* is a valid
///          subset of *rt*.
/// @see unflatten
caf::optional<record> flatten(const record& r, const record_type& rt);
caf::optional<data> flatten(const data& x, const type& t);

/// Unflattens a flattened record.
record unflatten(const record& r);

/// Unflattens a record according to a record type such that the record becomes
/// a recursive structure.
/// @param r The record to unflatten according to *rt*.
/// @param rt The type that defines the record structure.
/// @returns The unflattened record of *r* according to *rt*.
/// @see flatten
caf::optional<record> unflatten(const record& r, const record_type& rt);
caf::optional<data> unflatten(const data& x, const type& t);

/// Merges one record into another such that the source overwrites potential
/// keys in the destination.
/// @param src The record to merge into *dst*.
/// @param dst The result record containing the merged result.
void merge(const record& src, record& dst);

/// Evaluates a data predicate.
/// @param lhs The LHS of the predicate.
/// @param op The relational operator.
/// @param rhs The RHS of the predicate.
bool evaluate(const data& lhs, relational_operator op, const data& rhs);

// -- convertible -------------------------------------------------------------

template <class T, class... Opts>
data to_data(const T& x, Opts&&... opts) {
  data d;
  if (convert(x, d, std::forward<Opts>(opts)...))
    return d;
  return {};
}

caf::error convert(const record& xs, caf::dictionary<caf::config_value>& ys);
caf::error convert(const record& xs, caf::config_value& cv);
caf::error convert(const data& d, caf::config_value& cv);

bool convert(const caf::dictionary<caf::config_value>& xs, record& ys);
bool convert(const caf::dictionary<caf::config_value>& xs, data& y);
bool convert(const caf::config_value& x, data& y);

// -- JSON -------------------------------------------------------------

/// Prints data as JSON.
/// @param x The data instance.
/// @returns The JSON representation of *x*, or an error.
caf::expected<std::string> to_json(const data& x);

// -- YAML -------------------------------------------------------------

/// Parses YAML into a data.
/// @param str The string containing the YAML content
/// @returns The parsed YAML as data, or an error.
caf::expected<data> from_yaml(std::string_view str);

/// Loads YAML from a file.
/// @param file The file to load.
/// @returns The parsed YAML or an error.
caf::expected<data> load_yaml(const std::filesystem::path& file);

/// Loads all *.yml and *.yaml files in a given directory.
/// @param dir The directory to traverse recursively.
/// @param max_recursion The maximum number of nested directories to traverse
/// before aborting.
/// @returns The parsed YAML, one `data` instance per file, or an error.
caf::expected<std::vector<std::pair<std::filesystem::path, data>>>
load_yaml_dir(const std::filesystem::path& dir, size_t max_recursion
                                                = defaults::max_recursion);

/// Prints data as YAML.
/// @param x The data instance.
/// @returns The YAML representation of *x*, or an error.
caf::expected<std::string> to_yaml(const data& x);

} // namespace vast

namespace caf {

template <>
struct sum_type_access<vast::data> : default_sum_type_access<vast::data> {};

} // namespace caf

namespace std {

template <>
struct hash<vast::data> {
  size_t operator()(const vast::data& x) const {
    return vast::uhash<vast::xxhash>{}(x);
  }
};

} // namespace std

namespace fmt {

// Specialization which implements formatting of `<data,data>` pair.
template <>
struct formatter<vast::map::value_type> : vast::detail::empty_formatter_base {
  template <class ValueType, class FormatContext>
  auto format(const ValueType& v, FormatContext& ctx) {
    return fmt::format_to(ctx.out(), "{} -> {}", v.first, v.second);
  }
};

// Specialization which implements formatting of `<string,data>` pair.
template <>
struct formatter<vast::record::value_type>
  : vast::detail::empty_formatter_base {
  template <class ValueType, class FormatContext>
  auto format(const ValueType& v, FormatContext& ctx) {
    return fmt::format_to(ctx.out(), "{}: {}", v.first, v.second);
  }
};

/// Definition of fmt-formatting rules for vast::data.
///
/// @note This class is also reused by formatter class for
///       `vast::view<vast::data>`. And a part that is
///       sensetive to changes is a set of static functions with
///       explicit "this" parameter type of comes as template parameter.
///
/// Visitors doing rendering to a specific format for some of its
/// functions (for instance those which handle containers) apply an idiom
/// which can be called explicit templated this. This idiom helps visitors
/// to be reusable with a similar formatter for @c vast::view<data> type.
/// Let's refer to `json_visitor::format_list` as an example
/// it is an implementation of visiting `vast::list`.
/// It has plenty of logic in it and this logic
/// should be preserved unchanged for the corresponding view type
/// @c `vast::view<vast::list>`. Formatter for "view" types lives separately
/// and visitors in this formatter are not capable of handling view types
/// (or more precise: of handling view types that are of a different type
/// than a type wrapped to `vast::view<T>` in terms of C++ types).
/// That draws the following issue when it comes to reusing:
/// certainly we don't want to reimplement all the logic, so an obvious
/// approach would be using inheritance and adding some more visit handlers
/// for types which are not originally supported by the base.
/// But the view wrapper introduces nothing new in the domain
/// of how the things should be formatted however in terms of C++ types
/// it does. So having just an inheritance puts you in a position where
/// new visit handlers should be added while it has nothing to do with any
/// new formatting logic. This is where "explicit templated this" helps,
/// it allows implementing formatting logic the way it would be reusable
/// by ancestors giving them true behavior of its own this type
/// (and no polymorphism takes place).
/// It requires an extra function which is a reasonable trade-off.
template <>
struct formatter<vast::data> : public vast::detail::vast_formatter_base {
  using ascii_escape_string
    = vast::detail::escaped_string_view<vast::detail::print_escaper_functor>;
  using json_escape_string
    = vast::detail::escaped_string_view<vast::detail::json_escaper_functor>;

  template <typename Output>
  struct ascii_visitor {
    Output out_;

    ascii_visitor(Output out) : out_{std::move(out)} {
    }

    auto operator()(caf::none_t) {
      return format_to(out_, "nil");
    }
    auto operator()(bool b) {
      return format_to(out_, b ? "T" : "F");
    }
    auto operator()(vast::duration d) {
      return format_to(out_, "{}",
                       vast::detail::fmt_wrapped<vast::duration>{d});
    }
    auto operator()(const vast::time& t) {
      return format_to(out_, "{}", vast::detail::fmt_wrapped<vast::time>{t});
    }
    auto operator()(const std::string& s) {
      return (*this)(ascii_escape_string{s});
    }
    auto operator()(const std::string_view& s) {
      return (*this)(ascii_escape_string{s});
    }
    auto operator()(const vast::list& xs) {
      return format_to(out_, "[{}]", ::fmt::join(xs, ", "));
    }
    auto operator()(const vast::map& xs) {
      return format_to(out_, "{{{}}}", ::fmt::join(xs, ", "));
    }
    auto operator()(const vast::record& xs) {
      return format_to(out_, "<{}>", ::fmt::join(xs, ", "));
    }
    template <class T>
    auto operator()(const T& x) {
      return format_to(out_, "{}", x);
    }
  };

  template <typename Output, class PrintTraits>
  struct json_visitor {
    Output out_;
    PrintTraits print_traits_;

    json_visitor(Output out, PrintTraits print_traits)
      : out_{out}, print_traits_{std::move(print_traits)} {
    }

    auto operator()(caf::none_t) {
      return format_to(out_, "null");
    }
    auto operator()(bool b) {
      return format_to(out_, b ? "true" : "false");
    }
    auto operator()(vast::duration d) {
      return format_to(out_, "\"{}\"",
                       vast::detail::fmt_wrapped<vast::duration>{d});
    }
    auto operator()(const vast::time& t) {
      return format_to(out_, "\"{}\"",
                       vast::detail::fmt_wrapped<vast::time>{t});
    }
    auto operator()(const std::string& s) {
      return (*this)(json_escape_string{s});
    }

    template <class T>
    auto operator()(const T& x) {
      return format_to(out_, "{}", x);
    }

    template <class This, class L>
    static auto format_list(This& self, const L& xs) {
      *self.out_++ = '[';
      if (!xs.empty()) {
        self.print_traits_.inc_indent();
        bool is_first = true;
        for (const auto& x : xs) {
          if (is_first) {
            is_first = false;
            self.print_traits_.format_indent_before_first_item(self.out_);
          } else {
            *self.out_++ = ',';
            self.print_traits_.format_indent(self.out_);
          }
          self.out_ = caf::visit(self, x);
        }
        self.print_traits_.dec_indent();
        self.print_traits_.format_indent_after_last_item(self.out_);
      }
      *self.out_ = ']';
      return self.out_;
    }
    auto operator()(const vast::list& xs) {
      return format_list(*this, xs);
    }

    template <class This, class M>
    static auto format_map(This& self, const M& xs) {
      *self.out_++ = '[';
      if (!xs.empty()) {
        self.print_traits_.inc_indent();
        bool is_first = true;
        for (const auto& x : xs) {
          if (is_first) {
            is_first = false;
            self.print_traits_.format_indent_before_first_item(self.out_);
          } else {
            *self.out_++ = ',';
            self.print_traits_.format_indent(self.out_);
          }
          *self.out_++ = '{';
          self.print_traits_.inc_indent();
          self.print_traits_.format_indent_before_first_item(self.out_);

          self.print_traits_.format_field_start(self.out_, "key");
          self.out_ = caf::visit(self, x.first);
          *self.out_++ = ',';
          self.print_traits_.format_indent(self.out_);
          self.print_traits_.format_field_start(self.out_, "value");
          self.out_ = caf::visit(self, x.second);

          self.print_traits_.dec_indent();
          self.print_traits_.format_indent_after_last_item(self.out_);
          *self.out_++ = '}';
        }
        self.print_traits_.dec_indent();
        self.print_traits_.format_indent_after_last_item(self.out_);
      }
      *self.out_ = ']';
      return self.out_;
    }
    auto operator()(const vast::map& xs) {
      return format_map(*this, xs);
    }

    template <class This, class R>
    static auto format_record(This& self, const R& xs) {
      *self.out_++ = '{';
      if (!xs.empty()) {
        self.print_traits_.inc_indent();
        bool is_first = true;
        for (const auto& x : xs) {
          if (is_first) {
            is_first = false;
            self.print_traits_.format_indent_before_first_item(self.out_);
          } else {
            *self.out_++ = ',';
            self.print_traits_.format_indent(self.out_);
          }
          self.print_traits_.format_field_start(self.out_, x.first);
          self.out_ = caf::visit(self, x.second);
        }
        self.print_traits_.dec_indent();
        self.print_traits_.format_indent_after_last_item(self.out_);
      }
      *self.out_ = '}';
      return self.out_;
    }
    auto operator()(const vast::record& xs) {
      return format_record(*this, xs);
    }
  };

  template <bool RemoveSpaces>
  struct ndjson_print_traits {
    template <class... Ts>
    constexpr void dec_indent(Ts&&...) const noexcept {
    }
    template <class... Ts>
    constexpr void inc_indent(Ts&&...) const noexcept {
    }
    template <class Output>
    constexpr void format_indent_before_first_item(Output&) const noexcept {
    }
    template <class Output>
    constexpr void format_indent_after_last_item(Output&) const noexcept {
    }
    template <class Output>
    constexpr void format_indent(Output& out) const {
      if constexpr (!RemoveSpaces)
        *out++ = ' ';
    }
    template <class Output>
    void format_field_start(Output& out, std::string_view name) {
      out = format_to(out,
                      RemoveSpaces ? "{}:" : "{}: ", json_escape_string{name});
    }
  };

  struct json_print_traits {
    int indent_size;
    int current_indent{0};

    constexpr void dec_indent() noexcept {
      --current_indent;
      assert(current_indent >= 0);
    }
    constexpr void inc_indent() noexcept {
      ++current_indent;
    }
    template <class Output>
    constexpr void format_indent_before_first_item(Output& out) const {
      format_indent(out);
    }
    template <class Output>
    constexpr void format_indent_after_last_item(Output& out) const {
      format_indent(out);
    }
    template <class Output>
    constexpr void format_indent(Output& out) const {
      out = format_to(out, "\n{:<{}}", "", current_indent * indent_size);
    }
    template <class Output>
    void format_field_start(Output& out, std::string_view name) {
      out = format_to(out, "{}: ", json_escape_string{name});
    }
  };

  struct yaml_visitor {
    yaml_visitor(YAML::Emitter& out) : out_{out} {
    }

    YAML::Emitter& out_;
    auto operator()(caf::none_t) {
      out_ << YAML::Null;
    }
    auto operator()(bool x) {
      out_ << (x ? "true" : "false");
    }
    auto operator()(vast::integer x) {
      out_ << x;
    }
    auto operator()(vast::count x) {
      out_ << x;
    }
    auto operator()(vast::real x) {
      out_ << to_string(x);
    }
    auto operator()(vast::duration x) {
      out_ << to_string(vast::detail::fmt_wrapped<vast::duration>{x});
    }
    auto operator()(vast::time x) {
      out_ << to_string(vast::detail::fmt_wrapped<vast::time>{x});
    }
    auto operator()(const std::string& x) {
      out_ << x;
    }
    auto operator()(std::string_view x) {
      out_ << to_string(x);
    }
    auto operator()(const vast::pattern& x) {
      out_ << to_string(x);
    }
    auto operator()(const vast::address& x) {
      out_ << to_string(x);
    }
    auto operator()(const vast::subnet& x) {
      out_ << to_string(x);
    }
    auto operator()(const vast::enumeration& x) {
      out_ << to_string(x);
    }

    template <class This, class L>
    static auto format_list(This& self, const L& xs) {
      self.out_ << YAML::BeginSeq;
      for (const auto& x : xs)
        caf::visit(self, x);
      self.out_ << YAML::EndSeq;
    }
    auto operator()(const vast::list& xs) {
      format_list(*this, xs);
    }

    template <class This, class M>
    static auto format_map(This& self, const M& xs) {
      self.out_ << YAML::BeginMap;
      for (const auto& [k, v] : xs) {
        self.out_ << YAML::Key;
        caf::visit(self, k);
        self.out_ << YAML::Value;
        caf::visit(self, v);
      }
      self.out_ << YAML::EndMap;
    }
    auto operator()(const vast::map& xs) {
      format_map(*this, xs);
    };

    template <class This, class R>
    static auto format_record(This& self, const R& xs) {
      self.out_ << YAML::BeginMap;
      for (const auto& [k, v] : xs) {
        self.out_ << YAML::Key << to_string(k) << YAML::Value;
        caf::visit(self, v);
      }
      self.out_ << YAML::EndMap;
    }

    auto operator()(const vast::record& xs) {
      format_record(*this, xs);
    }
  };

  template <class This, class Data, class FormatContext>
  static auto format_impl(This& self, const Data& x, FormatContext& ctx) {
    using output_type = std::decay_t<decltype(ctx.out())>;
    auto do_format = [&x](auto f) { return caf::visit(f, x); };
    if (self.presentation == 'a') {
      return do_format(
        typename This::template ascii_visitor<output_type>{ctx.out()});
    } else if (self.presentation == 'j') {
      if (self.ndjson) {
        if (self.remove_spaces)
          return do_format(
            typename This::template json_visitor<
              output_type, ndjson_print_traits<true>>{ctx.out(), {}});
        else
          return do_format(
            typename This::template json_visitor<
              output_type, ndjson_print_traits<false>>{ctx.out(), {}});
      } else
        return do_format(
          typename This::template json_visitor<output_type, json_print_traits>{
            ctx.out(), {self.indent}});
    } else if (self.presentation == 'y') {
      // YAML visitor cannot stream data to fmt output directly
      // that is why we first collect the output in visitor
      // and then copy it to fmt output.
      YAML::Emitter out;
      out.SetOutputCharset(YAML::EscapeNonAscii);
      out.SetIndent(self.indent);
      typename This::yaml_visitor f{out};
      caf::visit(f, x);
      if (out.good())
        return format_to(ctx.out(), "{}",
                         std::string_view{out.c_str(), out.size()});
      throw fmt::format_error("yaml format failed");
    }
    return ctx.out();
  }

  template <class FormatContext>
  auto format(const vast::data& x, FormatContext& ctx) const {
    return format_impl(*this, x, ctx);
  }
};

} // namespace fmt
