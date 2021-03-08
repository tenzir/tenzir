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

#include "vast/fwd.hpp"

#include "vast/aliases.hpp"
#include "vast/data.hpp"
#include "vast/detail/escapers.hpp"
#include "vast/view.hpp"

#include <chrono>
#include <string_view>
#include <type_traits>

namespace vast::detail {

/// A base class for providing parsing vast types formatting options.
struct vast_formatter_base {
  char presentation = 'a';
  bool ndjson = false;
  bool remove_spaces = false;
  int indent = 2;

  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    auto it = std::begin(ctx);
    auto end = std::end(ctx);
    if (it != end) {
      if (*it == 'a' || *it == 'y') {
        presentation = *it;
        ++it;
      } else if (*it == 'j') {
        presentation = 'j';
        if (++it != end && *it != '}') {
          // JSON format is the following:
          // 1. "{:j[n[r]]}" this stands for NDJSON and optionl `r`
          //    stands for removing spaces.
          // 2. "{:j[i][NN]}" Indented multiline JSON, the size of indent
          //    by default is 2, and can be set explicitly with 2 digit integer,
          //    indentation greater than 100 make no sense.
          if (*it == 'n') {
            ndjson = true;
            if (++it != end) {
              if ((remove_spaces = *it == 'r'))
                ++it;
            }
          } else if (*it == 'i' || std::isdigit(*it)) {
            if (*it == 'i')
              ++it;
            if (it != end && std::isdigit(*it)) {
              indent = *it - '0';
              if (++it != end && std::isdigit(*it)) {
                indent = indent * 10 + (*it - '0');
                ++it;
              }
            }
          }
        }
      }

      // Check for garbage in format-string.
      if (*it != '}')
        throw fmt::format_error("invalid vast::data format-string");
    }
    return it;
  }
};

struct empty_formatter_base {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return std::end(ctx);
  }
};

/// An escaper functor for ascii formatting type.
struct print_escaper_functor {
  template <class F, class O>
  auto operator()(F& f, O out) const {
    print_escaper(f, out);
  }
};

/// An escaper functor for json formatting type.
struct json_escaper_functor {
  template <class F, class O>
  auto operator()(F& f, O out) const {
    json_escaper(f, out);
  }
};

// A separate type-introducing wrapper class for strings.
// A proper formatting is defined by escaper.
template <class Escaper>
struct escaped_string_view {
  std::string_view str;

  template <typename FormatContext>
  auto format(FormatContext& ctx) const {
    Escaper e{};
    auto out = ctx.out();
    *out++ = '"';
    auto f = str.begin();
    auto l = str.end();
    while (f != l) {
      e(f, out);
    }
    *out++ = '"';
    return out;
  }
};

/// A wrapper class for vast types which are not exclusively owned by vast
/// (e.g. duration which is eventually `std::chrono::duration`).
/// It is essential not to introduce a global custom formatting for such types
/// as it might lead to ambiguity compiler errors or in reverse inderectly
/// make a non vast owned code to work by providing fmt integrations,
/// which wouldn't be result of a conscious decision.
template <class T>
struct fmt_wrapped {
  T value;
};

template <class T>
struct fmt_wrapped_formatter;

/// Specialization which knows how to format duration type.
template <>
struct fmt_wrapped_formatter<duration> : public empty_formatter_base {
  template <class To, class R, class P>
  static auto is_at_least(std::chrono::duration<R, P> d) {
    return std::chrono::duration_cast<To>(std::chrono::abs(d)) >= To{1};
  }

  template <class To, class R, class P>
  static auto count(std::chrono::duration<R, P> d) {
    using fractional = std::chrono::duration<double, typename To::period>;
    return std::llround(100 * std::chrono::duration_cast<fractional>(d).count())
           / double{100.0};
  }

  template <typename FormatContext>
  auto format(duration d, FormatContext& ctx) const {
    using namespace std::chrono;
    if (is_at_least<days>(d))
      return fmt::format_to(ctx.out(), "{}d", count<days>(d));
    if (is_at_least<hours>(d))
      return fmt::format_to(ctx.out(), "{}h", count<hours>(d));
    if (is_at_least<minutes>(d))
      return fmt::format_to(ctx.out(), "{}m", count<minutes>(d));
    if (is_at_least<seconds>(d))
      return fmt::format_to(ctx.out(), "{}s", count<seconds>(d));
    if (is_at_least<milliseconds>(d))
      return fmt::format_to(ctx.out(), "{}ms", count<milliseconds>(d));
    if (is_at_least<microseconds>(d))
      return fmt::format_to(ctx.out(), "{}us", count<microseconds>(d));
    return fmt::format_to(ctx.out(), "{}ns", count<nanoseconds>(d));
  }
};

template <class Clock, class Duration>
struct fmt_wrapped_formatter<std::chrono::time_point<Clock, Duration>>
  : public empty_formatter_base {
  using time_point = std::chrono::time_point<Clock, Duration>;

  struct year_month_day {
    unsigned short year;
    unsigned char month;
    unsigned char day;
  };

  // Logic extracted from
  // https://github.com/HowardHinnant/date/blob/master/include/date/date.h
  // An explanation for this algorithm can be found here:
  // http://howardhinnant.github.io/date_algorithms.html#civil_from_days
  static constexpr year_month_day from_days(days dp) noexcept {
    static_assert(std::numeric_limits<unsigned>::digits >= 18,
                  "This algorithm has not been ported to a 16 bit unsigned "
                  "integer");
    static_assert(std::numeric_limits<int>::digits >= 20,
                  "This algorithm has not been ported to a 16 bit signed "
                  "integer");
    auto const z = dp.count() + 719468;
    auto const era = (z >= 0 ? z : z - 146096) / 146097;
    auto const doe = static_cast<unsigned>(z - era * 146097);
    auto const yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    auto const y = static_cast<days::rep>(yoe) + era * 400;
    auto const doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    auto const mp = (5 * doy + 2) / 153;
    auto const d = static_cast<unsigned char>(doy - (153 * mp + 2) / 5 + 1);
    auto const m = static_cast<unsigned char>(mp < 10 ? mp + 3 : mp - 9);
    return year_month_day{static_cast<unsigned short>(y + (m <= 2)), m, d};
  }

  template <typename FormatContext>
  auto format(time_point tp, FormatContext& ctx) const {
    using namespace std::chrono;

    auto sd = floor<days>(tp);
    auto [Y, M, D] = from_days(duration_cast<days>(sd - time{}));
    auto t = tp - sd;
    auto h = duration_cast<hours>(t);
    auto m = duration_cast<minutes>(t - h);
    auto s = duration_cast<seconds>(t - h - m);
    auto sub_secs = duration_cast<nanoseconds>(t - h - m - s).count();

    auto out
      = fmt::format_to(ctx.out(), "{:02}-{:02}-{:02}T{:02}:{:02}:{:02}",
                       static_cast<int>(Y), static_cast<unsigned>(M),
                       static_cast<unsigned>(D), static_cast<int>(h.count()),
                       static_cast<int>(m.count()),
                       static_cast<int>(s.count()));
    if (sub_secs != 0) {
      *out++ = '.';
      if (sub_secs % 1000000 == 0)
        return fmt::format_to(out, "{:03}", sub_secs / 1000000);
      if (sub_secs % 1000 == 0)
        return fmt::format_to(out, "{:06}", sub_secs / 1000);
      return fmt::format_to(out, "{:09}", sub_secs);
    }
    return out;
  }
};

/// A proxy formatter, that is capable of receiving wrapper type and
/// pass an unwrapped type to bse implementation.
template <class T>
struct fmt_proxy : public fmt_wrapped_formatter<T> {
  using base_type = fmt_wrapped_formatter<T>;
  using wrapped_type = fmt_wrapped<T>;
  using parameter_type
    = std::conditional_t<std::is_trivially_copyable_v<wrapped_type>,
                         wrapped_type, const wrapped_type&>;

  template <typename FormatContext>
  auto format(parameter_type v, FormatContext& ctx) const {
    return base_type::format(v.value, ctx);
  }
};

} // namespace vast::detail

namespace fmt {

/// Custom escape-aware formatting for strings.
template <class Escaper>
struct formatter<vast::detail::escaped_string_view<Escaper>> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) const {
    return std::end(ctx);
  }

  template <typename FormatContext>
  auto format(vast::detail::escaped_string_view<Escaper> s,
              FormatContext& ctx) const {
    return s.format(ctx);
  }
};

/// A definition in `fmt` namespace that makes T formatting recognizable by the
/// fmtlib.
template <class T>
struct formatter<vast::detail::fmt_wrapped<T>>
  : public vast::detail::fmt_proxy<T> {};

/// Custom formatter for `vast::pattern` type.
template <>
struct formatter<vast::pattern> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return std::end(ctx);
  }

  template <class P, class FormatContext>
  auto format(const P& p, FormatContext& ctx) {
    return format_to(ctx.out(), "/{}/", p.string());
  }
};

template <>
struct formatter<vast::view<vast::pattern>> : formatter<vast::pattern> {};

// Specialization which implements formatting of `<data,data>` pair.
template <>
struct formatter<vast::map::value_type> : vast::detail::empty_formatter_base {
  template <class ValueType, class FormatContext>
  auto format(const ValueType& v, FormatContext& ctx) {
    return fmt::format_to(ctx.out(), "{} -> {}", v.first, v.second);
  }
};

template <>
struct formatter<std::pair<vast::view<vast::data>, vast::view<vast::data>>>
  : formatter<vast::map::value_type> {};

// Specialization which implements formatting of `<string,data>` pair.
template <>
struct formatter<vast::record::value_type>
  : vast::detail::empty_formatter_base {
  template <class ValueType, class FormatContext>
  auto format(const ValueType& v, FormatContext& ctx) {
    return fmt::format_to(ctx.out(), "{}: {}", v.first, v.second);
  }
};

template <>
struct formatter<std::pair<std::string_view, vast::view<vast::data>>>
  : formatter<vast::record::value_type> {};

/// Definition of fmt-formatting rules for vast::data.
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

    // bool operator()(const vast::view<vast::data>& x) {
    //   return caf::visit(*this, x);
    // }
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
    auto operator()(const vast::view<vast::list>& xs) {
      return format_to(out_, "[{}]", ::fmt::join(xs, ", "));
    }
    auto operator()(const vast::map& xs) {
      return format_to(out_, "{{{}}}", ::fmt::join(xs, ", "));
    }
    auto operator()(const vast::view<vast::map>& xs) {
      return format_to(out_, "{{{}}}", ::fmt::join(xs, ", "));
    }
    auto operator()(const vast::record& xs) {
      return format_to(out_, "<{}>", ::fmt::join(xs, ", "));
    }
    auto operator()(const vast::view<vast::record>& xs) {
      return format_to(out_, "<{}>", ::fmt::join(xs, ", "));
    }

    template <class T>
    auto operator()(const T& x) {
      return format_to(out_, "{}", x);
    }
  };

  template <typename Output>
  struct json_visitor_base {
    Output out_;
    json_visitor_base(Output out) : out_{std::move(out)} {
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
  };

  template <typename Output, class PrintTraits>
  struct json_visitor : json_visitor_base<Output> {
    using super = json_visitor_base<Output>;
    PrintTraits print_traits_;
    json_visitor(Output out, PrintTraits print_traits)
      : super{out}, print_traits_{std::move(print_traits)} {
    }

    template <class T>
    auto operator()(const T& x) {
      return static_cast<super&>(*this)(x);
    }

    template <class L>
    auto format_list(const L& xs) {
      *this->out_++ = '[';
      if (!xs.empty()) {
        print_traits_.inc_indent();
        bool is_first = true;
        for (const auto& x : xs) {
          if (is_first) {
            is_first = false;
            print_traits_.format_indent_before_first_item(this->out_);
          } else {
            *this->out_++ = ',';
            print_traits_.format_indent(this->out_);
          }
          this->out_ = caf::visit(*this, x);
        }
        print_traits_.dec_indent();
        print_traits_.format_indent_after_last_item(this->out_);
      }
      *this->out_ = ']';
      return this->out_;
    }
    auto operator()(const vast::list& xs) {
      return format_list(xs);
    }
    auto operator()(const vast::view<vast::list>& xs) {
      return format_list(xs);
    }

    template <class M>
    auto format_map(const M& xs) {
      *this->out_++ = '[';
      if (!xs.empty()) {
        print_traits_.inc_indent();
        bool is_first = true;
        for (const auto& x : xs) {
          if (is_first) {
            is_first = false;
            print_traits_.format_indent_before_first_item(this->out_);
          } else {
            *this->out_++ = ',';
            print_traits_.format_indent(this->out_);
          }
          *this->out_++ = '{';
          print_traits_.inc_indent();
          print_traits_.format_indent_before_first_item(this->out_);

          print_traits_.format_field_start(this->out_, "key");
          this->out_ = caf::visit(*this, x.first);
          *this->out_++ = ',';
          print_traits_.format_indent(this->out_);
          print_traits_.format_field_start(this->out_, "value");
          this->out_ = caf::visit(*this, x.second);

          print_traits_.dec_indent();
          print_traits_.format_indent_after_last_item(this->out_);
          *this->out_++ = '}';
        }
        print_traits_.dec_indent();
        print_traits_.format_indent_after_last_item(this->out_);
      }
      *this->out_ = ']';
      return this->out_;
    }
    auto operator()(const vast::map& xs) {
      return format_map(xs);
    }
    auto operator()(const vast::view<vast::map>& xs) {
      return format_map(xs);
    }

    template <class R>
    auto format_record(const R& xs) {
      *this->out_++ = '{';
      if (!xs.empty()) {
        print_traits_.inc_indent();
        bool is_first = true;
        for (const auto& x : xs) {
          if (is_first) {
            is_first = false;
            print_traits_.format_indent_before_first_item(this->out_);
          } else {
            *this->out_++ = ',';
            print_traits_.format_indent(this->out_);
          }
          print_traits_.format_field_start(this->out_, x.first);
          this->out_ = caf::visit(*this, x.second);
        }
        print_traits_.dec_indent();
        print_traits_.format_indent_after_last_item(this->out_);
      }
      *this->out_ = '}';
      return this->out_;
    }
    auto operator()(const vast::record& xs) {
      return format_record(xs);
    }
    auto operator()(const vast::view<vast::record>& xs) {
      return format_record(xs);
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

    template <class... Ts>
    constexpr void dec_indent(Ts&&...) noexcept {
      --current_indent;
    }
    template <class... Ts>
    constexpr void inc_indent(Ts&&...) noexcept {
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

  template <class Data, class FormatContext>
  auto format(const Data& x, FormatContext& ctx) const {
    auto do_format = [&x](auto f) { return caf::visit(f, x); };
    if (presentation == 'a') {
      return do_format(ascii_visitor{ctx.out()});
    } else if (presentation == 'j') {
      if (ndjson) {
        if (remove_spaces)
          return do_format(
            json_visitor{ctx.out(), ndjson_print_traits<true>{}});
        else
          return do_format(
            json_visitor{ctx.out(), ndjson_print_traits<false>{}});
      } else
        return do_format(json_visitor{ctx.out(), json_print_traits{indent}});
    } else if (presentation == 'y') {
    }
    return ctx.out();
  }
};

template <>
struct formatter<vast::view<vast::data>> : formatter<vast::data> {};

} // namespace fmt
