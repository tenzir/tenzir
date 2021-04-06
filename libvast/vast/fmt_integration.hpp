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
#include "vast/detail/escapers.hpp"
#include "vast/time.hpp"

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <cassert>
#include <chrono>
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
      auto parse_indent = [&] {
        if (it != end && std::isdigit(*it)) {
          indent = *it - '0';
          if (++it != end && std::isdigit(*it)) {
            indent = indent * 10 + (*it - '0');
            ++it;
          }
        }
        assert(indent >= 0 && indent <= 99);
      };
      if (*it == 'a') {
        presentation = *it++;
      } else if (*it == 'y') {
        presentation = *it++;
        if (it != end && *it != '}') {
          parse_indent();
        }
      } else if (*it == 'j') {
        presentation = *it++;
        if (it != end && *it != '}') {
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
            parse_indent();
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

/// An auxiliary wrapper for double value representing duration part.
struct duration_double_precision_adjuster {
  double value;
};

struct duration_double_precision_adjuster_formatter
  : public empty_formatter_base {
  template <typename FormatContext>
  auto format(duration_double_precision_adjuster d, FormatContext& ctx) const {
    // To print a float number with precision of at most two digits
    // we print it as fixed 2-digits precision and later
    // look at the last symbol and cut it if it is zero.
    // For instance:
    //   0.00   => 0.0
    //   31.40  => 31.4
    //   271.82 => 271.82
    std::array<char, 64> buf;
    auto last = fmt::format_to(begin(buf), "{:.2f}", d.value);
    return fmt::format_to(
      ctx.out(),
      std::string_view{buf.data(),
                       static_cast<std::size_t>(std::distance(
                         begin(buf), *(last - 1) == '0' ? last - 1 : last))});
  }
};

/// Specialization which knows how to format duration type.
template <>
struct fmt_wrapped_formatter<duration> : public empty_formatter_base {
  template <class To, class R, class P>
  static auto is_at_least(std::chrono::duration<R, P> d) {
    return std::chrono::duration_cast<To>(std::chrono::abs(d)) >= To{1};
  }

  template <class To, class R, class P>
  static double count(std::chrono::duration<R, P> d) {
    using fractional = std::chrono::duration<double, typename To::period>;
    return std::chrono::duration_cast<fractional>(d).count();
  }

  template <typename FormatContext>
  auto format(duration d, FormatContext& ctx) const {
    // To print 1-2 digits precision float number
    // representing duration a @c duration_double_precision_adjuster
    // is used which is capable of printing double in an apropriate way.
    // The reason precision_adjuster is a separate routine and not embedded here
    // is that duration is suffixed with unit tag which would complecate
    // adjustment logic in this function dramaticaly.
    using namespace std::chrono;
    if (is_at_least<days>(d))
      return fmt::format_to(ctx.out(), "{}d",
                            duration_double_precision_adjuster{count<days>(d)});
    if (is_at_least<hours>(d))
      return fmt::format_to(
        ctx.out(), "{}h", duration_double_precision_adjuster{count<hours>(d)});
    if (is_at_least<minutes>(d))
      return fmt::format_to(
        ctx.out(), "{}m",
        duration_double_precision_adjuster{count<minutes>(d)});
    if (is_at_least<seconds>(d))
      return fmt::format_to(
        ctx.out(), "{}s",
        duration_double_precision_adjuster{count<seconds>(d)});
    if (is_at_least<milliseconds>(d))
      return fmt::format_to(
        ctx.out(), "{}ms",
        duration_double_precision_adjuster{count<milliseconds>(d)});
    if (is_at_least<microseconds>(d))
      return fmt::format_to(
        ctx.out(), "{}us",
        duration_double_precision_adjuster{count<microseconds>(d)});
    return fmt::format_to(
      ctx.out(), "{}ns",
      duration_double_precision_adjuster{count<nanoseconds>(d)});
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
struct formatter<vast::detail::escaped_string_view<Escaper>>
  : vast::detail::empty_formatter_base {
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

template <>
struct formatter<vast::detail::duration_double_precision_adjuster>
  : public vast::detail::duration_double_precision_adjuster_formatter {};

} // namespace fmt
