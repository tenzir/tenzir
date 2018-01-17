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

#ifndef VAST_DETAIL_CHRONO_IO_HPP
#define VAST_DETAIL_CHRONO_IO_HPP

// The MIT License (MIT)
//
// Copyright (c) 2016 Howard Hinnant
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// Our apologies.  When the previous paragraph was written, lowercase had not yet
// been invented (that woud involve another several millennia of evolution).
// We did not mean to shout.

// This is almost a verbatim copy from Howard's library, except that we don't
// use the unicode symbol mu for microseconds.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <ratio>
#include <string>
#include <type_traits>

namespace vast {
namespace detail {

namespace date
{

namespace detail
{

template <class CharT, std::size_t N>
class string_literal
{
    CharT p_[N];

public:
    using const_iterator = const CharT*;

    string_literal(string_literal const&) = default;
    string_literal& operator=(string_literal const&) = delete;

    template <std::size_t N1 = 2,
              class = std::enable_if_t<N1 == N>>
    constexpr string_literal(CharT c) noexcept
        : p_{c}
    {
    }

    constexpr string_literal(const CharT(&a)[N]) noexcept
        : p_{}
    {
        for (std::size_t i = 0; i < N; ++i)
            p_[i] = a[i];
    }

    template <class U = CharT, class = std::enable_if_t<1 < sizeof(U)>>
    constexpr string_literal(const char(&a)[N]) noexcept
        : p_{}
    {
        for (std::size_t i = 0; i < N; ++i)
            p_[i] = a[i];
    }

    template <class CharT2, class = std::enable_if_t<!std::is_same<CharT2, CharT>{}>>
    constexpr string_literal(string_literal<CharT2, N> const& a) noexcept
        : p_{}
    {
        for (std::size_t i = 0; i < N; ++i)
            p_[i] = a[i];
    }

    template <std::size_t N1, std::size_t N2,
              class = std::enable_if_t<N1 + N2 - 1 == N>>
    constexpr string_literal(const string_literal<CharT, N1>& x,
                             const string_literal<CharT, N2>& y) noexcept
        : p_{}
    {
        std::size_t i = 0;
        for (; i < N1-1; ++i)
            p_[i] = x[i];
        for (std::size_t j = 0; j < N2; ++j, ++i)
            p_[i] = y[j];
    }

    constexpr const CharT* data() const noexcept {return p_;}
    constexpr std::size_t size() const noexcept {return N-1;}

    constexpr const_iterator begin() const noexcept {return p_;}
    constexpr const_iterator end()   const noexcept {return p_ + N-1;}

    constexpr CharT const& operator[](std::size_t n) const noexcept
    {
        return p_[n];
    }

    template <class Traits>
    friend
    std::basic_ostream<CharT, Traits>&
    operator<<(std::basic_ostream<CharT, Traits>& os, const string_literal& s)
    {
        return os << s.p_;
    }
};

template <class CharT1, class CharT2, std::size_t N1, std::size_t N2>
constexpr
inline
string_literal<std::conditional_t<sizeof(CharT2) <= sizeof(CharT1), CharT1, CharT2>,
               N1 + N2 - 1>
operator+(const string_literal<CharT1, N1>& x, const string_literal<CharT2, N2>& y) noexcept
{
    using CharT = std::conditional_t<sizeof(CharT2) <= sizeof(CharT1), CharT1, CharT2>;
    return string_literal<CharT, N1 + N2 - 1>{string_literal<CharT, N1>{x},
                                              string_literal<CharT, N2>{y}};
}

template <class CharT, std::size_t N>
constexpr
inline
string_literal<CharT, N>
msl(const CharT(&a)[N]) noexcept
{
    return string_literal<CharT, N>{a};
}

template <class CharT,
          class = std::enable_if_t<std::is_same<CharT, char>{} ||
                                   std::is_same<CharT, wchar_t>{} ||
                                   std::is_same<CharT, char16_t>{} ||
                                   std::is_same<CharT, char32_t>{}>>
constexpr
inline
string_literal<CharT, 2>
msl(CharT c) noexcept
{
    return string_literal<CharT, 2>{c};
}

constexpr
std::size_t
to_string_len(std::intmax_t i)
{
    std::size_t r = 0;
    do
    {
        i /= 10;
        ++r;
    } while (i > 0);
    return r;
}

template <std::intmax_t N>
constexpr
inline
std::enable_if_t
<
    N < 10,
    string_literal<char, to_string_len(N)+1>
>
msl() noexcept
{
    return msl(char(N % 10 + '0'));
}

template <std::intmax_t N>
constexpr
inline
std::enable_if_t
<
    10 <= N,
    string_literal<char, to_string_len(N)+1>
>
msl() noexcept
{
    return msl<N/10>() + msl(char(N % 10 + '0'));
}

template <class CharT, std::intmax_t N, std::intmax_t D>
constexpr
inline
std::enable_if_t
<
    std::ratio<N, D>::type::den != 1,
    string_literal<CharT, to_string_len(std::ratio<N, D>::type::num) +
                          to_string_len(std::ratio<N, D>::type::den) + 4>
>
msl(std::ratio<N, D>) noexcept
{
    using R = typename std::ratio<N, D>::type;
    return msl(CharT{'['}) + msl<R::num>() + msl(CharT{'/'}) +
                             msl<R::den>() + msl(CharT{']'});
}

template <class CharT, std::intmax_t N, std::intmax_t D>
constexpr
inline
std::enable_if_t
<
    std::ratio<N, D>::type::den == 1,
    string_literal<CharT, to_string_len(std::ratio<N, D>::type::num) + 3>
>
msl(std::ratio<N, D>) noexcept
{
    using R = typename std::ratio<N, D>::type;
    return msl(CharT{'['}) + msl<R::num>() + msl(CharT{']'});
}

template <class CharT>
constexpr
inline
auto
msl(std::atto) noexcept
{
    return msl(CharT{'a'});
}

template <class CharT>
constexpr
inline
auto
msl(std::femto) noexcept
{
    return msl(CharT{'f'});
}

template <class CharT>
constexpr
inline
auto
msl(std::pico) noexcept
{
    return msl(CharT{'p'});
}

template <class CharT>
constexpr
inline
auto
msl(std::nano) noexcept
{
    return msl(CharT{'n'});
}

// No unicode for us.
template <class CharT>
constexpr
inline
auto
msl(std::micro) noexcept
{
    return msl(CharT{'u'});
}

//template <class CharT>
//constexpr
//inline
//std::enable_if_t
//<
//    std::is_same<CharT, char>{},
//    string_literal<char, 3>
//>
//msl(std::micro) noexcept
//{
//    return string_literal<char, 3>{"\xC2\xB5"};
//}
//
//template <class CharT>
//constexpr
//inline
//std::enable_if_t
//<
//    !std::is_same<CharT, char>{},
//    string_literal<CharT, 2>
//>
//msl(std::micro) noexcept
//{
//    return string_literal<CharT, 2>{CharT{static_cast<unsigned char>('\xB5')}};
//}

template <class CharT>
constexpr
inline
auto
msl(std::milli) noexcept
{
    return msl(CharT{'m'});
}

template <class CharT>
constexpr
inline
auto
msl(std::centi) noexcept
{
    return msl(CharT{'c'});
}

template <class CharT>
constexpr
inline
auto
msl(std::deci) noexcept
{
    return msl(CharT{'d'});
}

template <class CharT>
constexpr
inline
auto
msl(std::deca) noexcept
{
    return string_literal<CharT, 3>{"da"};
}

template <class CharT>
constexpr
inline
auto
msl(std::hecto) noexcept
{
    return msl(CharT{'h'});
}

template <class CharT>
constexpr
inline
auto
msl(std::kilo) noexcept
{
    return msl(CharT{'k'});
}

template <class CharT>
constexpr
inline
auto
msl(std::mega) noexcept
{
    return msl(CharT{'M'});
}

template <class CharT>
constexpr
inline
auto
msl(std::giga) noexcept
{
    return msl(CharT{'G'});
}

template <class CharT>
constexpr
inline
auto
msl(std::tera) noexcept
{
    return msl(CharT{'T'});
}

template <class CharT>
constexpr
inline
auto
msl(std::peta) noexcept
{
    return msl(CharT{'P'});
}

template <class CharT>
constexpr
inline
auto
msl(std::exa) noexcept
{
    return msl(CharT{'E'});
}

template <class CharT, class Rep, class Period>
constexpr
auto
get_units(const std::chrono::duration<Rep, Period>&)
{
    return msl<CharT>(Period{}) + string_literal<CharT, 2>{"s"};
}

template <class CharT, class Rep>
constexpr
auto
get_units(const std::chrono::duration<Rep, std::ratio<1>>&)
{
    return string_literal<CharT, 2>{"s"};
}

template <class CharT, class Rep>
constexpr
auto
get_units(const std::chrono::duration<Rep, std::ratio<60>>&)
{
    return string_literal<CharT, 4>{"min"};
}

template <class CharT, class Rep>
constexpr
auto
get_units(const std::chrono::duration<Rep, std::ratio<3600>>&)
{
    return string_literal<CharT, 2>{"h"};
}

}  // namespace detail

template <class CharT, class Traits, class Rep, class Period>
inline
std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& os,
           const std::chrono::duration<Rep, Period>& d)
{
    using namespace std::chrono;
    return os << d.count()
              << detail::get_units<CharT>(duration<Rep, typename Period::type>{});
}

}  // namespace date

}  // namespace detail
}  // namespace vast

#endif  // VAST_DETAIL_CHRONO_IO_HPP
