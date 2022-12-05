//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

// This file comes from a 3rd party and has been adapted to fit into the VAST
// code base. Details about the original file:
//
// - Repository: https://github.com/actor-framework/actor-framework
// - Commit:     c1a9059d071739ba354d57937da1a5f00e9b304e
// - Path:       libcaf_core/caf/streambuf.hpp
// - Author:     Dominik Charousset
// - Created:    April 14th, 2016 4:48 AM
// - License:    BSD 3-Clause

#pragma once

#include "caf/config.hpp"
#include "caf/detail/type_traits.hpp"

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <limits>
#include <streambuf>
#include <type_traits>
#include <vector>

namespace vast::detail {

/// The base class for all stream buffer implementations.
template <class CharT = char, class Traits = std::char_traits<CharT>>
class stream_buffer : public std::basic_streambuf<CharT, Traits> {
public:
  using base = std::basic_streambuf<CharT, Traits>;
  using pos_type = typename base::pos_type;
  using off_type = typename base::off_type;

protected:
  /// The standard only defines pbump(int), which can overflow on 64-bit
  /// architectures. All stream buffer implementations should therefore use
  /// these function instead. For a detailed discussion, see:
  /// https://gcc.gnu.org/bugzilla/show_bug.cgi?id=47921
  template <class T = int>
  typename std::enable_if<sizeof(T) == 4>::type safe_pbump(std::streamsize n) {
    while (n > std::numeric_limits<int>::max()) {
      this->pbump(std::numeric_limits<int>::max());
      n -= std::numeric_limits<int>::max();
    }
    this->pbump(static_cast<int>(n));
  }

  template <class T = int>
  typename std::enable_if<sizeof(T) == 8>::type safe_pbump(std::streamsize n) {
    this->pbump(static_cast<int>(n));
  }

  // As above, but for the get area.
  template <class T = int>
  typename std::enable_if<sizeof(T) == 4>::type safe_gbump(std::streamsize n) {
    while (n > std::numeric_limits<int>::max()) {
      this->gbump(std::numeric_limits<int>::max());
      n -= std::numeric_limits<int>::max();
    }
    this->gbump(static_cast<int>(n));
  }

  template <class T = int>
  typename std::enable_if<sizeof(T) == 8>::type safe_gbump(std::streamsize n) {
    this->gbump(static_cast<int>(n));
  }

  pos_type default_seekoff(off_type off, std::ios_base::seekdir dir,
                           std::ios_base::openmode which) {
    auto new_off = pos_type(off_type(-1));
    auto get = (which & std::ios_base::in) == std::ios_base::in;
    auto put = (which & std::ios_base::out) == std::ios_base::out;
    if (!(get || put))
      return new_off; // nothing to do
    if (get) {
      switch (dir) {
        default:
          return pos_type(off_type(-1));
        case std::ios_base::beg:
          new_off = 0;
          break;
        case std::ios_base::cur:
          new_off = this->gptr() - this->eback();
          break;
        case std::ios_base::end:
          new_off = this->egptr() - this->eback();
          break;
      }
      new_off += off;
      this->setg(this->eback(), this->eback() + new_off, this->egptr());
    }
    if (put) {
      switch (dir) {
        default:
          return pos_type(off_type(-1));
        case std::ios_base::beg:
          new_off = 0;
          break;
        case std::ios_base::cur:
          new_off = this->pptr() - this->pbase();
          break;
        case std::ios_base::end:
          new_off = this->egptr() - this->pbase();
          break;
      }
      new_off += off;
      this->setp(this->pbase(), this->epptr());
      safe_pbump(new_off);
    }
    return new_off;
  }

  pos_type default_seekpos(pos_type pos, std::ios_base::openmode which) {
    auto get = (which & std::ios_base::in) == std::ios_base::in;
    auto put = (which & std::ios_base::out) == std::ios_base::out;
    if (!(get || put))
      return pos_type(off_type(-1)); // nothing to do
    if (get)
      this->setg(this->eback(), this->eback() + pos, this->egptr());
    if (put) {
      this->setp(this->pbase(), this->epptr());
      safe_pbump(pos);
    }
    return pos;
  }
};

/// A streambuffer abstraction over a fixed array of bytes. This streambuffer
/// cannot overflow/underflow. Once it has reached its end, attempts to read
/// characters will return `trait_type::eof`.
template <class CharT = char, class Traits = std::char_traits<CharT>>
class arraybuf : public stream_buffer<CharT, Traits> {
public:
  using base = std::basic_streambuf<CharT, Traits>;
  using char_type = typename base::char_type;
  using traits_type = typename base::traits_type;
  using int_type = typename base::int_type;
  using pos_type = typename base::pos_type;
  using off_type = typename base::off_type;

  /// Constructs an array streambuffer from a container.
  /// @param c A contiguous container.
  /// @pre `c.data()` must point to a contiguous sequence of characters having
  ///      length `c.size()`.
  template <class Container,
            class = typename std::enable_if<
              caf::detail::has_data_member<Container>::value
              && caf::detail::has_size_member<Container>::value>::type>
  arraybuf(Container& c)
    : arraybuf(const_cast<char_type*>(c.data()), c.size()) {
    // nop
  }

  /// Constructs an array streambuffer from a raw character sequence.
  /// @param data A pointer to the first character.
  /// @param size The length of the character sequence.
  arraybuf(char_type* data, size_t size) {
    setbuf(data, static_cast<std::streamsize>(size));
  }

  // There exists a bug in libstdc++ version < 5: the implementation does not
  // provide  the necessary move constructors, so we have to roll our own :-/.
  // See https://gcc.gnu.org/bugzilla/show_bug.cgi?id=54316 for details.
  // TODO: remove after having raised the minimum GCC version to 5.
  arraybuf(arraybuf&& other) {
    this->setg(other.eback(), other.gptr(), other.egptr());
    this->setp(other.pptr(), other.epptr());
    other.setg(nullptr, nullptr, nullptr);
    other.setp(nullptr, nullptr);
  }

  // TODO: remove after having raised the minimum GCC version to 5.
  arraybuf& operator=(arraybuf&& other) {
    this->setg(other.eback(), other.gptr(), other.egptr());
    this->setp(other.pptr(), other.epptr());
    other.setg(nullptr, nullptr, nullptr);
    other.setp(nullptr, nullptr);
    return *this;
  }

protected:
  // -- positioning ----------------------------------------------------------

  std::basic_streambuf<char_type, Traits>*
  setbuf(char_type* s, std::streamsize n) override {
    this->setg(s, s, s + n);
    this->setp(s, s + n);
    return this;
  }

  pos_type
  seekpos(pos_type pos, std::ios_base::openmode which
                        = std::ios_base::in | std::ios_base::out) override {
    return this->default_seekpos(pos, which);
  }

  pos_type seekoff(off_type off, std::ios_base::seekdir dir,
                   std::ios_base::openmode which) override {
    return this->default_seekoff(off, dir, which);
  }

  // -- put area -------------------------------------------------------------

  std::streamsize xsputn(const char_type* s, std::streamsize n) override {
    auto available = this->epptr() - this->pptr();
    auto actual = std::min(n, static_cast<std::streamsize>(available));
    std::memcpy(this->pptr(), s,
                static_cast<size_t>(actual) * sizeof(char_type));
    this->safe_pbump(actual);
    return actual;
  }

  // -- get area -------------------------------------------------------------

  std::streamsize xsgetn(char_type* s, std::streamsize n) override {
    auto available = this->egptr() - this->gptr();
    auto actual = std::min(n, static_cast<std::streamsize>(available));
    std::memcpy(s, this->gptr(),
                static_cast<size_t>(actual) * sizeof(char_type));
    this->safe_gbump(actual);
    return actual;
  }
};

} // namespace vast::detail
