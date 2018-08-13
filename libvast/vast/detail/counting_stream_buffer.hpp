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

#include <cstddef>
#include <streambuf>
#include <string>

namespace vast::detail {

/// A streambuffer delegator that counts the number of bytes written to or read
/// from it.
template <class Streambuf>
class counting_stream_buffer
  : public std::basic_streambuf<typename Streambuf::char_type,
                                typename Streambuf::traits_type> {
public:
  using char_type = typename Streambuf::char_type;
  using traits_type = typename Streambuf::traits_type;
  using int_type = typename Streambuf::int_type;
  using pos_type = typename Streambuf::pos_type;
  using off_type = typename Streambuf::off_type;

  /// Constructs a counting_stream_buffer from another streambuffer.
  /// @param streambuf The streambuffer to delegate operations to.
  explicit counting_stream_buffer(Streambuf& streambuf) : streambuf_{streambuf} {
    // nop
  }

  // -- counters ---------------------------------------------------------------

  /// @returns the number of characters written into the underlying
  /// streambuffer.
  size_t put() const {
    return put_;
  }

  /// @returns the number of characters read from the underlying streambuffer.
  size_t got() const {
    return got_;
  }

  // -- locales ----------------------------------------------------------------

  auto pubimbue(const std::locale& loc) {
    return streambuf_.pubimbue(loc);
  }

  auto getloc() const {
    return streambuf_.getloc();
  }

  // -- positioning ------------------------------------------------------------

  auto pubsetbuf(char_type* s, std::streamsize n) {
    return streambuf_.pubsetbuf(s, n);
  }

  auto pubseekoff(off_type off, std::ios_base::seekdir dir,
                  std::ios_base::openmode which =
                    std::ios_base::in | std::ios_base::out) {
    return streambuf_.pubseekoff(off, dir, which);
  }

  auto pubseekpos(pos_type pos, std::ios_base::openmode which =
                                  std::ios_base::in | std::ios_base::out ) {
    return streambuf_.pubseekpos(pos, which);
  }

  auto pubsync() {
    return streambuf_.pubsync();
  }

  // -- get area ---------------------------------------------------------------

  auto snextc() {
    auto result = streambuf_.snextc();
    if (result != traits_type::eof())
      ++got_;
    return result;
  }

  auto sbumpc() {
    auto result = streambuf_.sbumpc();
    if (result != traits_type::eof())
      ++got_;
    return result;
  }

  [[deprecated]]
  auto stossc() {
    auto result = streambuf_.stossc();
    if (result != traits_type::eof())
      ++got_;
    return result;
  }

  auto sgetc() {
    auto result = streambuf_.sgetc();
    if (result != traits_type::eof())
      ++got_;
    return result;
  }

  auto sgetn(char_type* s, std::streamsize n) {
    auto result = streambuf_.sgetn(s, n);
    got_ += result;
    return result;
  }

  // -- put area ---------------------------------------------------------------

  auto sputc(char_type c) {
    auto result = streambuf_.sputc(c);
    if (result != traits_type::eof())
      ++put_;
    return result;
  }

  auto sputn(const char_type* s, std::streamsize n) {
    auto result = streambuf_.sputn(s, n);
    put_ += result;
    return result;
  }

  // -- putback ----------------------------------------------------------------

  auto sputbackc(char_type c) {
    auto result = streambuf_.sputbackc(c);
    if (result != traits_type::eof())
      --got_;
    return result;
  }

  auto sungetc() {
    auto result = streambuf_.sungetc();
    if (result != traits_type::eof())
      --got_;
    return result;
  }

private:
  Streambuf& streambuf_;
  size_t put_ = 0;
  size_t got_ = 0;
};

} // namespace vast::detail
