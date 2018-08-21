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

#include <fstream>

#include "vast/error.hpp"

#include "vast/detail/fdinbuf.hpp"
#include "vast/detail/fdostream.hpp"
#include "vast/detail/make_io_stream.hpp"
#include "vast/detail/posix.hpp"

namespace vast {
namespace detail {

expected<std::unique_ptr<std::istream>>
make_input_stream(const std::string& input, bool is_uds) {
  struct owning_istream : public std::istream {
    owning_istream(std::unique_ptr<std::streambuf>&& ptr)
      : std::istream{ptr.release()} {
      // nop
    }
    ~owning_istream() {
      delete rdbuf();
    }
  };
  if (is_uds) {
    if (input == "-")
      return make_error(ec::filesystem_error,
                        "cannot use stdin as UNIX domain socket");
    auto uds = unix_domain_socket::connect(input);
    if (!uds)
      return make_error(ec::filesystem_error,
                        "failed to connect to UNIX domain socket at", input);
    auto remote_fd = uds.recv_fd(); // Blocks!
    auto sb = std::make_unique<fdinbuf>(remote_fd);
    return std::make_unique<owning_istream>(std::move(sb));
  }
  if (input == "-") {
    auto sb = std::make_unique<fdinbuf>(0); // stdin
    return std::make_unique<owning_istream>(std::move(sb));
  }
  auto fb = std::make_unique<std::filebuf>();
  fb->open(input, std::ios_base::binary | std::ios_base::in);
  return std::make_unique<owning_istream>(std::move(fb));
}

expected<std::unique_ptr<std::ostream>>
make_output_stream(const std::string& output, bool is_uds) {
  if (is_uds) {
      return make_error(ec::filesystem_error,
                        "cannot use stdout as UNIX domain socket");
    auto uds = unix_domain_socket::connect(output);
    if (!uds)
      return make_error(ec::filesystem_error,
                        "failed to connect to UNIX domain socket at", output);
    auto remote_fd = uds.recv_fd(); // Blocks!
    return std::make_unique<fdostream>(remote_fd);
  }
  if (output == "-")
    return std::make_unique<fdostream>(1); // stdout
  return std::make_unique<std::ofstream>(output);
}

} // namespace detail
} // namespace vast
