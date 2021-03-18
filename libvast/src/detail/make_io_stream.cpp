// SPDX-FileCopyrightText: (c) 2017 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/make_io_stream.hpp"

#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/fdinbuf.hpp"
#include "vast/detail/fdostream.hpp"
#include "vast/detail/posix.hpp"
#include "vast/error.hpp"
#include "vast/path.hpp"

#include <caf/config_value.hpp>
#include <caf/settings.hpp>

#include <fstream>

namespace vast {
namespace detail {

caf::expected<std::unique_ptr<std::istream>>
make_input_stream(const std::string& input, path::type pt) {
  struct owning_istream : public std::istream {
    owning_istream(std::unique_ptr<std::streambuf>&& ptr)
      : std::istream{ptr.release()} {
      // nop
    }
    ~owning_istream() {
      delete rdbuf();
    }
  };
  switch (pt) {
    default:
      return caf::make_error(ec::filesystem_error, "unsupported path type",
                             input);
    case path::socket: {
      if (input == "-")
        return caf::make_error(ec::filesystem_error, "cannot use STDIN as UNIX "
                                                     "domain socket");
      auto uds = unix_domain_socket::connect(input);
      if (!uds)
        return caf::make_error(ec::filesystem_error,
                               "failed to connect to UNIX domain socket at",
                               input);
      auto remote_fd = uds.recv_fd(); // Blocks!
      auto sb = std::make_unique<fdinbuf>(remote_fd);
      return std::make_unique<owning_istream>(std::move(sb));
    }
    case path::fifo: { // TODO
      return caf::make_error(ec::unimplemented, "make_input_stream does not "
                                                "support fifo yet");
    }
    case path::regular_file: {
      if (input == "-") {
        auto sb = std::make_unique<fdinbuf>(0); // stdin
        return std::make_unique<owning_istream>(std::move(sb));
      }
      if (!exists(input))
        return caf::make_error(ec::filesystem_error, "file does not exist at",
                               input);
      auto fb = std::make_unique<std::filebuf>();
      fb->open(input, std::ios_base::binary | std::ios_base::in);
      return std::make_unique<owning_istream>(std::move(fb));
    }
  }
}

caf::expected<std::unique_ptr<std::ostream>>
make_output_stream(const std::string& output, socket_type st) {
  if (output == "-")
    return caf::make_error(ec::filesystem_error, "cannot use STDOUT as UNIX "
                                                 "domain socket");
  auto connect_st = st;
  if (connect_st == socket_type::fd)
    connect_st = socket_type::stream;
  auto uds = unix_domain_socket::connect(output, st);
  if (!uds)
    return caf::make_error(ec::filesystem_error,
                           "failed to connect to UNIX domain socket at",
                           output);
  auto remote_fd = uds.fd;
  if (st == socket_type::fd)
    remote_fd = uds.recv_fd();
  return std::make_unique<fdostream>(remote_fd);
}

caf::expected<std::unique_ptr<std::ostream>>
make_output_stream(const std::string& output, path::type pt) {
  switch (pt) {
    default:
      return caf::make_error(ec::filesystem_error, "unsupported path type",
                             output);
    case path::socket:
      return caf::make_error(ec::filesystem_error, "wrong overload for socket");
    case path::fifo: // TODO
      return caf::make_error(ec::unimplemented, "make_output_stream does not "
                                                "support fifo yet");
    case path::regular_file: {
      if (output == "-")
        return std::make_unique<fdostream>(1); // stdout
      return std::make_unique<std::ofstream>(output);
    }
  }
}

} // namespace detail
} // namespace vast
