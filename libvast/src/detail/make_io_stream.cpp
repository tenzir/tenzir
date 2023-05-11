//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2017 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/make_io_stream.hpp"

#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/fdinbuf.hpp"
#include "vast/detail/fdostream.hpp"
#include "vast/detail/posix.hpp"
#include "vast/error.hpp"

#include <caf/config_value.hpp>
#include <caf/settings.hpp>
#include <fmt/format.h>

#include <filesystem>
#include <fstream>

namespace vast::detail {

caf::expected<std::unique_ptr<std::istream>>
make_input_stream(const std::string& input,
                  std::filesystem::file_type file_type) {
  struct owning_istream : public std::istream {
    owning_istream(std::unique_ptr<std::streambuf>&& ptr)
      : std::istream{ptr.release()} {
      // nop
    }
    ~owning_istream() {
      delete rdbuf();
    }
  };
  switch (file_type) {
    default:
      return caf::make_error(ec::filesystem_error, "unsupported path type",
                             input);
    case std::filesystem::file_type::socket: {
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
    case std::filesystem::file_type::fifo: { // TODO
      return caf::make_error(ec::unimplemented, "make_input_stream does not "
                                                "support fifo yet");
    }
    case std::filesystem::file_type::regular: {
      if (input == "-") {
        auto sb = std::make_unique<fdinbuf>(0); // stdin
        return std::make_unique<owning_istream>(std::move(sb));
      }
      std::error_code err{};
      const auto input_exists
        = std::filesystem::exists(std::filesystem::path{input}, err);
      if (err)
        return caf::make_error(ec::filesystem_error,
                               fmt::format("failed to check if path {} "
                                           "exists: {}",
                                           input, err.message()));
      if (!input_exists)
        return caf::make_error(ec::filesystem_error, "file does not exist at",
                               input);
      auto fb = std::make_unique<std::filebuf>();
      fb->open(input, std::ios_base::binary | std::ios_base::in);
      return std::make_unique<owning_istream>(std::move(fb));
    }
  }
}

caf::expected<std::unique_ptr<std::istream>>
make_input_stream(const caf::settings& options) {
  auto input
    = get_or(options, "vast.import.read", defaults::import::read.data());
  auto uds = get_or(options, "vast.import.uds", false);
  auto fifo = get_or(options, "vast.import.fifo", false);
  const auto pt = uds ? std::filesystem::file_type::socket
                      : (fifo ? std::filesystem::file_type::fifo
                              : std::filesystem::file_type::regular);
  return make_input_stream(std::string{input}, pt);
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
  // TODO
  VAST_DIAGNOSTIC_PUSH
  VAST_DIAGNOSTIC_IGNORE_DEPRECATED
  return std::make_unique<fdostream>(remote_fd);
  VAST_DIAGNOSTIC_POP
}

caf::expected<std::unique_ptr<std::ostream>>
make_output_stream(const std::string& output,
                   std::filesystem::file_type file_type,
                   std::ios_base::openmode mode) {
  switch (file_type) {
    default:
      return caf::make_error(ec::filesystem_error, "unsupported path type",
                             output);
    case std::filesystem::file_type::socket:
      return caf::make_error(ec::filesystem_error, "wrong overload for socket");
    case std::filesystem::file_type::fifo: // TODO
      return caf::make_error(ec::unimplemented, "make_output_stream does not "
                                                "support fifo yet");
    case std::filesystem::file_type::regular: {
      if (output == "-") {
        // TODO
        VAST_DIAGNOSTIC_PUSH
        VAST_DIAGNOSTIC_IGNORE_DEPRECATED
        return std::make_unique<fdostream>(1); // stdout
        VAST_DIAGNOSTIC_POP
      }
      return std::make_unique<std::ofstream>(output, mode);
    }
  }
}

caf::expected<std::unique_ptr<std::ostream>>
make_output_stream(const caf::settings& options) {
  auto output
    = get_or(options, "vast.export.write", defaults::export_::write.data());
  auto uds = get_or(options, "vast.export.uds", false);
  auto fifo = get_or(options, "vast.export.fifo", false);
  const auto pt = uds ? std::filesystem::file_type::socket
                      : (fifo ? std::filesystem::file_type::fifo
                              : std::filesystem::file_type::regular);
  return make_output_stream(std::string{output}, pt);
}

} // namespace vast::detail
