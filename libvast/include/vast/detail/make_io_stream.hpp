//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2017 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/defaults.hpp"
#include "vast/detail/posix.hpp"

#include <caf/expected.hpp>
#include <caf/fwd.hpp>

#include <filesystem>
#include <iostream>
#include <memory>
#include <string>

namespace vast::detail {

caf::expected<std::unique_ptr<std::ostream>>
make_output_stream(const std::string& output, socket_type st);

caf::expected<std::unique_ptr<std::ostream>>
make_output_stream(const std::string& output,
                   std::filesystem::file_type file_type
                   = std::filesystem::file_type::regular,
                   std::ios_base::openmode mode = std::ios_base::out);

caf::expected<std::unique_ptr<std::ostream>>
make_output_stream(const caf::settings& options);

caf::expected<std::unique_ptr<std::istream>>
make_input_stream(const std::string& input,
                  std::filesystem::file_type file_type
                  = std::filesystem::file_type::regular);

caf::expected<std::unique_ptr<std::istream>>
make_input_stream(const caf::settings& options);

} // namespace vast::detail
