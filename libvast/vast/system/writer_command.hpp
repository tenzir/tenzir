// SPDX-FileCopyrightText: (c) 2018 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/command.hpp"

#include <string_view>

namespace vast::system {

command::fun make_writer_command(std::string_view format);

} // namespace vast::system
