// SPDX-FileCopyrightText: (c) 2018 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/aliases.hpp"

namespace vast::system {

/// Format-independent implementation for import sub-commands.
caf::message pcap_writer_command(const invocation& inv, caf::actor_system& sys);

} // namespace vast::system
