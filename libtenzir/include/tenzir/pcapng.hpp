//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <cstdint>

/// PCAPng utilities and data structures as defined in the IETF draft at
/// https://datatracker.ietf.org/doc/draft-tuexen-opsawg-pcapng/. Visit
/// https://pcapng.com/ for a high-level overview about the PCAPng format.
namespace tenzir::pcapng {

/// This field is the Block Type in a Section Header Block (SHB). It also serves
/// as magic number for the PCAPng file format.
constexpr uint32_t magic_number = 0x0a0d0d0a;

} // namespace tenzir::pcapng
