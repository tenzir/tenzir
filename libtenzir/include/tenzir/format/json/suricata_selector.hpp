//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

namespace tenzir::format::json {

struct suricata_selector {
  static constexpr auto field_name = "event_type";
  static constexpr auto type_prefix = "suricata";
};

} // namespace tenzir::format::json
