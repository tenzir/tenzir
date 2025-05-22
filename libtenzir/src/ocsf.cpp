//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/ocsf.hpp>

#include <optional>
#include <span>

namespace tenzir {

namespace {

struct ocsf_pair {
  ocsf_pair(std::string_view name, int64_t id) : name{name}, id{id} {
  }

  std::string_view name;
  int64_t id;
};

ocsf_pair category_map[] = {
  {"System Activity", 1},
  {"Findings", 2},
  {"Identity & Access Management", 3},
  {"Network Activity", 4},
  {"Discovery", 5},
  {"Application Activity", 6},
  {"Remediation", 7},
};

ocsf_pair class_map[] = {
  {"Base Event", 0},

  {"File System Activity", 1001},
  {"Kernel Extension Activity", 1002},
  {"Kernel Activity", 1003},
  {"Memory Activity", 1004},
  {"Module Activity", 1005},
  {"Scheduled Job Activity", 1006},
  {"Process Activity", 1007},
  {"Event Log Activity", 1008},

  {"Security Finding", 2001},
  {"Vulnerability Finding", 2002},
  {"Compliance Finding", 2003},
  {"Detection Finding", 2004},
  {"Incident Finding", 2005},
  {"Data Security Finding", 2006},

  {"Account Change", 3001},
  {"Authentication", 3002},
  {"Authorize Session", 3003},
  {"Entity Management", 3004},
  {"User Access Management", 3005},
  {"Group Management", 3006},

  {"Network Activity", 4001},
  {"HTTP Activity", 4002},
  {"DNS Activity", 4003},
  {"DHCP Activity", 4004},
  {"RDP Activity", 4005},
  {"SMB Activity", 4006},
  {"SSH Activity", 4007},
  {"FTP Activity", 4008},
  {"Email Activity", 4009},
  {"Network File Activity", 4010},
  {"Email File Activity", 4011},
  {"Email URL Activity", 4012},
  {"NTP Activity", 4013},
  {"Tunnel Activity", 4014},

  {"Device Inventory Info", 5001},
  {"Device Config State", 5002},
  {"User Inventory Info", 5003},
  {"Operating System Patch State", 5004},
  {"Kernel Object Query", 5006},
  {"File Query", 5007},
  {"Folder Query", 5008},
  {"Admin Group Query", 5009},
  {"Job Query", 5010},
  {"Module Query", 5011},
  {"Network Connection Query", 5012},
  {"Networks Query", 5013},
  {"Peripheral Device Query", 5014},
  {"Process Query", 5015},
  {"Service Query", 5016},
  {"User Session Query", 5017},
  {"User Query", 5018},
  {"Device Config State Change", 5019},
  {"Software Inventory Info", 5020},

  {"Web Resources Activity", 6001},
  {"Application Lifecycle", 6002},
  {"API Activity", 6003},
  {"Web Resource Access Activity", 6004},
  {"Datastore Activity", 6005},
  {"File Hosting Activity", 6006},
  {"Scan Activity", 6007},

  {"Remediation Activity", 7001},
  {"File Remediation Activity", 7002},
  {"Process Remediation Activity", 7003},
  {"Network Remediation Activity", 7004},
};

auto name_to_id(std::span<const ocsf_pair> lookup, std::string_view key)
  -> std::optional<int64_t> {
  for (const auto& [category, id] : lookup) {
    if (key == category) {
      return id;
    }
  }
  return std::nullopt;
}

auto id_to_name(std::span<const ocsf_pair> lookup, int64_t key)
  -> std::optional<std::string_view> {
  for (const auto& [category, id] : lookup) {
    if (key == id) {
      return category;
    }
  }
  return std::nullopt;
}

} // namespace

auto ocsf_class_name(int64_t id) -> std::optional<std::string_view> {
  return id_to_name(class_map, id);
}

auto ocsf_class_uid(std::string_view name) -> std::optional<int64_t> {
  return name_to_id(class_map, name);
}

auto ocsf_category_name(int64_t id) -> std::optional<std::string_view> {
  return id_to_name(category_map, id);
}

auto ocsf_category_uid(std::string_view name) -> std::optional<int64_t> {
  return name_to_id(category_map, name);
}

} // namespace tenzir
