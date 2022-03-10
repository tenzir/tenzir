//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/fwd.hpp>

#include <filesystem>
#include <string>

namespace vast {

/// This version number defines compatibility of persistent state with with
/// prior directory layouts and contents. Breaking changes shall bump the
/// version number. A breaking change includes the structure, sequence and
/// organization of the database directory itself, as well as incompatible
/// changes in binary content.
enum class db_version : uint8_t {
  invalid,
  v0,
  v1,
  latest = v1, // Alias for the latest version
  count,       // Number of enum values
};

/// @relates db_version
std::ostream& operator<<(std::ostream& str, const db_version& version);

/// Reads the DB version from a database directory.
/// @relates db_version
db_version read_db_version(const std::filesystem::path& db_dir);

/// Writes the current DB version if `db_dir/VERSION` does not
/// exist yet.
/// @relates db_version
caf::error initialize_db_version(const std::filesystem::path& db_dir);

/// Returns a human-readable decription of all breaking changes that have been
/// introduced to VAST since the passed version.
/// @relates db_version
std::string describe_breaking_changes_since(db_version);

} // namespace vast
