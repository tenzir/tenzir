//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/data.hpp>

#include <ftxui/dom/elements.hpp>
#include <ftxui/dom/table.hpp>

#include <string>

namespace vast::plugins::tui {

/// Renders the VAST logo.
ftxui::Element Vee();

/// Renders a wide ASCII art of the letters "V A S T".
ftxui::Element VAST();

/// Creates a key-value table from a record. Nested records will be rendered as
/// part of the value.
/// @param key The name of the first column header.
/// @param value The name of the second column header.
/// @returns A FTXUI table.
ftxui::Table make_table(std::string key, std::string value, const record& xs);

/// Creates a table that shows type statistics for all events in a VAST node.
/// @param status An instance of a status record.
/// @returns An event table
/// @relates make_table
ftxui::Table make_schema_table(const data& status);

/// Creates a table that shows the build configuration.
/// @param status An instance of a status record.
/// @returns A table of the build configuration.
/// @relates make_table
ftxui::Table make_build_configuration_table(const data& status);

/// Creates a table that shows the VAST version details.
/// @param status An instance of a status record.
/// @returns A table of the version details fo the various components.
/// @relates make_table
ftxui::Table make_version_table(const data& status);

} // namespace vast::plugins::tui
