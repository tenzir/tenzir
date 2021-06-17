//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

namespace vast::policy {

/// Indicates whether to merge or overwrite lists when merging.
enum class merge_lists {
  no,  ///< Overwrite lists when merging.
  yes, ///< Merge nested lists  when merging.
};

} // namespace vast::policy
