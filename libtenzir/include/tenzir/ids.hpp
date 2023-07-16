//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/aliases.hpp"
#include "tenzir/bitmap.hpp"
#include "tenzir/id_range.hpp"

namespace tenzir {

/// A set of IDs.
using ids = bitmap;

/// Generates an ID set for the given ranges. For example,
/// `make_ids({{10, 12}, {20, 22}})` will return an ID set containing the
/// ranges [10, 12) and [20, 22), i.e., 10, 11, 20, and 21. The bitmap is
/// at least of size `min_size. If the size is less than `min_size`, additional
/// bits of value `default_bit` are appended.
ids make_ids(std::initializer_list<id_range> ranges, size_t min_size = 0,
             bool default_bit = false);

/// Generates an ID set for the given table slice.
ids make_ids(const table_slice& slice);

} // namespace tenzir
