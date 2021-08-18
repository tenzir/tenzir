//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/legacy_type.hpp"

namespace vast::detail {

/// Applies various heuristics to make Zeek log fields more semantic than using
/// just basic type inference. For example, the `ts` field in a Zeek log almost
/// always refers to the event timestamp, and therefore the field should have
/// the `timestamp` alias instead of just `time`.
/// @param layout The layout to tune.
/// @returns The lifted version of *layout* with stronger types.
legacy_record_type zeekify(legacy_record_type layout);

} // namespace vast::detail
