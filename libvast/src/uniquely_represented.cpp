//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/hash/uniquely_represented.hpp"

#include "vast/flow.hpp"
#include "vast/ip.hpp"
#include "vast/port.hpp"
#include "vast/uuid.hpp"

// We require that compilers are able to pack certain key types in VAST.
// Otherwise performance may suffer substantially. For our supported list of
// compilers, our CI/CD pipeline ensures that this works as expected.
//
// If you encounter any of these static assertions, please file a bug report at
// https://github.com/tenzir/vast/issues.

using namespace vast;

static_assert(uniquely_represented<ip>);
static_assert(uniquely_represented<flow>);
static_assert(uniquely_represented<uint64_t>);
static_assert(uniquely_represented<int64_t>);
static_assert(uniquely_represented<port>);
static_assert(uniquely_represented<uuid>);
