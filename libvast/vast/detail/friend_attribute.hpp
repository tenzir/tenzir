//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

// An friend declaration like this
//
//     struct A {
//       [[nodiscard]]
//       friend int f();
//     }
//
// will compile fine on g++ but result in an error on clang++.
//
// We do want to use only friend declarations in some instances
// to ensure some functions can only be found via ADL and not
// regular lookup.
//
// So we only declare the attributes in this way when using g++,
// which is enough to catch errors in CI.
//
// Note that only attributes that are harmless to omit should be
// used in this way, so we can't safely make a `FRIEND_ATTRIBUTE(x)`
// function.

#ifdef __gcc__

#  define FRIEND_ATTRIBUTE_NODISCARD [[nodiscard]]

#else

#  define FRIEND_ATTRIBUTE_NODISCARD

#endif
