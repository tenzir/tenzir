//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/synopsis_factory.hpp"

#include "tenzir/bool_synopsis.hpp"
#include "tenzir/double_synopsis.hpp"
#include "tenzir/duration_synopsis.hpp"
#include "tenzir/hash/hash_append.hpp"
#include "tenzir/hash/legacy_hash.hpp"
#include "tenzir/int64_synopsis.hpp"
#include "tenzir/ip_synopsis.hpp"
#include "tenzir/string_synopsis.hpp"
#include "tenzir/time_synopsis.hpp"
#include "tenzir/uint64_synopsis.hpp"

namespace tenzir {

void factory_traits<synopsis>::initialize() {
  factory<synopsis>::add(type{ip_type{}}, make_ip_synopsis<legacy_hash>);
  factory<synopsis>::add<bool_type, bool_synopsis>();
  factory<synopsis>::add(type{string_type{}},
                         make_string_synopsis<legacy_hash>);
  factory<synopsis>::add<time_type, time_synopsis>();
  factory<synopsis>::add<duration_type, duration_synopsis>();
  factory<synopsis>::add<int64_type, int64_synopsis>();
  factory<synopsis>::add<uint64_type, uint64_synopsis>();
  factory<synopsis>::add<double_type, double_synopsis>();
}

} // namespace tenzir
