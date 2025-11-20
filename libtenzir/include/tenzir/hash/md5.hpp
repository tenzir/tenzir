//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/hash/openssl.hpp"

namespace tenzir {

/// The [MD5](https://en.wikipedia.org/wiki/MD5) hash algorithm powered by
/// OpenSSL's EVP hashing facilities.
using md5 = openssl::hash<openssl::algorithm::md5>;
using hmac_md5 = openssl::hmac<openssl::algorithm::md5>;

} // namespace tenzir
