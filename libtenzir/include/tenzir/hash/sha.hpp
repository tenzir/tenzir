//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |\\ \\  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/hash/openssl.hpp"

namespace tenzir {

/// OpenSSL-backed SHA-family hash algorithms.
using sha1 = openssl::hash<openssl::algorithm::sha1>;
using sha224 = openssl::hash<openssl::algorithm::sha224>;
using sha256 = openssl::hash<openssl::algorithm::sha256>;
using sha384 = openssl::hash<openssl::algorithm::sha384>;
using sha512 = openssl::hash<openssl::algorithm::sha512>;
using sha3_224 = openssl::hash<openssl::algorithm::sha3_224>;
using sha3_256 = openssl::hash<openssl::algorithm::sha3_256>;
using sha3_384 = openssl::hash<openssl::algorithm::sha3_384>;
using sha3_512 = openssl::hash<openssl::algorithm::sha3_512>;

/// OpenSSL-backed HMAC variants for the SHA family.
using hmac_sha1 = openssl::hmac<openssl::algorithm::sha1>;
using hmac_sha224 = openssl::hmac<openssl::algorithm::sha224>;
using hmac_sha256 = openssl::hmac<openssl::algorithm::sha256>;
using hmac_sha384 = openssl::hmac<openssl::algorithm::sha384>;
using hmac_sha512 = openssl::hmac<openssl::algorithm::sha512>;
using hmac_sha3_224 = openssl::hmac<openssl::algorithm::sha3_224>;
using hmac_sha3_256 = openssl::hmac<openssl::algorithm::sha3_256>;
using hmac_sha3_384 = openssl::hmac<openssl::algorithm::sha3_384>;
using hmac_sha3_512 = openssl::hmac<openssl::algorithm::sha3_512>;

} // namespace tenzir
