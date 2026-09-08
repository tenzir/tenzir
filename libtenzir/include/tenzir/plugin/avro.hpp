//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/arc.hpp>
#include <tenzir/data.hpp>
#include <tenzir/plugin/base.hpp>
#include <tenzir/result.hpp>
#include <tenzir/type.hpp>

#include <span>
#include <string>
#include <utility>
#include <vector>

namespace tenzir {

struct AvroValue {
  data value;
  type schema;
  size_t approx_bytes = 0;
};

/// An immutable writer schema that can decode independent messages concurrently.
class AvroDecoder {
public:
  virtual ~AvroDecoder() = default;

  /// Decode exactly one datum. Confluent omits the length of top-level bytes.
  virtual auto
  decode(std::span<std::byte const> bytes, bool confluent_bytes = false) const
    -> Result<AvroValue, std::string>
    = 0;
};

/// Keeps the Avro dependency in its plugin while exposing message decoding.
class AvroDecoderPlugin : public virtual plugin {
public:
  /// References are named schema definitions in dependency order.
  virtual auto compile(
    std::string const& schema,
    std::vector<std::pair<std::string, std::string>> const& references) const
    -> Result<Arc<AvroDecoder>, std::string>
    = 0;
};

} // namespace tenzir
