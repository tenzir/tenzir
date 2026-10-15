//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/plugins/iceberg/types.hpp"

#include <algorithm>

namespace tenzir::plugins::iceberg::file_io {

enum class FileIO {
  automatic,
  s3,
  gcs,
};

inline auto select_file_io(CatalogConfig const& config) -> FileIO {
  const auto has_s3_properties
    = std::ranges::any_of(config.properties, [](auto const& entry) {
        return entry.first.starts_with("s3.");
      });
  if (config.use_s3_file_io or has_s3_properties
      or config.aws_credentials_provider != nullptr) {
    return FileIO::s3;
  }
  if (config.gcp_auth) {
    return FileIO::gcs;
  }
  return FileIO::automatic;
}

} // namespace tenzir::plugins::iceberg::file_io
