//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/plugins/iceberg/facade.hpp"

#include <algorithm>
#include <string_view>

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

/// Whether opening the catalog should retry with the local FileIO: automatic
/// selection has nothing to detect the data plane from when the merged REST
/// configuration carries neither an `io-impl` nor a `warehouse`. Some catalog
/// servers (e.g. the Iceberg REST test fixture) vend absolute table locations
/// without advertising either property.
inline auto should_fall_back_to_local(FileIO selection,
                                      std::string_view error_message) -> bool {
  return selection == FileIO::automatic
         and error_message.find("is required to create FileIO")
               != std::string_view::npos;
}

} // namespace tenzir::plugins::iceberg::file_io
