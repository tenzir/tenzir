//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugins/iceberg/detail/file_io.hpp"

#include <tenzir/test/test.hpp>

#include <aws/core/auth/AWSCredentialsProvider.h>

namespace tenzir::plugins::iceberg {

namespace {

TEST("S3 warehouses use automatic FileIO selection by default") {
  auto config = CatalogConfig{};
  config.warehouse = "s3://bucket/warehouse";
  CHECK(file_io::select_file_io(config) == file_io::FileIO::automatic);
}

TEST("AWS authentication selects S3 FileIO without static credentials") {
  auto config = CatalogConfig{};
  config.use_s3_file_io = true;
  CHECK(file_io::select_file_io(config) == file_io::FileIO::s3);
}

TEST("a live AWS credentials provider selects S3 FileIO") {
  auto config = CatalogConfig{};
  config.aws_credentials_provider
    = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>("key",
                                                                "secret");
  CHECK(file_io::select_file_io(config) == file_io::FileIO::s3);
}

TEST("Google authentication selects GCS FileIO") {
  auto config = CatalogConfig{};
  config.gcp_auth = true;
  CHECK(file_io::select_file_io(config) == file_io::FileIO::gcs);
}

TEST("explicit S3 configuration takes precedence over Google authentication") {
  auto config = CatalogConfig{};
  config.properties["s3.endpoint"] = "https://storage.googleapis.com";
  config.gcp_auth = true;
  CHECK(file_io::select_file_io(config) == file_io::FileIO::s3);
}

} // namespace

} // namespace tenzir::plugins::iceberg
