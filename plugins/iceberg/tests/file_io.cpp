//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugins/iceberg/detail/file_io.hpp"

#include <tenzir/test/test.hpp>
#include <tenzir/uuid.hpp>

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <fmt/format.h>
#include <iceberg/arrow/arrow_io_internal.h>
#include <iceberg/file_io.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>

namespace tenzir::plugins::iceberg {

namespace {

TEST("S3 warehouses use automatic FileIO selection by default") {
  auto config = CatalogConfig{};
  config.warehouse = "s3://bucket/warehouse";
  CHECK(file_io::select_file_io(config) == file_io::FileIO::automatic);
}

TEST("GCS warehouses without Google authentication use automatic FileIO "
     "selection") {
  auto config = CatalogConfig{};
  config.warehouse = "gs://bucket/warehouse";
  config.properties["token"] = "secret-token";
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

TEST("explicit AWS IAM configuration takes precedence over Google "
     "authentication") {
  auto config = CatalogConfig{};
  config.use_s3_file_io = true;
  config.gcp_auth = true;
  CHECK(file_io::select_file_io(config) == file_io::FileIO::s3);
}

TEST("Arrow FileIO keeps percent escapes literal and reads decoded names of "
     "earlier writers") {
  auto const root
    = std::filesystem::temp_directory_path()
      / fmt::format("tenzir-iceberg-file-io-test-{}", uuid::random());
  std::filesystem::create_directories(root / "k=a%20b");
  std::filesystem::create_directories(root / "k=a b");
  std::filesystem::create_directories(root / "k=c%20d");
  std::filesystem::create_directories(root / "k=e f");
  auto const location = [&](std::string_view name) {
    return fmt::format("file://{}/{}", root.string(), name);
  };
  auto const content = [](std::filesystem::path const& path) {
    auto in = std::ifstream{path};
    return std::string{std::istreambuf_iterator<char>{in},
                       std::istreambuf_iterator<char>{}};
  };
  auto io = ::iceberg::arrow::ArrowFileSystemFileIO::MakeLocalFileIO();
  // A write stores the file under the literal name of its location, as other
  // Iceberg implementations expect.
  REQUIRE(io->WriteFile(location("k=a%20b/new.txt"), "literal").has_value());
  CHECK(std::filesystem::exists(root / "k=a%20b" / "new.txt"));
  CHECK(not std::filesystem::exists(root / "k=a b" / "new.txt"));
  auto read = io->ReadFile(location("k=a%20b/new.txt"), {});
  REQUIRE(read.has_value());
  CHECK_EQUAL(*read, "literal");
  // An earlier version stored the file under the decoded name. Reading its
  // location finds it, with and without a known length.
  {
    auto out = std::ofstream{root / "k=e f" / "old.txt"};
    out << "decoded";
  }
  auto legacy = io->ReadFile(location("k=e%20f/old.txt"), {});
  REQUIRE(legacy.has_value());
  CHECK_EQUAL(*legacy, "decoded");
  auto legacy_sized = io->ReadFile(location("k=e%20f/old.txt"), size_t{7});
  REQUIRE(legacy_sized.has_value());
  CHECK_EQUAL(*legacy_sized, "decoded");
  // If both names exist, the literal one wins.
  {
    auto out = std::ofstream{root / "k=a b" / "both.txt"};
    out << "decoded";
  }
  REQUIRE(io->WriteFile(location("k=a%20b/both.txt"), "literal").has_value());
  auto both = io->ReadFile(location("k=a%20b/both.txt"), {});
  REQUIRE(both.has_value());
  CHECK_EQUAL(*both, "literal");
  CHECK_EQUAL(content(root / "k=a b" / "both.txt"), "decoded");
  // A write never goes to the decoded name, also when only that one exists.
  REQUIRE(io->WriteFile(location("k=c%20d/old.txt"), "literal").has_value());
  CHECK(std::filesystem::exists(root / "k=c%20d" / "old.txt"));
  // A location that exists under neither name fails for the literal name.
  auto missing = io->ReadFile(location("k=a%20b/missing.txt"), {});
  REQUIRE(not missing.has_value());
  CHECK(missing.error().message.find("k=a%20b/missing.txt")
        != std::string::npos);
  // Deleting uses the literal name only.
  CHECK(not io->DeleteFile(location("k=e%20f/old.txt")).has_value());
  CHECK(std::filesystem::exists(root / "k=e f" / "old.txt"));
  auto ec = std::error_code{};
  std::filesystem::remove_all(root, ec);
}

} // namespace

} // namespace tenzir::plugins::iceberg
