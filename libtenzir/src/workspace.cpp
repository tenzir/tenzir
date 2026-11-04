//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/workspace.hpp"

#include "tenzir/data.hpp"
#include "tenzir/detail/env.hpp"
#include "tenzir/file.hpp"

#include <fmt/format.h>

#include <algorithm>
#include <pwd.h>
#include <string>
#include <string_view>
#include <unistd.h>
#include <unordered_set>
#include <vector>

namespace tenzir {

namespace {

constexpr auto manifest_name = std::string_view{"package.yaml"};
constexpr auto lockfile_name = std::string_view{"package.lock"};
constexpr auto local_directory = std::string_view{"packages"};
constexpr auto hash_prefix = std::string_view{"sha256:"};

/// Whether `hash` names a package's files as a lock does: `sha256:` and 64
/// lowercase hexadecimal digits. It names a directory in the package cache,
/// so nothing else may pass for one.
auto is_content_hash(std::string_view hash) -> bool {
  if (not hash.starts_with(hash_prefix)) {
    return false;
  }
  auto digest = hash.substr(hash_prefix.size());
  return digest.size() == 64 and std::ranges::all_of(digest, [](char c) {
           return (c >= '0' and c <= '9') or (c >= 'a' and c <= 'f');
         });
}

/// The contents of `path`, or why they cannot be read.
auto read_text(std::filesystem::path const& path, diagnostic_handler& dh)
  -> failure_or<std::string> {
  auto bytes = read_file(path);
  if (not bytes) {
    diagnostic::error("could not read {}", path)
      .note("{}", bytes.unwrap_err())
      .emit(dh);
    return failure::promise();
  }
  auto const& content = bytes.unwrap();
  return std::string{
    content.empty() ? "" : reinterpret_cast<char const*>(content.data()),
    content.size()};
}

/// `text` as a YAML record, where an empty document is an empty record.
auto yaml_record(std::string const& text, std::filesystem::path const& path,
                 diagnostic_handler& dh) -> failure_or<record> {
  auto value = from_yaml(text);
  if (not value) {
    diagnostic::error("could not read {}", path)
      .note("{}", value.error())
      .emit(dh);
    return failure::promise();
  }
  if (is<caf::none_t>(*value)) {
    return record{};
  }
  auto* fields = try_as<record>(&*value);
  if (not fields) {
    diagnostic::error("{} is not a record", path).emit(dh);
    return failure::promise();
  }
  return std::move(*fields);
}

/// What a workspace's `package.yaml` lists: its `dependencies` and nothing
/// else, since a workspace is no package.
auto workspace_dependencies(std::filesystem::path const& root,
                            diagnostic_handler& dh)
  -> failure_or<PackageDependencies> {
  auto path = root / manifest_name;
  auto ec = std::error_code{};
  if (not std::filesystem::exists(path, ec)) {
    return PackageDependencies{};
  }
  TRY(auto text, read_text(path, dh));
  TRY(auto fields, yaml_record(text, path, dh));
  auto result = PackageDependencies{};
  for (auto const& [key, value] : fields) {
    if (key != "dependencies") {
      diagnostic::error("{} lists only a workspace's `dependencies`, not `{}`",
                        path, key)
        .hint("put the workspace's own operators in a package under `{}/`",
              local_directory)
        .emit(dh);
      return failure::promise();
    }
    auto parsed = parse_package_dependencies(make_view(value));
    if (not parsed) {
      diagnostic::error("could not read the dependencies in {}", path)
        .note("{}", parsed.error())
        .emit(dh);
      return failure::promise();
    }
    result = std::move(*parsed);
  }
  return result;
}

/// A locked package: what the engine needs of a `package.lock` entry.
struct LockedPackage {
  std::string id;
  std::string version;
  std::string hash;
};

/// The packages `package.lock` pins.
auto locked_packages(std::filesystem::path const& root, diagnostic_handler& dh)
  -> failure_or<std::vector<LockedPackage>> {
  auto path = root / lockfile_name;
  TRY(auto text, read_text(path, dh));
  TRY(auto fields, yaml_record(text, path, dh));
  auto refuse
    = [&](std::string_view why) -> failure_or<std::vector<LockedPackage>> {
    diagnostic::error("{} is not a lock this version of Tenzir reads", path)
      .note("{}", why)
      .hint("run `tenzir-platform package lock` to write it again")
      .emit(dh);
    return failure::promise();
  };
  // YAML reads a plain `1` as unsigned.
  auto format = fields.find("format");
  auto is_one = format != fields.end()
                and (try_as<uint64_t>(&format->second)
                       ? *try_as<uint64_t>(&format->second) == 1
                       : try_as<int64_t>(&format->second)
                           and *try_as<int64_t>(&format->second) == 1);
  if (not is_one) {
    return refuse("it says no `format: 1`");
  }
  auto packages = fields.find("packages");
  if (packages == fields.end() or is<caf::none_t>(packages->second)) {
    return std::vector<LockedPackage>{};
  }
  auto const* entries = try_as<list>(&packages->second);
  if (not entries) {
    return refuse("its `packages` are not a list");
  }
  auto result = std::vector<LockedPackage>{};
  for (auto const& entry : *entries) {
    auto const* locked = try_as<record>(&entry);
    if (not locked) {
      return refuse("a package in it is not a record");
    }
    auto text_of = [&](std::string_view key) -> std::string {
      auto it = locked->find(key);
      if (it == locked->end()) {
        return {};
      }
      auto const* text = try_as<std::string>(&it->second);
      return text ? *text : std::string{};
    };
    auto package
      = LockedPackage{text_of("id"), text_of("version"), text_of("hash")};
    if (package.id.empty() or package.hash.empty()) {
      return refuse("a package in it has no `id` or `hash`");
    }
    if (not is_content_hash(package.hash)) {
      return refuse(
        fmt::format("the `hash` of `{}` is not a SHA-256 digest", package.id));
    }
    result.push_back(std::move(package));
  }
  return result;
}

/// The user's home directory: `HOME`, or else the user's entry in the user
/// database, which is where the package manager looks too.
auto home_directory() -> Option<std::filesystem::path> {
  if (auto home = detail::getenv("HOME"); home and not home->empty()) {
    return std::filesystem::path{*home};
  }
  auto size = ::sysconf(_SC_GETPW_R_SIZE_MAX);
  auto buffer = std::vector<char>(size > 0 ? static_cast<size_t>(size) : 16384);
  auto entry = passwd{};
  auto* found = static_cast<passwd*>(nullptr);
  if (::getpwuid_r(::getuid(), &entry, buffer.data(), buffer.size(), &found)
        != 0
      or not found or not found->pw_dir or *found->pw_dir == '\0') {
    return None{};
  }
  return std::filesystem::path{found->pw_dir};
}

} // namespace

auto find_workspace(std::filesystem::path const& from)
  -> Option<std::filesystem::path> {
  if (auto chosen = detail::getenv("TENZIR_WORKSPACE")) {
    if (*chosen == "none" or chosen->empty()) {
      return None{};
    }
    return std::filesystem::path{*chosen};
  }
  auto ec = std::error_code{};
  for (auto dir = from; not dir.empty(); dir = dir.parent_path()) {
    if (std::filesystem::exists(dir / lockfile_name, ec)) {
      return dir;
    }
    if (dir == dir.parent_path()) {
      break;
    }
  }
  return None{};
}

auto package_cache_directory() -> Option<std::filesystem::path> {
  // The XDG spec declares an empty or relative base directory invalid, as the
  // package manager does.
  if (auto xdg = detail::getenv("XDG_CACHE_HOME");
      xdg and std::filesystem::path{*xdg}.is_absolute()) {
    return std::filesystem::path{*xdg} / "tenzir" / "packages";
  }
  if (auto home = home_directory()) {
    return *home / ".cache" / "tenzir" / "packages";
  }
  return None{};
}

auto load_workspace(std::filesystem::path const& root, diagnostic_handler& dh)
  -> failure_or<Workspace> {
  auto result = Workspace{.root = root, .dependencies = {}, .packages = {}};
  TRY(result.dependencies, workspace_dependencies(root, dh));
  auto ids = std::unordered_set<std::string>{};
  auto ec = std::error_code{};
  // The packages written in the workspace, each named by its directory.
  auto locals = root / local_directory;
  if (std::filesystem::is_directory(locals, ec)) {
    for (auto const& entry : std::filesystem::directory_iterator{locals, ec}) {
      if (not std::filesystem::exists(entry.path() / manifest_name, ec)) {
        continue;
      }
      TRY(auto local, package::load(entry.path(), dh, true));
      if (local.id != entry.path().filename().string()) {
        diagnostic::error("{} says it is `{}`, but its directory names it",
                          entry.path() / manifest_name, local.id)
          .emit(dh);
        return failure::promise();
      }
      ids.insert(local.id);
      result.packages.push_back(std::move(local));
    }
  }
  // The packages the lock pins, which the package manager unpacked into the
  // cache under their hashes.
  TRY(auto locked, locked_packages(root, dh));
  if (locked.empty()) {
    return result;
  }
  auto cache = package_cache_directory();
  if (not cache) {
    diagnostic::error("cannot find the package cache")
      .note("neither `XDG_CACHE_HOME` nor `HOME` is set, and the user has "
            "no home directory")
      .emit(dh);
    return failure::promise();
  }
  for (auto const& entry : locked) {
    auto dir = *cache / entry.hash.substr(hash_prefix.size());
    if (not std::filesystem::is_directory(dir, ec)) {
      diagnostic::error("package `{}`{} is not in the package cache", entry.id,
                        entry.version.empty()
                          ? std::string{}
                          : fmt::format(" {}", entry.version))
        .note("looked in {}", dir)
        .hint("run `tenzir-platform package fetch` in {}", root)
        .emit(dh);
      return failure::promise();
    }
    TRY(auto fetched, package::load(dir, entry.id, dh, true));
    if (fetched.id != entry.id) {
      diagnostic::error("the cached package at {} says it is `{}`, but {} "
                        "locks it as `{}`",
                        dir, fetched.id, lockfile_name, entry.id)
        .emit(dh);
      return failure::promise();
    }
    if (not ids.insert(fetched.id).second) {
      diagnostic::error("`{}` is both locked and a local package of the "
                        "workspace",
                        fetched.id)
        .emit(dh);
      return failure::promise();
    }
    result.packages.push_back(std::move(fetched));
  }
  return result;
}

} // namespace tenzir
