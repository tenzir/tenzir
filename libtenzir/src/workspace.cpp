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
#include "tenzir/tql2/registry.hpp"
#include "tenzir/tql2/resolve.hpp"

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

/// Passes diagnostics on, and remembers whether one of them is an error.
class ErrorTracking final : public diagnostic_handler {
public:
  explicit ErrorTracking(diagnostic_handler& inner) : inner_{inner} {
  }

  auto emit(diagnostic d) -> void override {
    erred_ = erred_ or d.severity == severity::error;
    inner_.emit(std::move(d));
  }

  auto erred() const -> bool {
    return erred_;
  }

private:
  diagnostic_handler& inner_;
  bool erred_ = false;
};

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

/// `path` as YAML, or why it cannot be read.
auto read_yaml(std::filesystem::path const& path, diagnostic_handler& dh)
  -> failure_or<data> {
  auto yaml = load_yaml(path);
  if (not yaml) {
    diagnostic::error("could not read {}", path)
      .note("{}", yaml.error())
      .emit(dh);
    return failure::promise();
  }
  return std::move(*yaml);
}

/// A locked package: what the engine needs of a `package.lock` entry.
struct LockedPackage {
  std::string id;
  Option<std::string> version;
  std::string hash;
};

/// The packages `package.lock` pins.
auto locked_packages(std::filesystem::path const& root, diagnostic_handler& dh)
  -> failure_or<std::vector<LockedPackage>> {
  auto path = root / lockfile_name;
  TRY(auto yaml, read_yaml(path, dh));
  auto refuse
    = [&](std::string_view why) -> failure_or<std::vector<LockedPackage>> {
    diagnostic::error("{} is not a lock this version of Tenzir reads", path)
      .note("{}", why)
      .hint("run `tenzir-platform package lock` to write it again")
      .emit(dh);
    return failure::promise();
  };
  auto const* lock = try_as<record>(&yaml);
  if (not lock) {
    return refuse("it is not a record");
  }
  // YAML reads a plain `1` as unsigned.
  auto format = try_get<uint64_t>(*lock, "format");
  if (not format or not *format or **format != 1) {
    return refuse("it says no `format: 1`");
  }
  auto packages = lock->find("packages");
  if (packages == lock->end() or is<caf::none_t>(packages->second)) {
    return std::vector<LockedPackage>{};
  }
  auto const* entries = try_as<list>(&packages->second);
  if (not entries) {
    return refuse("its `packages` are not a list");
  }
  auto result = std::vector<LockedPackage>{};
  auto ids = std::unordered_set<std::string>{};
  for (auto const& entry : *entries) {
    auto const* locked = try_as<record>(&entry);
    if (not locked) {
      return refuse("a package in it is not a record");
    }
    auto const* id = get_if<std::string>(locked, "id");
    auto const* hash = get_if<std::string>(locked, "hash");
    if (not id or not hash) {
      return refuse("a package in it has no `id` or `hash` as text");
    }
    if (not is_content_hash(*hash)) {
      return refuse(
        fmt::format("the `hash` of `{}` is not a SHA-256 digest", *id));
    }
    if (not ids.insert(*id).second) {
      return refuse(fmt::format("it locks `{}` more than once", *id));
    }
    auto package = LockedPackage{.id = *id, .version = None{}, .hash = *hash};
    if (auto const* version = get_if<std::string>(locked, "version")) {
      package.version = *version;
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

/// The directories under `packages/` of the workspace at `root` that hold a
/// package, in order of their names, so that what loads and what fails does
/// not depend on the order the file system lists them in.
auto local_package_directories(std::filesystem::path const& root,
                               diagnostic_handler& dh)
  -> failure_or<std::vector<std::filesystem::path>> {
  auto result = std::vector<std::filesystem::path>{};
  auto locals = root / local_directory;
  auto ec = std::error_code{};
  auto failed = [&](std::string_view doing, std::filesystem::path const& path) {
    diagnostic::error("{}", ec).note("while {} {}", doing, path).emit(dh);
    return failure::promise();
  };
  // `exists` takes a missing directory for none, which `is_directory` says
  // with an error.
  if (not std::filesystem::exists(locals, ec)
      or not std::filesystem::is_directory(locals, ec)) {
    if (ec) {
      return failed("checking", locals);
    }
    return result;
  }
  for (auto it = std::filesystem::directory_iterator{locals, ec};
       it != std::filesystem::directory_iterator{}; it.increment(ec)) {
    if (ec) {
      break;
    }
    auto has_manifest = std::filesystem::exists(it->path() / manifest_name, ec);
    if (ec) {
      return failed("checking", it->path() / manifest_name);
    }
    if (has_manifest) {
      result.push_back(it->path());
    }
  }
  if (ec) {
    return failed("listing", locals);
  }
  std::ranges::sort(result);
  return result;
}

} // namespace

auto workspace_dependencies(data const& manifest, std::string_view origin,
                            diagnostic_handler& dh)
  -> failure_or<PackageDependencies> {
  auto result = PackageDependencies{};
  if (is<caf::none_t>(manifest)) {
    return result;
  }
  auto const* fields = try_as<record>(&manifest);
  if (not fields) {
    diagnostic::error("a workspace's manifest must be a record")
      .note("in {}", origin)
      .emit(dh);
    return failure::promise();
  }
  for (auto const& [key, value] : *fields) {
    if (key != "dependencies") {
      diagnostic::error("a workspace lists only its `dependencies`, not `{}`",
                        key)
        .note("in {}", origin)
        .hint("put the workspace's own operators in a package under `{}/`",
              local_directory)
        .emit(dh);
      return failure::promise();
    }
    auto parsed = parse_package_dependencies(make_view(value));
    if (not parsed) {
      diagnostic::error("could not read the workspace's dependencies")
        .note("in {}", origin)
        .note("{}", parsed.error())
        .emit(dh);
      return failure::promise();
    }
    result = std::move(*parsed);
  }
  return result;
}

auto load_workspace_package(package_files const& files, std::string_view origin,
                            std::string_view id, diagnostic_handler& dh,
                            bool only_entities) -> failure_or<package> {
  auto tracking = ErrorTracking{dh};
  auto loaded = package::load(files, origin, id, tracking, only_entities);
  if (not loaded or tracking.erred()) {
    return failure::promise();
  }
  if (loaded->id != id) {
    diagnostic::error("the package says its id is `{}`, but it is `{}` here",
                      loaded->id, id)
      .note("in {}", origin)
      .emit(dh);
    return failure::promise();
  }
  return loaded;
}

auto load_workspace_package(std::filesystem::path const& dir,
                            std::string_view id, diagnostic_handler& dh,
                            bool only_entities) -> failure_or<package> {
  TRY(auto files, read_package_files(dir, dh, only_entities));
  return load_workspace_package(files, dir.string(), id, dh, only_entities);
}

auto add_workspace_packages(registry& reg,
                            PackageDependencies const& dependencies,
                            std::span<package const> packages,
                            diagnostic_handler& dh, SourceMap* source_map)
  -> failure_or<ExternPrelude> {
  TRY(require_dependencies("the workspace", dependencies, packages, dh));
  TRY(add_packages(reg, packages, dh, source_map, PackageVisibility::declared));
  return extern_prelude(dependencies);
}

auto find_workspace(std::filesystem::path const& from)
  -> Option<std::filesystem::path> {
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
  auto result = Workspace{};
  // Without a manifest, the workspace lists nothing.
  auto manifest = root / manifest_name;
  auto ec = std::error_code{};
  if (std::filesystem::exists(manifest, ec)) {
    TRY(auto yaml, read_yaml(manifest, dh));
    TRY(result.dependencies,
        workspace_dependencies(yaml, manifest.string(), dh));
  } else if (ec) {
    diagnostic::error("{}", ec).note("while checking {}", manifest).emit(dh);
    return failure::promise();
  }
  // The packages written in the workspace, each named by its directory.
  TRY(auto locals, local_package_directories(root, dh));
  for (auto const& dir : locals) {
    TRY(auto local, load_workspace_package(dir, dir.filename().string(), dh,
                                           /*only_entities=*/true));
    result.packages.push_back(std::move(local));
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
    if (std::ranges::any_of(result.packages, [&](package const& local) {
          return local.id == entry.id;
        })) {
      diagnostic::error("`{}` is both locked and a local package of the "
                        "workspace",
                        entry.id)
        .emit(dh);
      return failure::promise();
    }
    auto dir = *cache / entry.hash.substr(hash_prefix.size());
    if (not std::filesystem::is_directory(dir, ec)) {
      diagnostic::error("package `{}`{} is not in the package cache", entry.id,
                        entry.version ? fmt::format(" {}", *entry.version)
                                      : std::string{})
        .note("looked in {}", dir)
        .hint("run `tenzir-platform package fetch` in {}", root)
        .emit(dh);
      return failure::promise();
    }
    TRY(auto fetched, load_workspace_package(dir, entry.id, dh,
                                             /*only_entities=*/true));
    result.packages.push_back(std::move(fetched));
  }
  return result;
}

} // namespace tenzir
