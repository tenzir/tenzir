//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/actors.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/error.hpp"

#include <caf/typed_event_based_actor.hpp>
#include <fmt/format.h>

// An in-memory implementation of the filesystem actor, to rule out
// test flakiness due to a slow disk and to be able to write to any
// path without permission issues.
inline auto memory_filesystem() -> tenzir::filesystem_actor::behavior_type {
  auto chunks
    = std::make_shared<std::map<std::filesystem::path, tenzir::chunk_ptr>>();
  return {
    [chunks](tenzir::atom::write, const std::filesystem::path& path,
             tenzir::chunk_ptr& chunk) {
      TENZIR_ASSERT(chunk, "attempted to write a null chunk");
      (*chunks)[path] = std::move(chunk);
      return tenzir::atom::ok_v;
    },
    [chunks](tenzir::atom::read, const std::filesystem::path& path)
      -> caf::result<tenzir::chunk_ptr> {
      auto chunk = chunks->find(path);
      if (chunk == chunks->end())
        return caf::make_error(tenzir::ec::no_such_file,
                               fmt::format("unknown file {}", path));
      return chunk->second;
    },
    [chunks](tenzir::atom::read, tenzir::atom::recursive,
             const std::vector<std::filesystem::path>&)
      -> caf::result<std::vector<tenzir::record>> {
      return caf::make_error(tenzir::ec::unimplemented,
                             "currently not implemented");
    },
    [chunks](
      tenzir::atom::move, const std::filesystem::path& from,
      const std::filesystem::path& to) -> caf::result<tenzir::atom::done> {
      auto chunk = chunks->find(from);
      if (chunk == chunks->end())
        return caf::make_error(tenzir::ec::no_such_file,
                               fmt::format("unknown file {}", from));
      auto x = chunks->extract(from);
      x.key() = to;
      chunks->insert(std::move(x));
      return tenzir::atom::done_v;
    },
    [chunks](
      tenzir::atom::move,
      const std::vector<std::pair<std::filesystem::path, std::filesystem::path>>&
        files) -> caf::result<tenzir::atom::done> {
      for (auto const& [from, to] : files) {
        auto chunk = chunks->find(from);
        if (chunk == chunks->end())
          return caf::make_error(tenzir::ec::no_such_file,
                                 fmt::format("unknown file {}", from));
        auto x = chunks->extract(from);
        x.key() = to;
        chunks->insert(std::move(x));
      }
      return tenzir::atom::done_v;
    },
    [chunks](tenzir::atom::mmap, const std::filesystem::path& path)
      -> caf::result<tenzir::chunk_ptr> {
      auto chunk = chunks->find(path);
      if (chunk == chunks->end())
        return caf::make_error(tenzir::ec::no_such_file,
                               fmt::format("unknown file {}", path));
      return chunk->second;
    },
    [chunks](tenzir::atom::erase, std::filesystem::path& path) {
      chunks->erase(path);
      return tenzir::atom::done_v;
    },
    [](tenzir::atom::status, tenzir::status_verbosity,
       tenzir::duration) -> tenzir::record {
      return {};
    },
  };
}
