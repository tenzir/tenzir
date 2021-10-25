//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/error.hpp"
#include "vast/system/actors.hpp"

#include <caf/typed_event_based_actor.hpp>

// An in-memory implementation of the filesystem actor, to rule out
// test flakiness due to a slow disk and to be able to write to any
// path without permission issues.
inline vast::system::filesystem_actor::behavior_type memory_filesystem() {
  auto chunks
    = std::make_shared<std::map<std::filesystem::path, vast::chunk_ptr>>();
  return {
    [chunks](vast::atom::write, const std::filesystem::path& path,
             vast::chunk_ptr& chunk) {
      (*chunks)[path] = std::move(chunk);
      return vast::atom::ok_v;
    },
    [chunks](vast::atom::read, const std::filesystem::path& path)
      -> caf::result<vast::chunk_ptr> {
      auto chunk = chunks->find(path);
      if (chunk == chunks->end())
        return caf::make_error(vast::ec::filesystem_error, "unknown file");
      return chunk->second;
    },
    [chunks](vast::atom::mmap, const std::filesystem::path& path)
      -> caf::result<vast::chunk_ptr> {
      auto chunk = chunks->find(path);
      if (chunk == chunks->end())
        return caf::make_error(vast::ec::filesystem_error, "unknown file");
      return chunk->second;
    },
    [chunks](vast::atom::erase, std::filesystem::path& path) {
      chunks->erase(path);
      return vast::atom::done_v;
    },
    [](vast::atom::status, vast::system::status_verbosity) -> vast::record {
      return {};
    },
  };
}
