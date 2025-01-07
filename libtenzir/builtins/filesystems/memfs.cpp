//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/atoms.hpp"

#include <tenzir/blob_storage.hpp>

#include <caf/actor_from_state.hpp>
#include <caf/scheduled_actor/flow.hpp>

namespace tenzir::plugins::memfs {

namespace {

struct memfs {
  explicit memfs(blob_storage_actor::pointer self) : self{self} {
  }

  auto make_behavior() -> blob_storage_actor::behavior_type {
    return {
      [this](atom::write, std::string path,
             caf::typed_stream<chunk_ptr> chunks) -> caf::result<void> {
        auto rp = self->make_response_promise<void>();
        self->observe(std::move(chunks), 30, 10)
          .do_on_error([rp](caf::error err) mutable {
            rp.deliver(std::move(err));
          })
          .do_on_complete([rp]() mutable {
            rp.deliver();
          })
          .for_each([this, path = std::move(path)](chunk_ptr chunk) {
            files[path].push_back(std::move(chunk));
          });
        return rp;
      },
      [this](atom::read, const std::string& path)
        -> caf::result<caf::typed_stream<chunk_ptr>> {
        const auto file = files.find(path);
        if (file == files.end()) {
          return diagnostic::error("no such file").
        }
      },
      [this](atom::move, std::string old_path,
             std::string new_path) -> caf::result<void> {
        return ec::unimplemented;
      },
      [this](atom::erase, std::string path) -> caf::result<void> {
        return ec::unimplemented;
      },
    };
  }

  blob_storage_actor::pointer self = {};
  std::unordered_map<std::string, std::vector<chunk_ptr>> files = {};
};

class plugin final : public virtual blob_storage_plugin {
public:
  auto name() const -> std::string override {
    return "memfs";
  }

  virtual auto spawn_blob_storage(caf::actor_system& sys)
    -> blob_storage_actor {
    return sys.spawn(caf::actor_from_state<memfs>)
  }
};

} // namespace

} // namespace tenzir::plugins::memfs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::memfs::plugin)
