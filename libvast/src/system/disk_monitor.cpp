#include "vast/system/disk_monitor.hpp"

#include "vast/concept/parseable/from_string.hpp"
#include "vast/concept/parseable/vast/uuid.hpp"
#include "vast/directory.hpp"
#include "vast/error.hpp"
#include "vast/fwd.hpp"
#include "vast/logger.hpp"
#include "vast/path.hpp"
#include "vast/system/archive.hpp"
#include "vast/uuid.hpp"

#include <caf/detail/scope_guard.hpp>
#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <sys/stat.h>

namespace vast::system {

namespace {

struct partition_diskstate {
  uuid id;
  off_t filesize;
  time_t mtime;
};

template <typename Fun>
std::shared_ptr<caf::detail::scope_guard<Fun>> make_shared_guard(Fun f) {
  return std::make_shared<caf::detail::scope_guard<Fun>>(std::forward<Fun>(f));
}

} // namespace

disk_monitor_actor::behavior_type
disk_monitor(disk_monitor_actor::stateful_pointer<disk_monitor_state> self,
             size_t hiwater, size_t lowater,
             std::chrono::seconds disk_scan_interval, const path& dbdir,
             archive_actor archive, index_actor index) {
  VAST_TRACE(VAST_ARG(hiwater), VAST_ARG(lowater), VAST_ARG(dbdir));
  using namespace std::string_literals;
  self->state.high_water_mark = hiwater;
  self->state.low_water_mark = lowater;
  self->state.archive = archive;
  self->state.index = index;
  self->state.dbdir = dbdir;
  self->send(self, atom::ping_v);
  return {
    [=](atom::ping) {
      self->delayed_send(self, disk_scan_interval, atom::ping_v);
      if (self->state.purging) {
        VAST_LOG_SPD_DEBUG("{} ignores ping because a deletion is still in "
                           "progress",
                           detail::id_or_name(self));
        return;
      }
      // TODO: This is going to do one syscall per file in the database
      // directory. This feels a bit wasteful, but in practice we didn't
      // see noticeable overhead even on large-ish databases.
      // Nonetheless, if this becomes relevant we should switch to using
      // `inotify()` or similar to do real-time tracking of the db size.
      auto size = recursive_size(self->state.dbdir);
      VAST_LOG_SPD_VERBOSE("{} checks db-directory of size {} bytes",
                           detail::id_or_name(self), size);
      if (size > self->state.high_water_mark && !self->state.purging) {
        self->state.purging = true;
        self->send(self, atom::erase_v);
      }
    },
    [=](atom::erase) {
      // Make sure the `purging` state will be reset once all continuations
      // have finished or we encountered an error.
      auto shared_guard
        = make_shared_guard([=] { self->state.purging = false; });
      directory index_dir = dbdir / "index";
      // TODO(ch20006): Add some check on the overall structure on the db dir.
      std::vector<partition_diskstate> partitions;
      for (auto file : index_dir) {
        auto partition = file.basename().str();
        if (partition == "index.bin")
          continue;
        uuid id;
        if (!parsers::uuid(partition, id)) {
          VAST_LOG_SPD_VERBOSE("{} failed to find partition {}",
                               detail::id_or_name(self), partition);
          continue;
        }
        // TODO: Wrap a more generic `stat()` using `vast::path`.
        struct stat statbuf;
        if (::stat(file.complete().str().c_str(), &statbuf) < 0)
          continue;
        partitions.push_back({id, statbuf.st_size, statbuf.st_mtime});
      }
      if (partitions.empty()) {
        VAST_LOG_SPD_VERBOSE("{} failed to find any partitions to delete",
                             detail::id_or_name(self));
        return;
      }
      VAST_LOG_SPD_DEBUG("{} found {} partitions on disk",
                         detail::id_or_name(self), partitions.size());
      std::sort(partitions.begin(), partitions.end(),
                [](const auto& lhs, const auto& rhs) {
                  return lhs.mtime < rhs.mtime;
                });
      auto oldest = partitions.front();
      VAST_LOG_SPD_VERBOSE("{} erases partition {} from index",
                           detail::id_or_name(self), oldest.id);
      self->request(index, caf::infinite, atom::erase_v, oldest.id)
        .then(
          [=, sg = shared_guard](ids erased_ids) {
            // TODO: It would be more natural if we could chain these futures,
            // instead of nesting them.
            VAST_LOG_SPD_VERBOSE("{} erases removed ids from archive",
                                 detail::id_or_name(self));
            self
              ->request(self->state.archive, caf::infinite, atom::erase_v,
                        erased_ids)
              .then(
                [=, sg = shared_guard](atom::done) {
                  auto sz = recursive_size(self->state.dbdir);
                  VAST_LOG_SPD_VERBOSE("{} erased ids from index; {} bytes "
                                       "left on disk",
                                       detail::id_or_name(self), sz);
                  if (sz > self->state.low_water_mark) {
                    // Repeat until we're below the low water mark
                    self->send(self, atom::erase_v);
                  }
                },
                [=, sg = shared_guard](caf::error e) {
                  VAST_LOG_SPD_WARN("{} failed to erase from archive: {}",
                                    detail::id_or_name(self), render(e));
                });
          },
          [=, sg = shared_guard](caf::error e) {
            VAST_LOG_SPD_WARN("{} failed to erase from index: {}",
                              detail::id_or_name(self), render(e));
          });
    },
    [=](atom::status, status_verbosity) {
      // TODO: Return some useful information here.
      return caf::settings{};
    },
  };
}

} // namespace vast::system
