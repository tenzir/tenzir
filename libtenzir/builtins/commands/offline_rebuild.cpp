//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// The offline rebuild tool, exposed as the `tenzir-rebuild` persona. It scans
// a state directory for partitions, reports their sizes, schemas, and import
// time ranges, and then consolidates partitions that are adjacent in import
// time into fewer, larger partitions. It operates directly on the on-disk
// state without spawning any actors and without connecting to a node.

#include "tenzir/fwd.hpp"

#include "tenzir/active_partition.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/command.hpp"
#include "tenzir/concept/convertible/to.hpp"
#include "tenzir/concept/parseable/numeric/integral.hpp"
#include "tenzir/concept/parseable/tenzir/uuid.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/data.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/available_memory.hpp"
#include "tenzir/detail/load_contents.hpp"
#include "tenzir/detail/pid_file.hpp"
#include "tenzir/detail/scope_guard.hpp"
#include "tenzir/detail/settings.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/error.hpp"
#include "tenzir/fbs/partition.hpp"
#include "tenzir/fbs/partition_synopsis.hpp"
#include "tenzir/fbs/partition_transform.hpp"
#include "tenzir/fbs/utils.hpp"
#include "tenzir/flatbuffer.hpp"
#include "tenzir/ids.hpp"
#include "tenzir/index.hpp"
#include "tenzir/index_config.hpp"
#include "tenzir/io/save.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/passive_partition.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/plugin/command.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/plugin/store.hpp"
#include "tenzir/qualified_record_field.hpp"
#include "tenzir/store.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/time_synopsis.hpp"
#include "tenzir/type.hpp"
#include "tenzir/uuid.hpp"

#include <caf/actor_system.hpp>
#include <caf/make_copy_on_write.hpp>
#include <caf/settings.hpp>
#include <sys/ioctl.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <map>
#include <numeric>
#include <system_error>
#include <termios.h>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace tenzir::plugins::offline_rebuild {

namespace {

using namespace std::chrono_literals;

/// The minimum delay between two rate-limited progress reports.
constexpr auto report_interval = 2s;

/// The delay after which a single long-running merge starts reporting
/// intermediate progress.
constexpr auto in_merge_report_interval = 10s;

/// Everything we need to know about one on-disk partition.
struct partition_record {
  uuid id = {};
  type schema = {};
  uint64_t events = 0;
  time min_import_time = {};
  time max_import_time = {};
  uint64_t version = 0;
  uint64_t approx_bytes = 0;
  uint64_t disk_bytes = 0;
  std::filesystem::path partition_path = {};
  std::filesystem::path synopsis_path = {};
  std::filesystem::path store_path = {};
  /// The range of the field that drives time-based grouping. Defaults to the
  /// import-time range, and to the range of the event timestamp field when
  /// span-based cutting is enabled and the schema has one.
  time cut_min = {};
  time cut_max = {};
  /// The name of the field behind `cut_min`/`cut_max`; "import time" when
  /// falling back, empty when span-based cutting is disabled.
  std::string cut_field = {};
};

/// A set of undersized same-schema partitions that are adjacent in import
/// time and get merged into a single partition.
struct merge_group {
  std::vector<partition_record> parts = {};
  uint64_t events = 0;
  uint64_t bytes = 0;
  time cut_min = time::max();
  time cut_max = time::min();
};

template <class... Ts>
void report(fmt::format_string<Ts...> str, Ts&&... xs) {
  fmt::print(stderr, "tenzir-rebuild: {}\n",
             fmt::format(str, std::forward<Ts>(xs)...));
}

auto human_bytes(uint64_t bytes) -> std::string {
  auto value = static_cast<double>(bytes);
  for (const auto* unit : {"B", "KiB", "MiB", "GiB"}) {
    if (value < 1024.0) {
      return fmt::format("{:.1f} {}", value, unit);
    }
    value /= 1024.0;
  }
  return fmt::format("{:.1f} TiB", value);
}

auto file_size_or_zero(const std::filesystem::path& path) -> uint64_t {
  auto err = std::error_code{};
  const auto size = std::filesystem::file_size(path, err);
  return err ? 0 : size;
}

/// The per-schema aggregation of the consolidation plan.
struct schema_plan {
  std::string name = {};
  size_t inputs = 0;
  size_t outputs = 0;
  uint64_t events = 0;
  uint64_t bytes = 0;
  std::string cut_field = {};
  bool selected = true;
};

/// Aggregates the merge groups into one plan entry per schema, ordered by
/// schema name.
auto make_schema_plans(const std::vector<merge_group>& groups)
  -> std::vector<schema_plan> {
  auto per_schema = std::map<std::string, schema_plan>{};
  for (const auto& group : groups) {
    auto name = std::string{group.parts.front().schema.name()};
    auto& plan = per_schema[name];
    plan.name = std::move(name);
    plan.inputs += group.parts.size();
    plan.outputs += 1;
    plan.events += group.events;
    plan.bytes += group.bytes;
    plan.cut_field = group.parts.front().cut_field;
  }
  auto plans = std::vector<schema_plan>{};
  plans.reserve(per_schema.size());
  for (auto& [name, plan] : per_schema) {
    plans.push_back(std::move(plan));
  }
  return plans;
}

auto format_plan_line(const schema_plan& plan) -> std::string {
  return fmt::format(
    "schema `{}`{}: {} partitions into {} ({} events, {})", plan.name,
    plan.cut_field.empty() ? std::string{}
                           : fmt::format(" cut by {}", plan.cut_field),
    plan.inputs, plan.outputs, plan.events, human_bytes(plan.bytes));
}

/// Prints the consolidation plan as one line per schema.
void report_plan(const std::vector<schema_plan>& plans,
                 std::string_view action) {
  for (const auto& plan : plans) {
    report("{} {}", action, format_plan_line(plan));
  }
}

/// Waits for the user to press enter. Returns false when stdin was closed
/// before a newline arrived.
auto wait_for_confirmation() -> bool {
  for (auto c = 0; (c = std::getchar()) != '\n';) {
    if (c == EOF) {
      return false;
    }
  }
  return true;
}

/// Temporarily switches stdin into a raw-ish mode for the interactive
/// selector: no line buffering, no echo, and no signal generation so that
/// ctrl-c aborts gracefully with the terminal restored.
class raw_terminal {
public:
  raw_terminal() {
    if (::tcgetattr(STDIN_FILENO, &saved_) != 0) {
      return;
    }
    auto raw = saved_;
    raw.c_lflag &= ~static_cast<tcflag_t>(ICANON | ECHO | ISIG);
    raw.c_cc[VMIN] = 1;
    raw.c_cc[VTIME] = 0;
    // TCSANOW instead of TCSAFLUSH so that input that arrived before the
    // switch (e.g. from a script driving the tool) is not discarded.
    active_ = ::tcsetattr(STDIN_FILENO, TCSANOW, &raw) == 0;
  }

  ~raw_terminal() {
    if (active_) {
      ::tcsetattr(STDIN_FILENO, TCSANOW, &saved_);
    }
  }

  raw_terminal(const raw_terminal&) = delete;
  raw_terminal(raw_terminal&&) = delete;
  raw_terminal& operator=(const raw_terminal&) = delete;
  raw_terminal& operator=(raw_terminal&&) = delete;

  [[nodiscard]] auto active() const -> bool {
    return active_;
  }

private:
  termios saved_ = {};
  bool active_ = false;
};

auto terminal_width() -> size_t {
  auto ws = winsize{};
  if (::ioctl(STDERR_FILENO, TIOCGWINSZ, &ws) == 0 and ws.ws_col > 0) {
    return ws.ws_col;
  }
  return 120;
}

/// Lets the user pick the schemas to consolidate from an interactive list.
/// Returns false when the user aborted.
auto select_schemas(std::vector<schema_plan>& plans) -> bool {
  auto terminal = raw_terminal{};
  if (not terminal.active()) {
    // Without terminal control, fall back to a plain confirmation.
    fmt::print(stderr, "tenzir-rebuild: press <enter> to start, or <ctrl-c> "
                       "to abort\n");
    return wait_for_confirmation();
  }
  fmt::print(stderr, "tenzir-rebuild: select the schemas to consolidate: "
                     "<up>/<down> move, <space> toggles, <enter> starts, q or "
                     "<ctrl-c> aborts\n");
  const auto width = terminal_width();
  auto cursor = size_t{0};
  const auto redraw = [&](bool first) {
    if (not first) {
      fmt::print(stderr, "\x1b[{}A", plans.size());
    }
    for (size_t i = 0; i < plans.size(); ++i) {
      auto line = fmt::format("{} [{}] {}", i == cursor ? '>' : ' ',
                              plans[i].selected ? 'x' : ' ',
                              format_plan_line(plans[i]));
      if (line.size() >= width) {
        line.resize(width - 1);
      }
      fmt::print(stderr, "\x1b[2K{}\n", line);
    }
  };
  redraw(true);
  const auto read_byte = []() -> int {
    char c = 0;
    return ::read(STDIN_FILENO, &c, 1) == 1 ? static_cast<unsigned char>(c)
                                            : -1;
  };
  const auto up = [&] {
    cursor = cursor > 0 ? cursor - 1 : cursor;
  };
  const auto down = [&] {
    cursor = cursor + 1 < plans.size() ? cursor + 1 : cursor;
  };
  for (;;) {
    switch (read_byte()) {
      case -1: // stdin closed
      case 'q':
      case 0x03: // ctrl-c
      case 0x04: // ctrl-d
        return false;
      case '\r':
      case '\n':
        return true;
      case ' ':
        plans[cursor].selected = not plans[cursor].selected;
        break;
      case 'k':
        up();
        break;
      case 'j':
        down();
        break;
      case 0x1b: { // escape sequence
        if (read_byte() != '[') {
          break;
        }
        switch (read_byte()) {
          case 'A':
            up();
            break;
          case 'B':
            down();
            break;
          default:
            break;
        }
        break;
      }
      default:
        break;
    }
    redraw(false);
  }
}

/// Probes the archive directory for the store file of the given uuid.
auto probe_store_path(const std::filesystem::path& state_dir, const uuid& id)
  -> std::filesystem::path {
  auto err = std::error_code{};
  for (const auto* ext : {"store", "feather", "parquet"}) {
    auto candidate = state_dir / "archive" / fmt::format("{}.{}", id, ext);
    if (std::filesystem::exists(candidate, err)) {
      return candidate;
    }
  }
  return {};
}

/// Refuses to continue if the state directory is in use by a live process,
/// and claims it for this process when `claim` is set. Unlike the check
/// performed by the node at startup we refuse for *any* live process, because
/// operating on a state directory that is concurrently modified corrupts
/// data. A dry run only checks and does not claim, so that it works on
/// read-only state directories and modifies nothing.
auto claim_state_directory(const std::filesystem::path& state_dir, bool claim)
  -> caf::error {
  const auto pid_file = state_dir / "pid.lock";
  auto err = std::error_code{};
  if (std::filesystem::exists(pid_file, err)) {
    auto contents = detail::load_contents(pid_file);
    if (not contents) {
      return diagnostic::error(contents.error())
        .note("failed to read `{}`", pid_file)
        .to_error();
    }
    const auto other_pid = to<int32_t>(*contents).value_or(-1);
    if (other_pid > 0 and other_pid != ::getpid()
        and ::getpgid(other_pid) >= 0) {
      return caf::make_error(
        ec::filesystem_error,
        fmt::format("state directory {} is in use by process {}; stop the "
                    "tenzir-node process first, or remove {} if the PID does "
                    "not belong to a Tenzir process",
                    state_dir, other_pid, pid_file));
    }
    if (not claim) {
      return caf::none;
    }
    // The previous owner is gone; remove the stale file so that the pid file
    // acquisition below starts from a clean slate.
    std::filesystem::remove(pid_file, err);
  }
  if (not claim) {
    return caf::none;
  }
  return detail::acquire_pid_file(pid_file);
}

/// Finishes up leftover partition transforms from an unclean shutdown, using
/// the same semantics as the index at startup: outputs recorded in a marker
/// are moved into place first, then the inputs are erased. A marker whose
/// outputs cannot be restored or whose inputs cannot be erased is kept and
/// the recovery fails, so that no data becomes unreachable.
auto replay_markers(const std::filesystem::path& state_dir,
                    const std::filesystem::path& index_dir) -> caf::error {
  const auto markers_dir = index_dir / "markers";
  auto err = std::error_code{};
  if (not std::filesystem::is_directory(markers_dir, err)) {
    return caf::none;
  }
  // Move a file into place, treating an already-moved source as success.
  const auto restore = [](const std::filesystem::path& from,
                          const std::filesystem::path& to) -> bool {
    auto ec = std::error_code{};
    if (std::filesystem::exists(to, ec)) {
      std::filesystem::remove(from, ec);
      return true;
    }
    if (not std::filesystem::exists(from, ec)) {
      return false;
    }
    std::filesystem::rename(from, to, ec);
    return not ec;
  };
  auto replayed = size_t{0};
  auto kept = size_t{0};
  for (const auto& entry :
       std::filesystem::directory_iterator(markers_dir, err)) {
    if (entry.path().extension() != ".marker") {
      continue;
    }
    auto chunk = chunk::mmap(entry.path());
    if (not chunk) {
      return diagnostic::error(chunk.error())
        .note("failed to read leftover transform marker `{}`", entry.path())
        .to_error();
    }
    auto maybe_flatbuffer
      = flatbuffer<fbs::PartitionTransform>::make(std::move(*chunk));
    if (not maybe_flatbuffer) {
      return diagnostic::error(maybe_flatbuffer.error())
        .note("failed to parse leftover transform marker `{}`", entry.path())
        .to_error();
    }
    const auto& transform = *maybe_flatbuffer;
    if (transform->transform_type()
        != fbs::partition_transform::PartitionTransform::v0) {
      return caf::make_error(ec::format_error,
                             fmt::format("unknown version of leftover "
                                         "transform marker `{}`",
                                         entry.path()));
    }
    const auto* transform_v0 = transform->transform_as_v0();
    // Restore the outputs before erasing anything so that a failure keeps all
    // data recoverable.
    auto restored = true;
    if (const auto* outputs = transform_v0->output_partitions()) {
      for (const auto* id : *outputs) {
        const auto output_id = uuid::from_flatbuffer(*id);
        const auto name = fmt::format("{:l}", output_id);
        // A store written by a streaming merge stays at its temporary path
        // until the marker is durable; promote it on replay.
        for (const auto* ext : {"store", "feather", "parquet"}) {
          const auto final_path
            = state_dir / "archive" / fmt::format("{}.{}", output_id, ext);
          const auto tmp_path
            = std::filesystem::path{final_path.string() + ".tmp"};
          auto ec = std::error_code{};
          if (std::filesystem::exists(tmp_path, ec)) {
            restored = restore(tmp_path, final_path) and restored;
          }
        }
        restored = restore(markers_dir / name, index_dir / name) and restored;
        restored
          = restore(markers_dir / (name + ".mdx"), index_dir / (name + ".mdx"))
            and restored;
      }
    }
    if (not restored) {
      // Move every output that already sits under its final name back into
      // the markers directory—the loop above may have installed some outputs
      // before failing, and a previous crashed run may have installed
      // others—so that the index never sees a marker's outputs alongside its
      // inputs. Once input erasure has begun the outputs stay in place
      // instead, as they may be the only remaining copy of the data.
      const auto restage = [](const std::filesystem::path& final_path,
                              const std::filesystem::path& staged_path) {
        auto ec = std::error_code{};
        if (not std::filesystem::exists(final_path, ec)) {
          return;
        }
        if (std::filesystem::exists(staged_path, ec)) {
          std::filesystem::remove(final_path, ec);
        } else {
          std::filesystem::rename(final_path, staged_path, ec);
        }
        if (ec) {
          report("failed to move the partially installed transform output "
                 "`{}` back into the markers directory: {}",
                 final_path, ec.message());
        }
      };
      if (const auto* outputs = transform_v0->output_partitions()) {
        for (const auto* id : *outputs) {
          const auto name = fmt::format("{:l}", uuid::from_flatbuffer(*id));
          restage(index_dir / name, markers_dir / name);
          restage(index_dir / (name + ".mdx"), markers_dir / (name + ".mdx"));
        }
      }
      report("keeping transform marker `{}`: failed to restore its outputs",
             entry.path());
      ++kept;
      continue;
    }
    // Erase the inputs; files that are already gone do not count as errors.
    auto erased = true;
    const auto erase = [&erased](const std::filesystem::path& path) {
      auto ec = std::error_code{};
      std::filesystem::remove(path, ec);
      if (ec) {
        erased = false;
      }
    };
    if (const auto* inputs = transform_v0->input_partitions()) {
      for (const auto* id : *inputs) {
        const auto input_id = uuid::from_flatbuffer(*id);
        erase(index_dir / fmt::format("{:l}", input_id));
        erase(index_dir / fmt::format("{:l}.mdx", input_id));
        if (auto store_path = probe_store_path(state_dir, input_id);
            not store_path.empty()) {
          erase(store_path);
        }
      }
    }
    if (not erased) {
      report("keeping transform marker `{}`: failed to erase its inputs",
             entry.path());
      ++kept;
      continue;
    }
    auto ec = std::error_code{};
    std::filesystem::remove(entry.path(), ec);
    if (ec) {
      ++kept;
      continue;
    }
    ++replayed;
  }
  if (kept > 0) {
    return caf::make_error(
      ec::filesystem_error,
      fmt::format("failed to finish {} interrupted partition transform(s); "
                  "their markers remain in `{}`, resolve the underlying "
                  "filesystem problem and run again",
                  kept, markers_dir));
  }
  // Remove stray temporary stores from merges that died before their marker
  // became durable. Safe now that no marker references a temporary file.
  for (const auto& archive_entry : std::filesystem::directory_iterator(
         state_dir / "archive",
         std::filesystem::directory_options::skip_permission_denied, err)) {
    if (archive_entry.path().extension() == ".tmp") {
      auto ec = std::error_code{};
      std::filesystem::remove(archive_entry.path(), ec);
    }
  }
  std::filesystem::remove(markers_dir, err);
  if (replayed > 0) {
    report("finished {} interrupted partition transform(s) left over from an "
           "unclean shutdown",
           replayed);
  }
  return caf::none;
}

/// The field names that qualify as the event timestamp for time-based
/// grouping, in order of preference. Matched case-insensitively against the
/// last path segment of every time-typed field.
constexpr auto cut_field_names
  = std::array<std::string_view, 8>{"timestamp",  "time",      "ts",
                                    "event_time", "eventtime", "datetime",
                                    "date_time",  "_time"};

/// Finds the best matching event timestamp field in a partition synopsis and
/// returns its name together with the min/max range recorded in the field's
/// time synopsis.
auto extract_cut_range(const partition_synopsis& ps)
  -> Option<std::tuple<std::string, time, time>> {
  auto best_rank = cut_field_names.size();
  auto result = Option<std::tuple<std::string, time, time>>{};
  for (const auto& [field, synopsis] : ps.field_synopses_) {
    if (not synopsis) {
      continue;
    }
    const auto* ts = dynamic_cast<const time_synopsis*>(synopsis.get());
    if (not ts or ts->min() > ts->max()) {
      continue;
    }
    auto leaf = std::string{field.field_name()};
    if (const auto pos = leaf.rfind('.'); pos != std::string::npos) {
      leaf.erase(0, pos + 1);
    }
    std::transform(leaf.begin(), leaf.end(), leaf.begin(), [](unsigned char c) {
      return std::tolower(c);
    });
    const auto* match
      = std::find(cut_field_names.begin(), cut_field_names.end(), leaf);
    const auto rank
      = static_cast<size_t>(std::distance(cut_field_names.begin(), match));
    if (rank < best_rank) {
      best_rank = rank;
      result
        = std::tuple{std::string{field.field_name()}, ts->min(), ts->max()};
    }
  }
  return result;
}

/// Reads the metadata of a single partition, preferring the synopsis file
/// and falling back to the partition file itself.
auto load_record(const std::filesystem::path& state_dir,
                 const std::filesystem::path& index_dir, const uuid& id,
                 bool cut_by_field) -> caf::expected<partition_record> {
  auto rec = partition_record{};
  rec.id = id;
  rec.partition_path = index_dir / fmt::format("{:l}", id);
  rec.synopsis_path = index_dir / fmt::format("{:l}.mdx", id);
  rec.store_path = probe_store_path(state_dir, id);
  rec.disk_bytes = file_size_or_zero(rec.partition_path)
                   + file_size_or_zero(rec.synopsis_path)
                   + file_size_or_zero(rec.store_path);
  auto ps = partition_synopsis{};
  auto have_synopsis = false;
  auto err = std::error_code{};
  if (std::filesystem::exists(rec.synopsis_path, err)) {
    if (auto chunk = chunk::mmap(rec.synopsis_path)) {
      if (auto fb = flatbuffer<fbs::PartitionSynopsis>::make(std::move(*chunk));
          fb
          and (*fb)->partition_synopsis_type()
                == fbs::partition_synopsis::PartitionSynopsis::legacy) {
        if (auto error = unpack(*(*fb)->partition_synopsis_as_legacy(), ps,
                                /*lazy_sketches=*/true);
            not error.valid()) {
          have_synopsis = true;
        }
      }
    }
  }
  if (not have_synopsis) {
    auto chunk = chunk::mmap(rec.partition_path);
    if (not chunk) {
      return diagnostic::error(chunk.error())
        .note("failed to read partition `{}`", rec.partition_path)
        .to_error();
    }
    auto maybe_partition = partition_chunk::get_flatbuffer(*chunk);
    if (not maybe_partition) {
      return diagnostic::error(maybe_partition.error())
        .note("malformed partition `{}`", rec.partition_path)
        .to_error();
    }
    if ((*maybe_partition)->partition_type()
        != fbs::partition::Partition::legacy) {
      return caf::make_error(ec::format_error,
                             fmt::format("unknown version of partition `{}`",
                                         rec.partition_path));
    }
    if (auto error = unpack(*(*maybe_partition)->partition_as_legacy(), ps);
        error.valid()) {
      return diagnostic::error(error)
        .note("failed to read synopsis of partition `{}`", rec.partition_path)
        .to_error();
    }
  }
  rec.schema = ps.schema;
  rec.events = ps.events;
  rec.min_import_time = ps.min_import_time;
  rec.max_import_time = ps.max_import_time;
  rec.version = ps.version;
  rec.approx_bytes = ps.approx_bytes;
  rec.cut_min = ps.min_import_time;
  rec.cut_max = ps.max_import_time;
  if (cut_by_field) {
    rec.cut_field = "import time";
    if (auto range = extract_cut_range(ps)) {
      rec.cut_field = std::move(std::get<0>(*range));
      rec.cut_min = std::get<1>(*range);
      rec.cut_max = std::get<2>(*range);
    }
  }
  return rec;
}

/// Scans the index directory for partitions and loads their metadata.
auto scan_partitions(const std::filesystem::path& state_dir,
                     const std::filesystem::path& index_dir, bool cut_by_field)
  -> caf::expected<std::vector<partition_record>> {
  auto err = std::error_code{};
  auto candidates = std::vector<uuid>{};
  for (const auto& entry :
       std::filesystem::directory_iterator(index_dir, err)) {
    auto id = uuid{};
    if (not entry.path().extension().empty()
        or not parsers::uuid(entry.path().stem().string(), id)) {
      continue;
    }
    candidates.push_back(id);
  }
  if (err) {
    return caf::make_error(ec::filesystem_error,
                           fmt::format("failed to list directory contents of "
                                       "{}: {}",
                                       index_dir, err.message()));
  }
  auto records = std::vector<partition_record>{};
  records.reserve(candidates.size());
  auto last_report = std::chrono::steady_clock::now();
  for (const auto& id : candidates) {
    auto rec = load_record(state_dir, index_dir, id, cut_by_field);
    if (not rec) {
      report("skipping unreadable partition {}: {}", id,
             render(rec.error(), false));
      continue;
    }
    records.push_back(std::move(*rec));
    if (const auto now = std::chrono::steady_clock::now();
        now - last_report >= report_interval) {
      last_report = now;
      report("scanned {}/{} partitions", records.size(), candidates.size());
    }
  }
  return records;
}

/// The threshold relative to the maximum partition size below which a
/// partition counts as undersized, matching the online rebuilder.
constexpr auto undersized_threshold = 0.8;

/// Groups partitions by schema and then greedily merges runs of undersized
/// partitions that are adjacent in import time, bounded by a memory budget
/// for a single merge.
///
/// Mirroring the online rebuilder, a group keeps growing while its running
/// total is below `undersized_threshold` times the maximum partition size and
/// only stops after crossing it. Merged outputs are therefore never
/// undersized again, at the cost of a single output exceeding the maximum
/// partition size by less than one input partition. Partitions that are
/// already at or above the threshold are never rewritten; they act as
/// boundaries between groups so that a merged partition never spans the
/// import-time range of a partition that stays in place.
///
/// When `max_span` is positive, a group additionally only grows while the
/// union of its partitions' timestamp ranges stays within the span, so merged
/// partitions cover no more than that much time. A partition whose own range
/// already exceeds the span cannot be shrunk without reading its events and
/// stays in place like a full partition.
auto plan_consolidation(std::vector<partition_record> records,
                        uint64_t max_events, uint64_t byte_budget,
                        duration max_span) -> std::vector<merge_group> {
  const auto target_events = static_cast<uint64_t>(
    undersized_threshold * static_cast<double>(max_events));
  auto by_schema = std::unordered_map<type, std::vector<partition_record>>{};
  for (auto& rec : records) {
    if (not rec.schema) {
      // Heterogeneous partitions predating version 1 have no schema in their
      // synopsis; they cannot be consolidated by this tool.
      report("skipping partition {} with unsupported version {}", rec.id,
             rec.version);
      continue;
    }
    by_schema[rec.schema].push_back(std::move(rec));
  }
  auto groups = std::vector<merge_group>{};
  for (auto& [schema, parts] : by_schema) {
    // Learn the decoded size per event from the partitions of this schema
    // that carry an estimate, and use the ratio to size those that do not
    // (e.g. partitions written before the estimate existed). Their on-disk
    // size alone underestimates the decoded size by up to an order of
    // magnitude due to compression, which would blow the memory budget.
    auto known_bytes = uint64_t{0};
    auto known_events = uint64_t{0};
    for (const auto& rec : parts) {
      if (rec.approx_bytes > 0) {
        known_bytes += rec.approx_bytes;
        known_events += rec.events;
      }
    }
    const auto estimated_bytes = [&](const partition_record& rec) -> uint64_t {
      if (rec.approx_bytes > 0) {
        return rec.approx_bytes;
      }
      if (known_events > 0) {
        return std::max(rec.disk_bytes,
                        rec.events * (known_bytes / known_events + 1));
      }
      // Nothing to extrapolate from; assume the store shrank by this much
      // through compression.
      constexpr auto assumed_compression_factor = uint64_t{10};
      return rec.disk_bytes * assumed_compression_factor;
    };
    std::sort(parts.begin(), parts.end(),
              [](const partition_record& lhs, const partition_record& rhs) {
                return std::tie(lhs.cut_min, lhs.cut_max, lhs.id)
                       < std::tie(rhs.cut_min, rhs.cut_max, rhs.id);
              });
    auto current = merge_group{};
    const auto flush = [&] {
      if (current.parts.size() > 1) {
        groups.push_back(std::move(current));
      }
      current = merge_group{};
    };
    for (auto& rec : parts) {
      if (rec.events >= target_events
          or (max_span > duration::zero()
              and rec.cut_max - rec.cut_min > max_span)) {
        // The partition is not undersized, or it alone already covers more
        // time than the allowed span; leave it in place. It closes the
        // current group so that no merged partition spans its range.
        flush();
        continue;
      }
      const auto bytes = estimated_bytes(rec);
      // Stop growing a group only after its total crossed the target, like
      // the online rebuilder; the memory budget and the time span remain hard
      // limits that are checked before adding.
      if (not current.parts.empty()
          and (current.events >= target_events
               or current.bytes + bytes > byte_budget
               or (max_span > duration::zero()
                   and std::max(current.cut_max, rec.cut_max)
                           - std::min(current.cut_min, rec.cut_min)
                         > max_span))) {
        flush();
      }
      current.events += rec.events;
      current.bytes += bytes;
      current.cut_min = std::min(current.cut_min, rec.cut_min);
      current.cut_max = std::max(current.cut_max, rec.cut_max);
      current.parts.push_back(std::move(rec));
    }
    flush();
  }
  // Process large groups first so that the biggest space savings materialize
  // early when the tool is interrupted.
  std::sort(groups.begin(), groups.end(),
            [](const merge_group& lhs, const merge_group& rhs) {
              return lhs.parts.size() > rhs.parts.size();
            });
  return groups;
}

auto serialize_synopsis(const partition_synopsis& synopsis, bool verify)
  -> caf::expected<chunk_ptr> {
  flatbuffers::FlatBufferBuilder builder;
  const auto ps = pack(builder, synopsis);
  if (not ps) {
    return ps.error();
  }
  fbs::PartitionSynopsisBuilder ps_builder(builder);
  ps_builder.add_partition_synopsis_type(
    fbs::partition_synopsis::PartitionSynopsis::legacy);
  ps_builder.add_partition_synopsis(ps->Union());
  auto ps_offset = ps_builder.Finish();
  fbs::FinishPartitionSynopsisBuffer(builder, ps_offset);
  auto chunk = fbs::release(builder);
  if (verify) {
    if (auto checked
        = flatbuffer<fbs::PartitionSynopsis>::make(chunk_ptr{chunk});
        not checked) {
      return diagnostic::error(checked.error())
        .note("refusing to write malformed partition synopsis")
        .to_error();
    }
  }
  return chunk;
}

struct merge_result {
  uuid output_id = {};
  uint64_t events = 0;
  uint64_t bytes_written = 0;
};

/// Merges all partitions of a group into a single new partition and swaps it
/// in for the inputs, using the same marker protocol as the node so that an
/// interrupted merge is finished by the next run or by the node at startup.
auto execute_merge(const merge_group& group,
                   const std::filesystem::path& state_dir,
                   const std::filesystem::path& index_dir,
                   const index_config& synopsis_opts,
                   uint64_t partition_capacity,
                   const std::string& store_backend, uint64_t byte_budget,
                   std::string_view progress) -> caf::expected<merge_result> {
  const auto* out_plugin = plugins::find<store_plugin>(store_backend);
  if (not out_plugin) {
    return caf::make_error(ec::invalid_configuration,
                           fmt::format("unknown store backend `{}`",
                                       store_backend));
  }
  auto out_store = out_plugin->make_active_store();
  if (not out_store) {
    return diagnostic::error(out_store.error())
      .note("failed to create `{}` store", store_backend)
      .to_error();
  }
  (*out_store)->set_origin("rebuild");
  const auto schema = group.parts.front().schema;
  auto data = active_partition_state::serialization_data{};
  data.id = uuid::random();
  data.store_id = store_backend;
  data.store_header = chunk::copy(data.id);
  data.synopsis = caf::make_copy_on_write<partition_synopsis>();
  auto& synopsis = data.synopsis.unshared();
  synopsis.schema = schema;
  auto input_store_paths = std::vector<std::filesystem::path>{};
  input_store_paths.reserve(group.parts.size());
  const auto merge_start = std::chrono::steady_clock::now();
  auto last_progress = merge_start;
  const auto maybe_report_progress = [&](size_t inputs_done) {
    const auto now = std::chrono::steady_clock::now();
    if (now - last_progress < in_merge_report_interval) {
      return;
    }
    last_progress = now;
    report("{} still merging {} partitions of schema `{}`: {}/{} inputs, {} "
           "events, running for {}",
           progress, group.parts.size(), schema.name(), inputs_done,
           group.parts.size(), data.events,
           std::chrono::duration_cast<std::chrono::seconds>(now - merge_start));
  };
  const auto memory_before = detail::available_memory();
  for (size_t input = 0; input < group.parts.size(); ++input) {
    const auto& rec = group.parts[input];
    // Guard against blowing far past the memory budget when the size
    // estimates turn out to be wrong: abort the merge cleanly instead of
    // driving the machine into swap. Nothing has been persisted at this
    // point, so aborting leaves the inputs untouched.
    if (memory_before) {
      if (const auto current = detail::available_memory()) {
        const auto used = memory_before->bytes > current->bytes
                            ? memory_before->bytes - current->bytes
                            : uint64_t{0};
        if (used > byte_budget + byte_budget / 2) {
          return caf::make_error(
            ec::out_of_memory,
            fmt::format("merge of {} partitions of schema `{}` exceeded the "
                        "memory budget ({} used, {} budget); use "
                        "--max-merge-memory to adjust the budget",
                        group.parts.size(), schema.name(), human_bytes(used),
                        human_bytes(byte_budget)));
        }
      }
    }
    auto partition_chunk = chunk::mmap(rec.partition_path);
    if (not partition_chunk) {
      return diagnostic::error(partition_chunk.error())
        .note("failed to read partition `{}`", rec.partition_path)
        .to_error();
    }
    auto pstate = passive_partition_state{};
    if (auto error = pstate.initialize_from_chunk(*partition_chunk);
        error.valid()) {
      return diagnostic::error(error)
        .note("failed to load partition {}", rec.id)
        .to_error();
    }
    if (pstate.id != rec.id) {
      // A misnamed or copied partition file may reference a store that a
      // partition under another name still needs; refuse to consume it.
      return caf::make_error(
        ec::format_error,
        fmt::format("partition file `{}` contains partition {} instead of {}",
                    rec.partition_path, pstate.id, rec.id));
    }
    const auto* in_plugin = plugins::find<store_plugin>(pstate.store_id);
    if (not in_plugin) {
      return caf::make_error(ec::format_error,
                             fmt::format("partition {} uses the store backend "
                                         "`{}` that this tool "
                                         "does not support",
                                         rec.id, pstate.store_id));
    }
    if (pstate.store_header.size() != uuid::num_bytes) {
      return caf::make_error(
        ec::format_error,
        fmt::format("unexpected store header size for partition {}: expected "
                    "{}, got {}",
                    rec.id, uuid::num_bytes, pstate.store_header.size()));
    }
    const auto store_uuid
      = uuid{pstate.store_header.subspan<0, uuid::num_bytes>()};
    if (store_uuid != rec.id) {
      // The supported store backends always name a partition's store after
      // the partition itself. A damaged header referencing another
      // partition's store would make this merge consume and later delete data
      // that a partition outside of the merge still needs.
      return caf::make_error(ec::format_error,
                             fmt::format("partition {} references the store of "
                                         "a different "
                                         "partition {}",
                                         rec.id, store_uuid));
    }
    auto store_path = state_dir / "archive"
                      / fmt::format("{}.{}", store_uuid, pstate.store_id);
    auto store_chunk = chunk::mmap(store_path);
    if (not store_chunk) {
      return diagnostic::error(store_chunk.error())
        .note("failed to read store for partition {} at `{}`", rec.id,
              store_path)
        .to_error();
    }
    auto in_store = in_plugin->make_passive_store();
    if (not in_store) {
      return diagnostic::error(in_store.error())
        .note("failed to create `{}` store", pstate.store_id)
        .to_error();
    }
    if (auto error = (*in_store)->load(std::move(*store_chunk));
        error.valid()) {
      return diagnostic::error(error)
        .note("failed to load store for partition {}", rec.id)
        .to_error();
    }
    for (auto&& maybe_slice : (*in_store)->slices()) {
      maybe_report_progress(input);
      if (not maybe_slice) {
        return diagnostic::error(maybe_slice.error())
          .note("failed to read store for partition {}", rec.id)
          .to_error();
      }
      auto slice = std::move(*maybe_slice);
      if (slice.rows() == 0) {
        continue;
      }
      if (slice.schema() != schema) {
        return caf::make_error(ec::format_error,
                               fmt::format("partition {} unexpectedly contains "
                                           "events of schema "
                                           "`{}` instead of `{}`",
                                           rec.id, slice.schema(), schema));
      }
      if (slice.import_time() == time{}) {
        slice.import_time(rec.min_import_time);
      }
      slice.offset(data.events);
      synopsis.min_import_time
        = std::min(synopsis.min_import_time, slice.import_time());
      synopsis.max_import_time
        = std::max(synopsis.max_import_time, slice.import_time());
      auto& ids = data.type_ids[std::string{slice.schema().name()}];
      const auto first = slice.offset();
      const auto last = first + slice.rows();
      TENZIR_ASSERT(first >= ids.size());
      ids.append_bits(false, first - ids.size());
      ids.append_bits(true, last - first);
      data.events += slice.rows();
      synopsis.add(slice, partition_capacity, synopsis_opts);
      if (auto error = (*out_store)->add({std::move(slice)}); error.valid()) {
        return diagnostic::error(error)
          .note("failed to append events of partition {}", rec.id)
          .to_error();
      }
    }
    input_store_paths.push_back(std::move(store_path));
    maybe_report_progress(input + 1);
  }
  synopsis.shrink();
  synopsis.events = data.events;
  const auto expected_events
    = std::accumulate(group.parts.begin(), group.parts.end(), uint64_t{0},
                      [](uint64_t acc, const partition_record& rec) {
                        return acc + rec.events;
                      });
  if (data.events != expected_events) {
    return caf::make_error(ec::logic_error,
                           fmt::format("refusing to persist merged partition: "
                                       "expected {} events, "
                                       "got {}",
                                       expected_events, data.events));
  }
  // Serialize everything before touching the state directory.
  auto fields = std::vector<struct record_type::field>{};
  for (const auto& [field, offset] : as<record_type>(schema).leaves()) {
    const auto qf = qualified_record_field{schema, offset};
    fields.emplace_back(std::string{qf.name()}, qf.type());
  }
  // Events with an empty record schema have no leaves; `pack_full` supports
  // a default-constructed combined schema for them, like the online partition
  // transformer.
  auto partition_chunk_out
    = pack_full(data, fields.empty() ? record_type{} : record_type{fields});
  if (not partition_chunk_out) {
    return diagnostic::error(partition_chunk_out.error())
      .note("failed to serialize merged partition")
      .to_error();
  }
  auto synopsis_chunk_out = serialize_synopsis(
    *data.synopsis, synopsis_opts.skip_synopsis_verification);
  if (not synopsis_chunk_out) {
    return diagnostic::error(synopsis_chunk_out.error())
      .note("failed to serialize merged partition synopsis")
      .to_error();
  }
  auto store_chunk_out = (*out_store)->finish();
  if (not store_chunk_out) {
    return diagnostic::error(store_chunk_out.error())
      .note("failed to serialize merged store")
      .to_error();
  }
  // Persist the outputs: first the store, then partition and synopsis staged
  // in the markers directory, then the marker that records the swap. Once the
  // marker exists the merge is durable; a crash before that leaves at most an
  // orphaned output that the cleanup below or a later run removes.
  const auto markers_dir = index_dir / "markers";
  auto err = std::error_code{};
  std::filesystem::create_directories(markers_dir, err);
  if (err) {
    return caf::make_error(ec::filesystem_error,
                           fmt::format("failed to create `{}`: {}", markers_dir,
                                       err.message()));
  }
  const auto output_name = fmt::format("{:l}", data.id);
  const auto store_out_path
    = state_dir / "archive" / fmt::format("{}.{}", data.id, store_backend);
  const auto staged_partition = markers_dir / output_name;
  const auto staged_synopsis = markers_dir / (output_name + ".mdx");
  const auto marker_path
    = markers_dir / fmt::format("{:l}.marker", uuid::random());
  auto cleanup_staged = detail::scope_guard{[&]() noexcept {
    auto ec = std::error_code{};
    std::filesystem::remove(staged_partition, ec);
    std::filesystem::remove(staged_synopsis, ec);
  }};
  const auto save = [](const std::filesystem::path& path,
                       const chunk_ptr& chunk) -> caf::error {
    if (auto error = io::save(path, std::span{chunk->data(), chunk->size()});
        error.valid()) {
      return diagnostic::error(error)
        .note("failed to write `{}`", path)
        .to_error();
    }
    return caf::none;
  };
  if (auto error = save(store_out_path, *store_chunk_out); error.valid()) {
    return error;
  }
  if (auto error = save(staged_partition, *partition_chunk_out);
      error.valid()) {
    return error;
  }
  if (auto error = save(staged_synopsis, *synopsis_chunk_out); error.valid()) {
    return error;
  }
  auto input_ids = std::vector<uuid>{};
  input_ids.reserve(group.parts.size());
  for (const auto& rec : group.parts) {
    input_ids.push_back(rec.id);
  }
  auto marker
    = create_marker(input_ids, {data.id}, keep_original_partition::no);
  if (auto error = save(marker_path, marker); error.valid()) {
    return error;
  }
  // The swap is durable now; perform it. Failures below leave the marker in
  // place so that the next run or the node finishes the swap.
  cleanup_staged.disable();
  std::filesystem::rename(store_tmp_path, store_out_path, err);
  if (err) {
    return caf::make_error(ec::filesystem_error,
                           fmt::format("failed to move merged store into "
                                       "place: {}",
                                       err.message()));
  }
  std::filesystem::rename(staged_partition, index_dir / output_name, err);
  if (err) {
    return caf::make_error(ec::filesystem_error,
                           fmt::format("failed to move merged partition into "
                                       "place: {}",
                                       err.message()));
  }
  std::filesystem::rename(staged_synopsis, index_dir / (output_name + ".mdx"),
                          err);
  if (err) {
    return caf::make_error(ec::filesystem_error,
                           fmt::format("failed to move merged partition "
                                       "synopsis into place: {}",
                                       err.message()));
  }
  // Erase the inputs. Only remove the marker if every input is verifiably
  // gone; otherwise a later replay must finish the erasure, or exports could
  // return the same events from both the inputs and the merged output.
  auto inputs_erased = true;
  const auto erase_input = [&](const std::filesystem::path& path) {
    auto ec = std::error_code{};
    std::filesystem::remove(path, ec);
    if (ec) {
      inputs_erased = false;
    }
  };
  for (size_t i = 0; i < group.parts.size(); ++i) {
    const auto& rec = group.parts[i];
    erase_input(rec.partition_path);
    erase_input(rec.synopsis_path);
    erase_input(input_store_paths[i]);
  }
  if (not inputs_erased) {
    // The merged output is installed and consistent, but automation must see
    // the incomplete cleanup in the exit status.
    return caf::make_error(
      ec::filesystem_error,
      fmt::format("failed to erase all inputs of the merge into {}; the "
                  "transform marker remains and the next run finishes the "
                  "swap",
                  data.id));
  }
  std::filesystem::remove(marker_path, err);
  return merge_result{
    .output_id = data.id,
    .events = data.events,
    .bytes_written = file_size_or_zero(index_dir / output_name)
                     + file_size_or_zero(index_dir / (output_name + ".mdx"))
                     + file_size_or_zero(store_out_path),
  };
}

auto offline_rebuild_command(const invocation& inv, caf::actor_system&)
  -> caf::message {
  const auto start_time = std::chrono::steady_clock::now();
  const auto dry_run
    = caf::get_or(inv.options, "tenzir.offline-rebuild.dry-run", false);
  const auto max_events = caf::get_or(inv.options, "tenzir.max-partition-size",
                                      defaults::max_partition_size);
  const auto max_span = caf::get_or(
    inv.options, "tenzir.offline-rebuild.max-partition-span", duration::zero());
  if (max_span < duration::zero()) {
    return caf::make_message(
      caf::make_error(ec::invalid_argument,
                      "--max-partition-span must be a positive duration"));
  }
  const auto store_backend = std::string{defaults::store_backend};
  auto synopsis_opts = index_config{};
  if (const auto* index_settings = caf::get_if(&inv.options, "tenzir.index")) {
    const auto index_settings_data = to<data>(*index_settings);
    if (not index_settings_data) {
      return caf::make_message(
        diagnostic::error(index_settings_data.error())
          .note("failed to convert `tenzir.index` configuration")
          .to_error());
    }
    if (auto error = convert(*index_settings_data, synopsis_opts);
        error.valid()) {
      return caf::make_message(
        diagnostic::error(error)
          .note("failed to parse `tenzir.index` configuration")
          .to_error());
    }
  }
  auto err = std::error_code{};
  const auto state_dir = std::filesystem::absolute(
    caf::get_or(inv.options, "tenzir.state-directory",
                std::string{defaults::state_directory}),
    err);
  if (err) {
    return caf::make_message(caf::make_error(
      ec::filesystem_error,
      fmt::format("failed to resolve state directory: {}", err.message())));
  }
  const auto index_dir = state_dir / "index";
  if (not std::filesystem::is_directory(state_dir, err)) {
    return caf::make_message(caf::make_error(
      ec::filesystem_error,
      fmt::format("state directory {} does not exist", state_dir)));
  }
  if (auto error = claim_state_directory(state_dir, /*claim=*/not dry_run);
      error.valid()) {
    return caf::make_message(std::move(error));
  }
  auto release_pid_file = detail::scope_guard{[&]() noexcept {
    if (dry_run) {
      return;
    }
    auto ec = std::error_code{};
    std::filesystem::remove(state_dir / "pid.lock", ec);
  }};
  if (not std::filesystem::is_directory(index_dir, err)) {
    report("state directory {} contains no partitions; nothing to do",
           state_dir);
    return {};
  }
  if (dry_run) {
    // A dry run must not modify anything, but scanning over unrecovered
    // transforms would produce a plan that does not match what the next real
    // run sees, and could double-count events.
    if (std::filesystem::is_directory(index_dir / "markers", err)
        and not std::filesystem::is_empty(index_dir / "markers", err)) {
      return caf::make_message(caf::make_error(
        ec::unspecified,
        "leftover partition transforms from an unclean shutdown exist; run "
        "without --dry-run (or start the node once) to finish them, then "
        "retry"));
    }
  } else if (auto error = replay_markers(state_dir, index_dir); error.valid()) {
    return caf::make_message(std::move(error));
  }
  // Phase 1: scan.
  report("scanning {}", index_dir);
  auto records
    = scan_partitions(state_dir, index_dir, max_span > duration::zero());
  if (not records) {
    return caf::make_message(std::move(records.error()));
  }
  auto schemas = std::unordered_map<type, uint64_t>{};
  auto total_events = uint64_t{0};
  auto total_bytes = uint64_t{0};
  for (const auto& rec : *records) {
    ++schemas[rec.schema];
    total_events += rec.events;
    total_bytes += rec.disk_bytes;
  }
  report("scanned {} partitions: {} events, {} on disk, {} schemas",
         records->size(), total_events, human_bytes(total_bytes),
         schemas.size());
  // Phase 2: plan.
  const auto max_merge_memory = detail::get_bytesize(
    inv.options, "tenzir.offline-rebuild.max-merge-memory", 0);
  if (not max_merge_memory) {
    return caf::make_message(diagnostic::error(max_merge_memory.error())
                               .note("failed to parse `--max-merge-memory`")
                               .to_error());
  }
  const auto byte_budget = *max_merge_memory > 0
                             ? *max_merge_memory
                             : detail::available_memory()
                                   .value_or(detail::available_memory_info{
                                     .bytes = uint64_t{512} * 1024 * 1024,
                                     .source = "fallback",
                                   })
                                   .bytes
                                 / 4;
  auto groups = plan_consolidation(std::move(*records), max_events, byte_budget,
                                   max_span);
  if (groups.empty()) {
    report("all partitions are already consolidated; nothing to do");
    return {};
  }
  const auto planned_inputs
    = std::accumulate(groups.begin(), groups.end(), size_t{0},
                      [](size_t acc, const merge_group& group) {
                        return acc + group.parts.size();
                      });
  report("planned {} merges covering {} partitions (memory budget: {})",
         groups.size(), planned_inputs, human_bytes(byte_budget));
  auto plans = make_schema_plans(groups);
  if (dry_run) {
    report_plan(plans, "would merge");
    return {};
  }
  // Let the user narrow down and confirm the plan, but only when a human can
  // answer.
  if (::isatty(STDIN_FILENO) != 0) {
    if (not select_schemas(plans)) {
      report("aborted");
      return {};
    }
    auto selected = std::unordered_set<std::string>{};
    for (const auto& plan : plans) {
      if (plan.selected) {
        selected.insert(plan.name);
      }
    }
    std::erase_if(groups, [&](const merge_group& group) {
      return not selected.contains(
        std::string{group.parts.front().schema.name()});
    });
    if (groups.empty()) {
      report("no schemas selected; nothing to do");
      return {};
    }
    report("consolidating {} of {} schemas", selected.size(), plans.size());
  } else {
    report_plan(plans, "will merge");
  }
  // Phase 3: consolidate.
  auto merged_partitions = size_t{0};
  auto merged_events = uint64_t{0};
  auto bytes_before = uint64_t{0};
  auto bytes_after = uint64_t{0};
  auto failures = size_t{0};
  auto last_report = std::chrono::steady_clock::now();
  for (size_t i = 0; i < groups.size(); ++i) {
    const auto& group = groups[i];
    auto result = execute_merge(group, state_dir, index_dir, synopsis_opts,
                                max_events, store_backend, byte_budget,
                                fmt::format("[{}/{}]", i + 1, groups.size()));
    if (not result) {
      ++failures;
      report("failed to merge {} partitions of schema `{}`: {}",
             group.parts.size(), group.parts.front().schema.name(),
             render(result.error(), false));
      continue;
    }
    merged_partitions += group.parts.size();
    merged_events += result->events;
    bytes_after += result->bytes_written;
    for (const auto& rec : group.parts) {
      bytes_before += rec.disk_bytes;
    }
    if (const auto now = std::chrono::steady_clock::now();
        now - last_report >= report_interval or i + 1 == groups.size()) {
      last_report = now;
      report("merged {}/{} groups: {} partitions ({} events) so far", i + 1,
             groups.size(), merged_partitions, merged_events);
    }
  }
  if (std::filesystem::is_empty(index_dir / "markers", err)) {
    std::filesystem::remove(index_dir / "markers", err);
  }
  const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
    std::chrono::steady_clock::now() - start_time);
  report("done: merged {} partitions into {} ({} events) in {}; {} -> {} on "
         "disk",
         merged_partitions, groups.size() - failures, merged_events, elapsed,
         human_bytes(bytes_before), human_bytes(bytes_after));
  if (failures > 0) {
    return caf::make_message(
      caf::make_error(ec::unspecified, fmt::format("{} of {} merges failed",
                                                   failures, groups.size())));
  }
  return {};
}

class plugin final : public virtual command_plugin {
public:
  plugin() = default;
  ~plugin() override = default;

  auto name() const -> std::string override {
    return "offline-rebuild";
  }

  auto make_command() const
    -> std::pair<std::unique_ptr<command>, command::factory> override {
    auto cmd = std::make_unique<command>(
      "offline-rebuild",
      "consolidates partitions of a state directory that is not in use by a "
      "node",
      command::opts("?tenzir.offline-rebuild")
        .add<bool>("dry-run", "show the consolidation plan without changing "
                              "anything")
        .add<duration>("max-partition-span",
                       "maximum time range a merged partition may cover, "
                       "measured on the event timestamp field (a time field "
                       "named like 'timestamp', 'time', or 'ts'; import time "
                       "when the schema has none)")
        .add<std::string>("max-merge-memory",
                          "memory budget for a single merge, e.g. 8GiB "
                          "(default: a quarter of the available memory)"));
    auto factory = command::factory{
      {"offline-rebuild", offline_rebuild_command},
    };
    return {std::move(cmd), std::move(factory)};
  }
};

} // namespace

} // namespace tenzir::plugins::offline_rebuild

TENZIR_REGISTER_PLUGIN(tenzir::plugins::offline_rebuild::plugin)
