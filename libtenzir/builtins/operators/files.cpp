//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <sys/stat.h>
#include <sys/types.h>

#include <filesystem>
#include <grp.h>
#include <pwd.h>
#include <unistd.h>
#include <vector>

namespace tenzir::plugins::files {

namespace {

struct files_args {
  std::optional<std::string> path = {};
  bool recurse_directories = {};
  bool follow_directory_symlink = {};
  bool skip_permission_denied = {};

  friend auto inspect(auto& f, files_args& x) -> bool {
    return f.object(x).fields(
      f.field("path", x.path),
      f.field("recurse_directories", x.recurse_directories),
      f.field("follow_directory_symlink", x.follow_directory_symlink),
      f.field("skip_permission_denied", x.skip_permission_denied));
  }
};

struct FilesArgs {
  Option<std::string> path = {};
  bool recurse_directories = {};
  bool follow_directory_symlink = {};
  bool skip_permission_denied = {};
};

struct FileListingResult {
  std::vector<table_slice> slices = {};
  std::vector<diagnostic> diagnostics = {};
};

auto to_legacy_args(const FilesArgs& args) -> files_args {
  return {
    .path = args.path ? std::optional<std::string>{*args.path} : std::nullopt,
    .recurse_directories = args.recurse_directories,
    .follow_directory_symlink = args.follow_directory_symlink,
    .skip_permission_denied = args.skip_permission_denied,
  };
}

auto is_permission_error(std::error_code ec) -> bool {
  return ec == std::errc::permission_denied
         or ec == std::errc::operation_not_permitted;
}

auto directory_options(files_args const& args)
  -> std::filesystem::directory_options {
  auto result = std::filesystem::directory_options::none;
  if (args.follow_directory_symlink) {
    result |= std::filesystem::directory_options::follow_directory_symlink;
  }
  return result;
}

auto emit_filesystem_error(const std::filesystem::path& path,
                           std::error_code ec, diagnostic_handler& dh) -> void {
  diagnostic::error(
    "{}",
    std::filesystem::filesystem_error{"failed to list directory", path, ec}
      .what())
    .emit(dh);
}

auto emit_skipped_directory_warning(const std::filesystem::path& path,
                                    std::error_code ec, diagnostic_handler& dh)
  -> void {
  diagnostic::warning("skipping unreadable directory `{}`: {}", path,
                      ec.message())
    .emit(dh);
}

auto should_recurse_into(const std::filesystem::directory_entry& entry,
                         std::filesystem::directory_options options,
                         bool skip_permission_denied, diagnostic_handler& dh)
  -> bool {
  auto ec = std::error_code{};
  const auto follow_symlinks
    = (options & std::filesystem::directory_options::follow_directory_symlink)
      != std::filesystem::directory_options::none;
  const auto status
    = follow_symlinks ? entry.status(ec) : entry.symlink_status(ec);
  if (ec) {
    if (skip_permission_denied and is_permission_error(ec)) {
      return false;
    }
    diagnostic::warning("failed to inspect `{}`: {}", entry.path(),
                        ec.message())
      .emit(dh);
    return false;
  }
  return status.type() == std::filesystem::file_type::directory;
}

auto list_directory(const std::filesystem::path& path,
                    std::filesystem::directory_options options,
                    bool skip_permission_denied, diagnostic_handler& dh)
  -> generator<std::filesystem::directory_entry> {
  auto ec = std::error_code{};
  auto iterator = std::filesystem::directory_iterator{path, options, ec};
  if (ec) {
    if (skip_permission_denied and is_permission_error(ec)) {
      co_return;
    }
    emit_filesystem_error(path, ec, dh);
    co_return;
  }
  const auto end = std::filesystem::directory_iterator{};
  while (iterator != end) {
    co_yield *iterator;
    ec.clear();
    iterator.increment(ec);
    if (ec) {
      if (skip_permission_denied and is_permission_error(ec)) {
        co_return;
      }
      emit_filesystem_error(path, ec, dh);
      co_return;
    }
  }
}

auto list_directory_recursive(const std::filesystem::path& path,
                              std::filesystem::directory_options options,
                              bool skip_permission_denied,
                              diagnostic_handler& dh)
  -> generator<std::filesystem::directory_entry> {
  auto ec = std::error_code{};
  auto stack = std::vector<std::filesystem::directory_iterator>{};
  auto root = std::filesystem::directory_iterator{path, options, ec};
  if (ec) {
    if (skip_permission_denied and is_permission_error(ec)) {
      co_return;
    }
    emit_filesystem_error(path, ec, dh);
    co_return;
  }
  stack.push_back(std::move(root));
  const auto end = std::filesystem::directory_iterator{};
  while (not stack.empty()) {
    auto& current = stack.back();
    if (current == end) {
      stack.pop_back();
      continue;
    }
    auto entry = *current;
    ec.clear();
    current.increment(ec);
    if (ec) {
      if (skip_permission_denied and is_permission_error(ec)) {
        stack.pop_back();
        continue;
      }
      emit_filesystem_error(entry.path(), ec, dh);
      co_return;
    }
    co_yield entry;
    if (not should_recurse_into(entry, options, skip_permission_denied, dh)) {
      continue;
    }
    ec.clear();
    auto child = std::filesystem::directory_iterator{entry.path(), options, ec};
    if (ec) {
      if (is_permission_error(ec)) {
        if (not skip_permission_denied) {
          emit_skipped_directory_warning(entry.path(), ec, dh);
        }
        continue;
      }
      emit_filesystem_error(entry.path(), ec, dh);
      co_return;
    }
    stack.push_back(std::move(child));
  }
}

auto make_file_events(auto listing) -> generator<table_slice> {
  auto identity = [](auto x) {
    return x;
  };
  auto get_time = [](auto x) {
    return std::chrono::time_point_cast<duration>(
      std::chrono::file_clock::to_sys(x));
  };
  static constexpr auto max_length
    = detail::narrow_cast<int64_t>(defaults::import::table_slice_size);
  const auto file_permissions_type = type{
    "tenzir.file_permissions",
    record_type{
      {"read", bool_type{}},
      {"write", bool_type{}},
      {"execute", bool_type{}},
    },
  };
  const auto schema = type{
    "tenzir.file",
    record_type{
      {"path", string_type{}},
      {"type", string_type{}},
      {"permissions",
       record_type{
         {"owner", file_permissions_type},
         {"group", file_permissions_type},
         {"others", file_permissions_type},
       }},
      {"owner", string_type{}},
      {"group", string_type{}},
      {"file_size", uint64_type{}},
      {"hard_link_count", uint64_type{}},
      {"last_write_time", time_type{}},
    },
  };
  auto builder = series_builder{schema};
  for (const auto& entry : std::move(listing)) {
    auto status_ec = std::error_code{};
    const auto status = entry.status(status_ec);
    auto event = builder.record();
    event.field("path", entry.path().string());
    event.field("type", [&]() -> data_view2 {
      if (status_ec) {
        return {};
      }
      using std::filesystem::file_type;
      switch (status.type()) {
        case file_type::regular:
          return "regular";
        case file_type::directory:
          return "directory";
        case file_type::symlink:
          return "symlink";
        case file_type::block:
          return "block";
        case file_type::character:
          return "character";
        case file_type::fifo:
          return "fifo";
        case file_type::socket:
          return "socket";
        case file_type::unknown:
          return "unknown";
        case file_type::not_found:
          return "not_found";
        default:
          return {};
      }
    }());
    {
      using std::filesystem::perms;
      auto permissions = event.field("permissions").record();
      auto has_perm = [perms = status.permissions()](auto perm) {
        return perms::none != (perms & perm);
      };
#define X(name)                                                                \
  do {                                                                         \
    auto name = permissions.field(#name).record();                             \
    name.field("read", has_perm(perms::name##_read));                          \
    name.field("write", has_perm(perms::name##_write));                        \
    name.field("execute", has_perm(perms::name##_exec));                       \
  } while (0)
      X(owner);
      X(group);
      X(others);
#undef X
    }
    {
      struct ::stat stat_buf = {};
      const auto stat_result = ::stat(entry.path().c_str(), &stat_buf);
      if (stat_result == 0) {
        if (const auto* pwuid = getpwuid(stat_buf.st_uid)) {
          event.field("owner", pwuid->pw_name);
        }
        if (const auto* grgid = getgrgid(stat_buf.st_gid)) {
          event.field("group", grgid->gr_name);
        }
      }
    }
#define X(builder, name, transformation)                                       \
  do {                                                                         \
    auto ec = std::error_code{};                                               \
    const auto name = entry.name(ec);                                          \
    if (not ec) {                                                              \
      builder.field(#name, transformation(name));                              \
    }                                                                          \
  } while (0)
    X(event, file_size, identity);
    X(event, hard_link_count, identity);
    X(event, last_write_time, get_time);
#undef X
    if (builder.length() >= max_length) {
      co_yield builder.finish_assert_one_slice();
    }
  }
  co_yield builder.finish_assert_one_slice();
}

auto make_file_listing(files_args args) -> FileListingResult {
  auto result = FileListingResult{};
  auto dh = collecting_diagnostic_handler{};
  try {
    auto ec = std::error_code{};
    const auto path = args.path ? std::filesystem::path{*args.path}
                                : std::filesystem::current_path(ec);
    if (ec) {
      diagnostic::error("{}",
                        std::filesystem::filesystem_error{
                          "failed to determine current directory", ec}
                          .what())
        .emit(dh);
      result.diagnostics = std::move(dh).collect();
      return result;
    }
    const auto options = directory_options(args);
    auto gen = args.recurse_directories
                 ? make_file_events(list_directory_recursive(
                     path, options, args.skip_permission_denied, dh))
                 : make_file_events(list_directory(
                     path, options, args.skip_permission_denied, dh));
    for (auto&& slice : std::move(gen)) {
      if (slice.rows() > 0) {
        result.slices.push_back(std::move(slice));
      }
    }
  } catch (const std::filesystem::filesystem_error& err) {
    diagnostic::error("{}", err.what()).emit(dh);
  }
  result.diagnostics = std::move(dh).collect();
  return result;
}

class files_operator final : public crtp_operator<files_operator> {
public:
  files_operator() = default;

  files_operator(files_args args) : args_{std::move(args)} {
  }

  auto make_generator(auto listing) const -> generator<table_slice> {
    for (auto&& slice : make_file_events(std::move(listing))) {
      co_yield std::move(slice);
    }
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    co_yield {};
    try {
      const auto path = args_.path ? std::filesystem::path{*args_.path}
                                   : std::filesystem::current_path();
      const auto options = directory_options(args_);
      if (args_.recurse_directories) {
        auto gen = make_generator(list_directory_recursive(
          path, options, args_.skip_permission_denied, ctrl.diagnostics()));
        for (auto&& result : std::move(gen)) {
          co_yield std::move(result);
        }
      } else {
        auto gen = make_generator(list_directory(
          path, options, args_.skip_permission_denied, ctrl.diagnostics()));
        for (auto&& result : std::move(gen)) {
          co_yield std::move(result);
        }
      }
    } catch (const std::filesystem::filesystem_error& err) {
      diagnostic::error("{}", err.what()).emit(ctrl.diagnostics());
    }
  }

  auto name() const -> std::string override {
    return "files";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, files_operator& x) -> bool {
    return f.object(x).fields(f.field("args", x.args_));
  }

private:
  files_args args_ = {};
};

class Files final : public Operator<void, table_slice> {
public:
  explicit Files(FilesArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx&) -> Task<void> override {
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    if (done_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    auto args = to_legacy_args(args_);
    co_return co_await spawn_blocking([args = std::move(args)]() mutable {
      return make_file_listing(std::move(args));
    });
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto listing = std::move(result).as<FileListingResult>();
    for (auto& slice : listing.slices) {
      co_await push(std::move(slice));
    }
    for (auto& diag : listing.diagnostics) {
      ctx.dh().emit(std::move(diag));
    }
    done_ = true;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("done", done_);
  }

private:
  FilesArgs args_ = {};
  bool done_ = false;
};

class plugin final : public virtual operator_plugin<files_operator>,
                     public virtual operator_factory_plugin,
                     public virtual OperatorPlugin {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = files_args{};
    TRY(argument_parser2::operator_("files")
          .positional("dir", args.path)
          .named("recurse", args.recurse_directories)
          .named("follow_symlinks", args.follow_directory_symlink)
          .named("skip_permission_denied", args.skip_permission_denied)
          .parse(inv, ctx));
    return std::make_unique<files_operator>(std::move(args));
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"files", "https://docs.tenzir.com/"
                                           "operators/files"};
    auto args = files_args{};
    parser.add(args.path, "<path>");
    parser.add("-r,--recurse-directories", args.recurse_directories);
    parser.add("--follow-directory-symlink", args.follow_directory_symlink);
    parser.add("--skip-permission-denied", args.skip_permission_denied);
    parser.parse(p);
    return std::make_unique<files_operator>(std::move(args));
  }

  auto describe() const -> Description override {
    auto d = Describer<FilesArgs, Files>{"https://docs.tenzir.com/operators/"
                                         "files"};
    d.positional("dir", &FilesArgs::path);
    d.named("recurse", &FilesArgs::recurse_directories);
    d.named("follow_symlinks", &FilesArgs::follow_directory_symlink);
    d.named("skip_permission_denied", &FilesArgs::skip_permission_denied);
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::files

TENZIR_REGISTER_PLUGIN(tenzir::plugins::files::plugin)
