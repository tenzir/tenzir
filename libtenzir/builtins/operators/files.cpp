//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_caf.hpp>
#include <tenzir/glob.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/filesystem/api.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/util/future.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <filesystem>
#include <grp.h>
#include <pwd.h>
#include <unistd.h>

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

// template <class T>
// void add_actor_callback(auto* self, arrow::Future<T> fut, auto&& fn) {
//   using result_type
//     = std::conditional_t<std::same_as<T, arrow::internal::Empty>,
//     arrow::Status,
//                          arrow::Result<T>>;
//   std::move(fut).AddCallback([self, fn = std::forward<decltype(fn)>(fn)](
//                                const result_type& result) mutable {
//     self->delay_fn([fn = std::move(fn), result]() mutable -> void {
//       return std::move(fn)(std::move(result));
//     });
//   });
// }

template <class F>
void async_iter(arrow::fs::FileInfoGenerator gen, F&& f) {
  auto next = gen();
  next.AddCallback([gen = std::move(gen), f = std::forward<F>(f)](
                     arrow::Result<arrow::fs::FileInfoVector> infos_result) {
    // TODO: Don't check.
    auto infos = check(infos_result);
    auto done = infos.empty();
    f(std::move(infos));
    if (done) {
      return;
    }
    async_iter(std::move(gen), std::move(f));
  });
}

class files_operator final : public crtp_operator<files_operator> {
public:
  files_operator() = default;

  files_operator(files_args args) : args_{std::move(args)} {
  }

  auto make_generator(auto listing) const -> generator<table_slice> {
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
      auto event = builder.record();
      event.field("path", entry.path().string());
      event.field("type", [&]() -> data_view2 {
        using std::filesystem::file_type;
        switch (entry.status().type()) {
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
        auto has_perm = [perms = entry.status().permissions()](auto perm) {
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

  auto
  operator()(operator_control_plane& ctrl) const -> generator<table_slice> {
#if 1
    auto executor = caf_executor{&ctrl.self()};
    auto io_ctx = arrow::io::IOContext{arrow::default_memory_pool(), &executor};
    TENZIR_ASSERT(args_.path);
    auto path = std::string{};
    // TODO: Relative local-filesystem paths.
    // TODO: Arrow removes trailing slashes here.
    auto fs = arrow::fs::FileSystemFromUriOrPath(*args_.path, io_ctx, &path);
    if (not fs.ok()) {
      diagnostic::error("{}", fs.status().ToStringWithoutContextLines())
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto glob = parse_glob(path);
    // TODO: Figure out the proper logic here.
    if (auto star = path.find('*'); star != std::string::npos) {
      auto slash = path.rfind('/', star);
      TENZIR_ASSERT(slash != std::string::npos);
      path = path.substr(0, slash + 1);
    }
    auto sel = arrow::fs::FileSelector{};
    sel.base_dir = path;
    sel.recursive = true;
    // TODO: Schema.
    auto b = series_builder{};
    // We intentionally define the lambda in the scope of the generator to make
    // sure that we do not capture anything that doesn't survive.
    auto process = [&](arrow::fs::FileInfoVector infos) {
      if (infos.empty()) {
        ctrl.set_waiting(false);
        return;
      }
      for (auto& info : infos) {
        if (not matches(info.path(), glob)) {
          continue;
        }
        auto r = b.record();
        r.field("path", info.path());
        r.field("type", std::invoke([&] -> data_view2 {
                  switch (info.type()) {
                    case arrow::fs::FileType::NotFound:
                      // TODO: This should not happen, right?
                      TENZIR_UNREACHABLE();
                    case arrow::fs::FileType::Unknown:
                      return caf::none;
                    case arrow::fs::FileType::File:
                      return "file";
                    case arrow::fs::FileType::Directory:
                      return "directory";
                  }
                  TENZIR_UNREACHABLE();
                }));
        r.field("size", info.size() == arrow::fs::kNoSize
                          ? data_view2{caf::none}
                          : info.size());
        r.field("last_modified", info.mtime() == arrow::fs::kNoTime
                                   ? data_view2{caf::none}
                                   : info.mtime());
      }
    };
    ctrl.set_waiting(true);
    (*fs)
      ->GetFileInfoAsync(std::vector{path})
      .AddCallback([&](arrow::Result<std::vector<arrow::fs::FileInfo>> infos) {
        // TODO: Improve diagnostics.
        if (not infos.ok()) {
          diagnostic::error("{}", infos.status().ToStringWithoutContextLines())
            .emit(ctrl.diagnostics());
          return;
        }
        TENZIR_ASSERT(infos->size() == 1);
        auto root_info = std::move((*infos)[0]);
        switch (root_info.type()) {
          case arrow::fs::FileType::NotFound:
            diagnostic::error("`{}` does not exist", *args_.path)
              .emit(ctrl.diagnostics());
            return;
          case arrow::fs::FileType::Unknown:
            diagnostic::error("`{}` is unknown", *args_.path)
              .emit(ctrl.diagnostics());
            return;
          case arrow::fs::FileType::File:
            // TODO: What do we do?
            diagnostic::error("`{}` is file", *args_.path)
              .emit(ctrl.diagnostics());
            return;
          case arrow::fs::FileType::Directory:
            auto gen = (*fs)->GetFileInfoGenerator(sel);
            async_iter(std::move(gen), std::move(process));
            return;
        }
        TENZIR_UNREACHABLE();
      });
    co_yield {};
    for (auto slice : b.finish_as_table_slice("tenzir.file")) {
      co_yield std::move(slice);
    }
#else
    try {
      const auto path = args_.path ? std::filesystem::path{*args_.path}
                                   : std::filesystem::current_path();
      const auto options = [&] {
        auto result = std::filesystem::directory_options::none;
        if (args_.follow_directory_symlink) {
          result
            |= std::filesystem::directory_options::follow_directory_symlink;
        }
        if (args_.skip_permission_denied) {
          result |= std::filesystem::directory_options::skip_permission_denied;
        }
        return result;
      }();
      auto builder = series_builder{};
      if (args_.recurse_directories) {
        auto gen = make_generator(
          std::filesystem::recursive_directory_iterator{path, options});
        for (auto&& result : std::move(gen)) {
          co_yield std::move(result);
        }
      } else {
        auto gen
          = make_generator(std::filesystem::directory_iterator{path, options});
        for (auto&& result : std::move(gen)) {
          co_yield std::move(result);
        }
      }
    } catch (const std::filesystem::filesystem_error& err) {
      diagnostic::error("{}", err.what()).emit(ctrl.diagnostics());
    }
#endif
  }

  auto name() const -> std::string override {
    return "files";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto
  optimize(expression const&, event_order) const -> optimize_result override {
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, files_operator& x) -> bool {
    return f.object(x).fields(f.field("args", x.args_));
  }

private:
  files_args args_ = {};
};

class plugin final : public virtual operator_plugin<files_operator>,
                     public virtual operator_factory_plugin {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
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
};

} // namespace

} // namespace tenzir::plugins::files

TENZIR_REGISTER_PLUGIN(tenzir::plugins::files::plugin)
