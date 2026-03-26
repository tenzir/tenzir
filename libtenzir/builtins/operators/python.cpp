//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/as_bytes.hpp>
#include <tenzir/async.hpp>
#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/async/subprocess.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/concept/parseable/string/quoted_string.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/env.hpp>
#include <tenzir/detail/installdirs.hpp>
#include <tenzir/detail/overload.hpp>
#include <tenzir/detail/posix.hpp>
#include <tenzir/detail/preserved_fds.hpp>
#include <tenzir/detail/scope_guard.hpp>
#include <tenzir/detail/strip_leading_indentation.hpp>
#include <tenzir/error.hpp>
#include <tenzir/generator.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/secret_resolution_utilities.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/type.hpp>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <boost/algorithm/string.hpp>
#include <boost/asio.hpp>
#include <boost/interprocess/sync/named_semaphore.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/settings.hpp>

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <iterator>
#include <string_view>
#include <system_error>
#include <unistd.h>

#if __has_include(<boost/process/v1/child.hpp>)

#  include <boost/process/v1/args.hpp>
#  include <boost/process/v1/async.hpp>
#  include <boost/process/v1/async_system.hpp>
#  include <boost/process/v1/child.hpp>
#  include <boost/process/v1/cmd.hpp>
#  include <boost/process/v1/env.hpp>
#  include <boost/process/v1/environment.hpp>
#  include <boost/process/v1/error.hpp>
#  include <boost/process/v1/exe.hpp>
#  include <boost/process/v1/group.hpp>
#  include <boost/process/v1/handles.hpp>
#  include <boost/process/v1/io.hpp>
#  include <boost/process/v1/pipe.hpp>
#  include <boost/process/v1/search_path.hpp>
#  include <boost/process/v1/shell.hpp>
#  include <boost/process/v1/spawn.hpp>
#  include <boost/process/v1/start_dir.hpp>
#  include <boost/process/v1/system.hpp>

namespace bp = boost::process::v1;

#else

#  include <boost/process.hpp>

namespace bp = boost::process;

#endif

namespace tenzir::plugins::python {
namespace {

auto to_strings = [](auto&& range) {
  std::vector<std::string> out;
  out.reserve(std::ranges::size(range));
  for (auto&& item : range) {
    out.emplace_back(std::string{item});
  }
  return out;
};

/// Arrow InputStream API implementation over a file descriptor.
class arrow_fd_wrapper : public ::arrow::io::InputStream {
public:
  explicit arrow_fd_wrapper(int fd) : fd_{fd} {
  }

  auto Close() -> ::arrow::Status override {
    fd_ = -1;
    return ::arrow::Status::OK();
  }

  auto closed() const -> bool override {
    return fd_ == -1;
  }

  auto Tell() const -> ::arrow::Result<int64_t> override {
    return pos_;
  }

  auto Read(int64_t nbytes, void* out) -> ::arrow::Result<int64_t> override {
    auto bytes_read = detail::read(fd_, out, nbytes);
    if (not bytes_read) {
      return ::arrow::Status::IOError(fmt::to_string(bytes_read.error()));
    }
    auto sbytes = detail::narrow_cast<int64_t>(*bytes_read);
    pos_ += sbytes;
    return sbytes;
  }

  auto Read(int64_t nbytes)
    -> ::arrow::Result<std::shared_ptr<::arrow::Buffer>> override {
    ARROW_ASSIGN_OR_RAISE(auto buffer,
                          ::arrow::AllocateResizableBuffer(nbytes));
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read,
                          Read(nbytes, buffer->mutable_data()));
    ARROW_RETURN_NOT_OK(buffer->Resize(bytes_read, false));
    buffer->ZeroPadding();
    return buffer;
  }

private:
  int fd_ = -1;
  int64_t pos_ = 0;
};

constexpr auto PYTHON_SCAFFOLD = R"_(
from tenzir_operator.executor import main

main()
)_";

struct config {
  // Implicit arguments passed to every invocation of `pip install`.
  std::optional<std::string> implicit_requirements;

  // Whether to create a virtualenv environment for the python
  // operator.
  bool create_venvs = true;

  template <class Inspector>
  friend auto inspect(Inspector& f, config& x) -> bool {
    auto result
      = f.object(x)
          .pretty_name("tenzir.plugins.python.config")
          .fields(f.field("implicit-requirements", x.implicit_requirements),
                  f.field("create-venvs", x.create_venvs));
    return result;
  }
};

constexpr auto code_pipe_child_fd = 3;
constexpr auto error_pipe_child_fd = 4;

struct python_runtime {
  std::filesystem::path python_executable;
  bp::environment env = boost::this_process::environment();
  std::optional<std::filesystem::path> venv;
  std::optional<std::string> virtual_env;
  std::optional<std::string> uv_cache_dir;
  std::optional<std::string> uv_python;
};

struct PythonArgs {
  located<secret> code = located{secret{}, location::unknown};
  Option<located<std::string>> file;
  Option<located<std::string>> requirements;
};

auto process_path_env() -> std::vector<bp::filesystem::path> {
  auto result = std::vector<bp::filesystem::path>{};
  auto raw_path = detail::getenv("PATH");
  if (not raw_path) {
    return result;
  }
  auto path = std::string_view{*raw_path};
  while (true) {
    auto separator = path.find(':');
    auto entry
      = separator == std::string_view::npos ? path : path.substr(0, separator);
    if (not entry.empty()) {
      result.emplace_back(std::string{entry});
    }
    if (separator == std::string_view::npos) {
      break;
    }
    path.remove_prefix(separator + 1);
  }
  return result;
}

using code_or_path_t = located<std::variant<std::filesystem::path, secret>>;

auto drain_pipe(bp::ipstream& pipe) -> std::string {
  auto result = std::string{};
  if (pipe.peek() == bp::ipstream::traits_type::eof()) {
    return result;
  }
  auto line = std::string{};
  while (std::getline(pipe, line)) {
    if (not result.empty()) {
      result += '\n';
    }
    result += line;
  }
  boost::trim(result);
  return result;
}

auto find_wheel(const std::filesystem::path& directory,
                std::string_view project)
  -> std::optional<std::filesystem::path> {
  auto normalized = std::string{project};
  std::replace(normalized.begin(), normalized.end(), '-', '_');
  const auto prefix = fmt::format("{}-", normalized);
  const auto suffix = std::string{".whl"};
  auto best_path = std::optional<std::filesystem::path>{};
  auto best_name = std::string{};
  std::error_code ec;
  if (not std::filesystem::exists(directory, ec)) {
    return std::nullopt;
  }
  for (const auto& entry : std::filesystem::directory_iterator{directory, ec}) {
    if (ec) {
      break;
    }
    if (not entry.is_regular_file()) {
      continue;
    }
    const auto filename = entry.path().filename().string();
    if (not boost::algorithm::starts_with(filename, prefix)
        || not boost::algorithm::ends_with(filename, suffix)) {
      continue;
    }
    if (not best_path || filename > best_name) {
      best_name = filename;
      best_path = entry.path();
    }
  }
  return best_path;
}

auto make_subprocess_env(const python_runtime& runtime)
  -> std::vector<std::string> {
  auto result = std::vector<std::string>{};
  for (auto [key, value] : detail::environment()) {
    auto entry = fmt::format("{}={}", key, value);
    if (runtime.virtual_env and key == "VIRTUAL_ENV") {
      entry = fmt::format("VIRTUAL_ENV={}", *runtime.virtual_env);
    } else if (runtime.uv_cache_dir and key == "UV_CACHE_DIR") {
      entry = fmt::format("UV_CACHE_DIR={}", *runtime.uv_cache_dir);
    } else if (runtime.uv_python and key == "UV_PYTHON") {
      entry = fmt::format("UV_PYTHON={}", *runtime.uv_python);
    }
    result.push_back(std::move(entry));
  }
  auto append_missing
    = [&](std::string_view key, const std::optional<std::string>& value) {
        if (not value) {
          return;
        }
        auto prefix = fmt::format("{}=", key);
        auto exists = std::ranges::any_of(result, [&](const auto& entry) {
          return entry.starts_with(prefix);
        });
        if (not exists) {
          result.push_back(fmt::format("{}={}", key, *value));
        }
      };
  append_missing("VIRTUAL_ENV", runtime.virtual_env);
  append_missing("UV_CACHE_DIR", runtime.uv_cache_dir);
  append_missing("UV_PYTHON", runtime.uv_python);
  return result;
}

auto prepare_runtime(const config& config, std::string_view requirements,
                     diagnostic_handler& dh, const caf::settings& system_config)
  -> failure_or<python_runtime> {
  auto implicit_requirements = std::string{};
  auto bundled_wheels = std::vector<std::string>{};
  const auto python_dir = detail::install_datadir() / "python";
  if (config.implicit_requirements) {
    implicit_requirements = *config.implicit_requirements;
  } else if (find_wheel(python_dir, "tenzir-operator")) {
    std::error_code ec;
    for (const auto& entry :
         std::filesystem::directory_iterator{python_dir, ec}) {
      if (ec) {
        break;
      }
      if (not entry.is_regular_file()) {
        continue;
      }
      if (entry.path().extension() != ".whl") {
        continue;
      }
      bundled_wheels.push_back(entry.path().string());
    }
  } else {
    implicit_requirements = "tenzir-operator";
  }
  auto runtime = python_runtime{};
  auto process_path = process_path_env();
  runtime.python_executable
    = std::filesystem::path{bp::search_path("python3", process_path).string()};
  if (runtime.python_executable.empty()) {
    diagnostic::error("Failed to find python3").emit(dh);
    return failure::promise();
  }
  if (not config.create_venvs) {
    return runtime;
  }
  auto venv_base_dir = std::filesystem::path{};
  if (const auto* cache_dir
      = get_if<std::string>(&system_config, "tenzir.cache-directory")) {
    venv_base_dir = std::filesystem::path{*cache_dir} / "python" / "venvs";
  } else {
    venv_base_dir
      = std::filesystem::temp_directory_path() / "tenzir" / "python" / "venvs";
  }
  auto ec = std::error_code{};
  std::filesystem::create_directories(venv_base_dir, ec);
  auto venv = fmt::format("{}/uvenv-XXXXXX", venv_base_dir.string());
  if (mkdtemp(venv.data()) == nullptr) {
    diagnostic::error("{}", detail::describe_errno())
      .note("failed to create a unique directory for the python virtual "
            "environment in {}",
            venv_base_dir)
      .emit(dh);
    return failure::promise();
  }
  runtime.venv = std::filesystem::path{venv};
  auto cleanup_venv
    = detail::scope_guard{[venv_path = *runtime.venv]() noexcept {
        auto ignored = std::error_code{};
        std::filesystem::remove_all(venv_path, ignored);
      }};
  runtime.env["VIRTUAL_ENV"] = venv;
  runtime.virtual_env = venv;
  runtime.uv_cache_dir
    = (venv_base_dir.parent_path() / "cache" / "uv").string();
  runtime.env["UV_CACHE_DIR"] = *runtime.uv_cache_dir;
#if TENZIR_ENABLE_BUNDLED_UV
  auto uv_executable = detail::install_libexecdir() / "uv";
#else
  auto uv_executable = bp::search_path("uv");
#endif
  if (uv_executable.empty()) {
    diagnostic::error("Failed to find uv").emit(dh);
    return failure::promise();
  }
  auto venv_invocation = std::vector<std::string>{
    uv_executable.string(),
    "venv",
    runtime.venv->string(),
  };
  TENZIR_VERBOSE("creating a python venv with: '{}'",
                 fmt::join(venv_invocation, "' '"));
  auto venv_err = bp::ipstream{};
  if (bp::system(venv_invocation, runtime.env, bp::std_err > venv_err,
                 detail::preserved_fds{{STDERR_FILENO}},
                 bp::detail::limit_handles_{})
      != 0) {
    diagnostic::error("{}", drain_pipe(venv_err))
      .note("failed to create virtualenv")
      .emit(dh);
    return failure::promise();
  }
  const auto venv_python = *runtime.venv / "bin" / "python3";
  runtime.uv_python = venv_python.string();
  runtime.env["UV_PYTHON"] = *runtime.uv_python;
  auto run_install
    = [&](auto args, std::string_view error_note) -> failure_or<void> {
    using std::begin;
    using std::end;
    auto first = begin(args);
    auto last = end(args);
    if (first == last) {
      return {};
    }
    auto invocation = std::vector<std::string>{
      uv_executable.string(), "pip", "install", "--python",
      venv_python.string(),
    };
    invocation.insert(invocation.end(), std::make_move_iterator(first),
                      std::make_move_iterator(last));
    auto install_err = bp::ipstream{};
    TENZIR_VERBOSE("installing python modules with: '{}'",
                   fmt::join(invocation, "' '"));
    if (bp::system(invocation, runtime.env, bp::std_err > install_err,
                   detail::preserved_fds{{STDOUT_FILENO, STDERR_FILENO}},
                   bp::detail::limit_handles_{})
        != 0) {
      diagnostic::error("{}", drain_pipe(install_err))
        .note("{}", error_note)
        .emit(dh);
      return failure::promise();
    }
    return {};
  };
  if (not implicit_requirements.empty()) {
    auto implicit_vec = detail::split_escaped(implicit_requirements, " ", "\\");
    TRY(run_install(std::move(implicit_vec),
                    "failed to install implicit requirements"));
  } else if (not bundled_wheels.empty()) {
    TRY(run_install(std::move(bundled_wheels),
                    "failed to install bundled Python wheels"));
  }
  if (not requirements.empty()) {
    auto requirements_vec = detail::split(requirements, " ");
    TRY(run_install(to_strings(std::move(requirements_vec)),
                    "failed to install additional requirements"));
  }
  runtime.python_executable = venv_python;
  cleanup_venv.disable();
  return runtime;
}

class python_operator final : public crtp_operator<python_operator> {
public:
  python_operator() = default;

  explicit python_operator(struct config config, std::string requirements,
                           code_or_path_t code_or_path)
    : config_{std::move(config)},
      requirements_{std::move(requirements)},
      code_{std::move(code_or_path)} {
  }

  auto execute(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    try {
      // Compute some config values delayed at runtime, because
      // `detail::install_datadir` and the venv base dir may be different
      // between the client and node process.
      auto implicit_requirements = std::string{};
      auto bundled_wheels = std::vector<std::string>{};
      const auto python_dir = detail::install_datadir() / "python";
      if (config_.implicit_requirements) {
        implicit_requirements = *config_.implicit_requirements;
      } else {
        const auto find_wheel = [&](const std::filesystem::path& directory,
                                    std::string_view project)
          -> std::optional<std::filesystem::path> {
          auto normalized = std::string{project};
          std::replace(normalized.begin(), normalized.end(), '-', '_');
          const auto prefix = fmt::format("{}-", normalized);
          const auto suffix = std::string{".whl"};
          auto best_path = std::optional<std::filesystem::path>{};
          auto best_name = std::string{};
          std::error_code ec;
          if (not std::filesystem::exists(directory, ec)) {
            return std::nullopt;
          }
          for (const auto& entry :
               std::filesystem::directory_iterator{directory}) {
            if (not entry.is_regular_file()) {
              continue;
            }
            const auto filename = entry.path().filename().string();
            if (not boost::algorithm::starts_with(filename, prefix)
                || ! boost::algorithm::ends_with(filename, suffix)) {
              continue;
            }
            if (not best_path || filename > best_name) {
              best_name = filename;
              best_path = entry.path();
            }
          }
          return best_path;
        };
        if (find_wheel(python_dir, "tenzir-operator")) {
          std::error_code ec;
          for (const auto& entry :
               std::filesystem::directory_iterator{python_dir, ec}) {
            if (not entry.is_regular_file()) {
              continue;
            }
            if (entry.path().extension() != ".whl") {
              continue;
            }
            bundled_wheels.push_back(entry.path().string());
          }
        } else {
          implicit_requirements = std::string{"tenzir-operator"};
        }
      }
      auto venv_base_dir = std::optional<std::filesystem::path>{};
      if (not config_.create_venvs) {
        venv_base_dir = std::nullopt;
      } else if (const auto* cache_dir
                 = get_if<std::string>(&ctrl.self().home_system().config(),
                                       "tenzir.cache-directory")) {
        venv_base_dir = std::filesystem::path{*cache_dir} / "python" / "venvs";
      } else {
        venv_base_dir = std::filesystem::temp_directory_path() / "tenzir"
                        / "python" / "venvs";
      }
      // Creating a pipeline through the API waits until a pipeline has started
      // up succesfully, which requires all individual execution nodes to have
      // started up immediately. This happens once the operator yielded for the
      // first time. We yield here immediately as creating the virtual
      // environment can take a fair amount of time, which empirically led to
      // 504 errors on app.tenzir.com, especially when viewing the dashboard
      // when many charts were using the Python operator.
      co_yield {};
      // Get the code to be executed.
      auto code = std::string{};
      if (const auto* path = try_as<std::filesystem::path>(code_.inner)) {
        auto code_chunk = chunk::make_empty();
        if (auto err = read(*path, code_chunk); err.valid()) {
          diagnostic::error(err)
            .note("failed to read code from file")
            .emit(ctrl.diagnostics());
        }
        code = std::string{reinterpret_cast<const char*>(code_chunk->data()),
                           code_chunk->size()};
      } else if (const auto* secret = try_as<class secret>(code_.inner)) {
        co_yield ctrl.resolve_secrets_must_yield({make_secret_request(
          "code", *secret, code_.source, code, ctrl.diagnostics())});
      } else {
        TENZIR_UNREACHABLE();
      }
      code = detail::strip_leading_indentation(std::move(code));
      // Setup python prerequisites.
      bp::pipe std_out;
      bp::pipe std_in;
      bp::ipstream std_err;
      auto process_path = process_path_env();
      auto python_executable = bp::search_path("python3", process_path);
      auto env = bp::environment{boost::this_process::environment()};
      // Automatically create a virtualenv with all requirements preinstalled,
      // unless disabled by node config.
      auto maybe_venv = std::optional<std::filesystem::path>{};
      if (config_.create_venvs) {
        TENZIR_ASSERT(venv_base_dir);
        auto ec = std::error_code{};
        std::filesystem::create_directories(*venv_base_dir, ec);
        auto venv = fmt::format("{}/uvenv-XXXXXX", venv_base_dir->string());
        if (mkdtemp(venv.data()) == nullptr) {
          diagnostic::error("{}", detail::describe_errno())
            .note("failed to create a unique directory for the python "
                  "virtual environment in {}",
                  *venv_base_dir)
            .throw_();
        }
        auto venv_path = std::filesystem::path{venv};
        maybe_venv = venv_path;
        env["VIRTUAL_ENV"] = venv;
        env["UV_CACHE_DIR"]
          = (venv_base_dir->parent_path() / "cache" / "uv").string();
      }
      auto venv_cleanup = [&] {
        return detail::scope_guard([maybe_venv]() noexcept {
          if (maybe_venv) {
            std::error_code ec;
            auto exists = std::filesystem::exists(*maybe_venv, ec);
            if (ec) {
              // ctrl can already be gone, so we can't emit a diagnostic here.
              TENZIR_WARN("python operator failed to check for venv at {}: {}",
                          *maybe_venv, ec);
              return;
            }
            if (not exists) {
              return;
            }
            std::filesystem::remove_all(*maybe_venv, ec);
            if (ec) {
              // ctrl can already be gone, so we can't emit a diagnostic here.
              TENZIR_WARN("python operator failed to remove venv at {}: {}",
                          *maybe_venv, ec);
            }
          }
        });
      }();
      if (maybe_venv) {
#if TENZIR_ENABLE_BUNDLED_UV
        auto uv_executable = detail::install_libexecdir() / "uv";
#else
        auto uv_executable = bp::search_path("uv", process_path);
#endif
        if (uv_executable.empty()) {
          diagnostic::error("Failed to find uv").emit(ctrl.diagnostics());
          co_return;
        }
        auto venv_invocation = std::vector<std::string>{
          uv_executable.string(),
          "venv",
          maybe_venv->string(),
        };
        TENZIR_VERBOSE("creating a python venv with: '{}'",
                       fmt::join(venv_invocation, "' '"));
        if (bp::system(venv_invocation, env, bp::std_err > std_err,
                       detail::preserved_fds{{STDERR_FILENO}},
                       bp::detail::limit_handles_{})
            != 0) {
          auto venv_error = drain_pipe(std_err);
          // We need to delete the potentially broken venv here to make sure
          // that it doesn't stick around to break later runs of the python
          // operator.
          diagnostic::error("{}", venv_error)
            .note("failed to create virtualenv")
            .throw_();
        }
        const auto venv_python
          = std::filesystem::path{*maybe_venv} / "bin" / "python3";
        env["UV_PYTHON"] = venv_python.string();
        auto run_install = [&](auto args, std::string_view error_note) {
          using std::begin;
          using std::end;
          auto first = begin(args);
          auto last = end(args);
          if (first == last) {
            return;
          }
          auto invocation = std::vector<std::string>{
            uv_executable.string(), "pip", "install", "--python",
            venv_python.string(),
            // FIXME: Debug logging clogs the error pipe and deadlocks uv.
            // We should use bp::child instead of bp::system and read from the
            // pipe while the process is running.
            //"-vv",
          };
          invocation.insert(invocation.end(), std::make_move_iterator(first),
                            std::make_move_iterator(last));
          bp::ipstream install_err;
          TENZIR_VERBOSE("installing python modules with: '{}'",
                         fmt::join(invocation, "' '"));
          if (bp::system(invocation, env, bp::std_err > install_err,
                         detail::preserved_fds{{STDOUT_FILENO, STDERR_FILENO}},
                         bp::detail::limit_handles_{})
              != 0) {
            auto pip_error = drain_pipe(install_err);
            diagnostic::error("{}", pip_error).note("{}", error_note).throw_();
          }
        };
        if (not implicit_requirements.empty()) {
          auto implicit_vec
            = detail::split_escaped(implicit_requirements, " ", "\\");
          run_install(std::move(implicit_vec),
                      "failed to install implicit requirements");
        } else if (not bundled_wheels.empty()) {
          // Install the bundled wheels to expose the static `tenzir` executable
          // that ships inside the python `tenzir` project wheel.
          run_install(std::move(bundled_wheels),
                      "failed to install bundled Python wheels");
        }
        if (not requirements_.empty()) {
          auto requirements_vec = detail::split(requirements_, " ");
          run_install(to_strings(std::move(requirements_vec)),
                      "failed to install additional requirements");
        }
        python_executable = venv_python;
      }
      bp::opstream codepipe; // pipe to transmit the code
      // If we redirect stderr to get error information, we need to switch to a
      // select()-style read loop to ensure python (or a child process) doesn't
      // deadlock when trying to write to stderr. So we use a separate pipe
      // that's only used by the python executor and has well-defined semantics.
      bp::ipstream errpipe;
      // TODO: Put this into a finalizer so it can be cleaned up correctly in
      // case of errors or other early returns.
      auto child = bp::child{
        python_executable,
        "-c",
        PYTHON_SCAFFOLD,
        fmt::to_string(codepipe.pipe().native_source()),
        fmt::to_string(errpipe.pipe().native_sink()),
        env,
        bp::std_out > std_out,
        bp::std_in < std_in,
        detail::preserved_fds{{STDIN_FILENO, STDOUT_FILENO, STDERR_FILENO,
                               codepipe.pipe().native_source(),
                               errpipe.pipe().native_sink()}},
        bp::detail::limit_handles_{}};
      ::close(codepipe.pipe().native_source());
      codepipe.pipe().assign_source(-1);
      if (code.empty()) {
        // The current implementation always expects a non-empty input.
        // Otherwise, it blocks forever on a `read` call.
        codepipe << " ";
      } else {
        codepipe << code;
      }
      codepipe.flush();
      // We need to close the file descriptor manually because the `close()`
      // member function of the codepipe doesn't do this.
      // The destructor of `codepipe` will try to close the file descriptors
      // unless they are set to -1.
      ::close(codepipe.pipe().native_sink());
      codepipe.pipe().assign_sink(-1);
      // Although we already closed the file descriptors of the codepipe we now
      // also close the wrapper object to make sure we don't leak any resources.
      codepipe.close();
      ::close(errpipe.pipe().native_sink());
      errpipe.pipe().assign_sink(-1);
      if (not child.running()) {
        auto python_error = drain_pipe(errpipe);
        diagnostic::error("{}", python_error)
          .note("python process exited with error")
          .throw_();
      }
      for (auto&& slice : input) {
        if (not child.running()) {
          auto python_error = drain_pipe(errpipe);
          diagnostic::error("{}", python_error)
            .note("python process exited with error")
            .throw_();
        }
        if (slice.rows() == 0) {
          co_yield {};
          continue;
        }
        auto original_schema_name = slice.schema().name();
        auto batch = to_record_batch(slice);
        auto stream = check(arrow::io::BufferOutputStream::Create(
          4096, tenzir::arrow_memory_pool()));
        auto ipc_write_opts = arrow::ipc::IpcWriteOptions::Defaults();
        ipc_write_opts.memory_pool = tenzir::arrow_memory_pool();
        auto writer = check(arrow::ipc::MakeStreamWriter(
          stream, slice.schema().to_arrow_schema(), ipc_write_opts));
        if (not writer->WriteRecordBatch(*batch).ok()) {
          diagnostic::error("failed to convert input batch to Arrow format")
            .note("failed to write in conversion from input batch to Arrow "
                  "format")
            .emit(ctrl.diagnostics());
          co_return;
        }
        if (auto status = writer->Close(); ! status.ok()) {
          diagnostic::error("{}", status.message())
            .note("failed to close writer in conversion from input batch to "
                  "Arrow format")
            .emit(ctrl.diagnostics());
          co_return;
        }
        auto result = stream->Finish();
        if (not result.status().ok()) {
          diagnostic::error("{}", result.status().message())
            .note(
              "failed to flush in conversion from input batch to Arrow format")
            .emit(ctrl.diagnostics());
          co_return;
        }
        std_in.write(reinterpret_cast<const char*>((*result)->data()),
                     detail::narrow<int>((*result)->size()));
        auto file = arrow_fd_wrapper{std_out.native_source()};
        auto reader = arrow::ipc::RecordBatchStreamReader::Open(
          &file, arrow_ipc_read_options());
        if (not reader.status().ok()) {
          auto python_error = drain_pipe(errpipe);
          diagnostic::error("{}", python_error)
            .note("python process exited with error")
            .emit(ctrl.diagnostics());
          co_return;
        }
        auto result_batch = (*reader)->ReadNext();
        if (not result_batch.status().ok()) {
          auto python_error = drain_pipe(errpipe);
          diagnostic::error("{}", python_error)
            .note("python process exited with error")
            .emit(ctrl.diagnostics());
          co_return;
        }
        // The writer on the other side writes an invalid record batch as
        // end-of-stream marker; we have to read it now to remove it from
        // the pipe.
        if (auto result = (*reader)->ReadNext(); ! result.ok()) {
          diagnostic::error("{}", result.status().message())
            .note("failed to read closing bytes")
            .emit(ctrl.diagnostics());
          co_return;
        }
        static_cast<void>((*reader)->Close());
        // Prepare the output.
        auto output = table_slice{result_batch->batch};
        auto new_type = type{original_schema_name, output.schema()};
        auto actual_result
          = arrow::RecordBatch::Make(new_type.to_arrow_schema(),
                                     static_cast<int64_t>(output.rows()),
                                     result_batch->batch->columns());
        output = table_slice{actual_result, new_type};
        co_yield output;
      }
      std_in.close();
    } catch (const std::exception& ex) {
      diagnostic::error("{}", ex.what()).emit(ctrl.diagnostics());
    }
    co_return;
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    return execute(std::move(input), ctrl);
  }

  auto name() const -> std::string override {
    return "python";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const& /*filter*/, event_order order) const
    -> optimize_result override {
    return optimize_result::order_invariant(*this, order);
  }

  friend auto inspect(auto& f, python_operator& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.python.python-operator")
      .fields(f.field("config", x.config_),
              f.field("requirements", x.requirements_),
              f.field("code", x.code_));
  }

private:
  config config_ = {};
  std::string requirements_;
  code_or_path_t code_;
};

class Python final : public Operator<table_slice, table_slice> {
public:
  enum class Lifecycle {
    starting,
    running,
    done,
  };

  Python(config config, PythonArgs args)
    : config_{std::move(config)}, args_{std::move(args)} {
  }

  Python(Python&&) noexcept = default;
  auto operator=(Python&&) noexcept -> Python& = default;
  Python(const Python&) = delete;
  auto operator=(const Python&) -> Python& = delete;

  ~Python() override {
    cleanup_venv();
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    try {
      auto code = co_await resolve_code(ctx);
      if (not code) {
        lifecycle_ = Lifecycle::done;
        co_return;
      }
      auto requirements
        = args_.requirements ? args_.requirements->inner : std::string{};
      auto config = config_;
      auto system_config = caf::content(ctx.actor_system().config());
      auto runtime = co_await spawn_blocking(
        [config = std::move(config), requirements = std::move(requirements),
         dh = diagnostic_handler_ref{ctx.dh()},
         system_config = std::move(system_config)]() mutable {
          return prepare_runtime(config, requirements, dh, system_config);
        });
      if (not runtime) {
        lifecycle_ = Lifecycle::done;
        co_return;
      }
      venv_ = runtime->venv;
      auto spec = SubprocessSpec{
        .argv
        = {
          runtime->python_executable.string(),
          "-c",
          PYTHON_SCAFFOLD,
          fmt::to_string(code_pipe_child_fd),
          fmt::to_string(error_pipe_child_fd),
        },
        .env = make_subprocess_env(*runtime),
        .stdin_mode = PipeMode::pipe,
        .stdout_mode = PipeMode::pipe,
        .stderr_mode = PipeMode::inherit,
        .pipe_input_fds = {code_pipe_child_fd},
        .pipe_output_fds = {error_pipe_child_fd},
        .use_path = false,
      };
      subprocess_ = co_await Subprocess::spawn(std::move(spec));
      auto code_pipe = subprocess_->input_pipe(code_pipe_child_fd);
      TENZIR_ASSERT(code_pipe.is_some());
      // Keep `input` non-empty so the scaffold always receives a code payload,
      // even when `code` resolves to the empty string.
      auto input = code->empty() ? std::string{" "} : *code;
      auto bytes = std::span{
        reinterpret_cast<const std::byte*>(input.data()),
        input.size(),
      };
      co_await (*code_pipe).write(bytes);
      co_await (*code_pipe).close();
      lifecycle_ = Lifecycle::running;
    } catch (const std::exception& ex) {
      diagnostic::error("{}", ex.what()).emit(ctx.dh());
      lifecycle_ = Lifecycle::done;
      cleanup_venv();
    }
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (lifecycle_ != Lifecycle::running) {
      co_return;
    }
    auto failure = Option<std::string>{};
    try {
      if (not co_await ensure_child_running()) {
        co_await emit_subprocess_failure(ctx.dh(),
                                         "python process exited with error");
        co_return;
      }
      auto serialized = serialize_input(input, ctx.dh());
      if (not serialized) {
        lifecycle_ = Lifecycle::done;
        cleanup_venv();
        co_return;
      }
      auto stdin_pipe = subprocess_->stdin_pipe();
      TENZIR_ASSERT(stdin_pipe.is_some());
      co_await (*stdin_pipe).write(*serialized);
      auto stdout_pipe = subprocess_->stdout_pipe();
      TENZIR_ASSERT(stdout_pipe.is_some());
      auto result_batch = co_await read_output_batch(*stdout_pipe);
      if (not result_batch) {
        co_await emit_subprocess_failure(ctx.dh(),
                                         "python process exited with error");
        co_return;
      }
      auto original_schema_name = input.schema().name();
      auto output = table_slice{*result_batch};
      auto new_type = type{original_schema_name, output.schema()};
      auto actual_result
        = arrow::RecordBatch::Make(new_type.to_arrow_schema(),
                                   static_cast<int64_t>(output.rows()),
                                   (*result_batch)->columns());
      co_await push(table_slice{actual_result, new_type});
    } catch (const std::exception& ex) {
      failure = ex.what();
    }
    if (failure.is_some()) {
      co_await emit_subprocess_failure(ctx.dh(), *failure);
    }
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push);
    if (lifecycle_ == Lifecycle::done) {
      cleanup_venv();
      co_return FinalizeBehavior::done;
    }
    try {
      if (subprocess_) {
        if (auto stdin_pipe = subprocess_->stdin_pipe();
            stdin_pipe.is_some() and not(*stdin_pipe).is_closed()) {
          co_await (*stdin_pipe).close();
        }
        auto return_code = co_await subprocess_->wait();
        auto process_failed
          = not return_code.exited() or return_code.exitStatus() != 0;
        if (process_failed) {
          auto error = co_await drain_error_pipe();
          if (error.empty()) {
            error = fmt::format("python process {}", return_code.str());
          }
          if (not return_code.exited()) {
            diagnostic::error("{}", error)
              .note("python process {}", return_code.str())
              .emit(ctx.dh());
          } else {
            diagnostic::error("{}", error).emit(ctx.dh());
          }
        }
      }
    } catch (const std::exception& ex) {
      diagnostic::error("{}", ex.what()).emit(ctx.dh());
    }
    lifecycle_ = Lifecycle::done;
    cleanup_venv();
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return lifecycle_ == Lifecycle::done ? OperatorState::done
                                         : OperatorState::unspecified;
  }

  auto snapshot(Serde& serde) -> void override {
    TENZIR_UNUSED(serde);
    // This operator owns a live subprocess session whose transport state is
    // inherently local to the current process and machine.
    //
    // FIXME: To support snapshot/restore, extend the protocol between the host
    // operator and the Python subprocess so the child can export and import
    // logical execution state into a fresh subprocess after restart.
  }

private:
  auto resolve_code(OpCtx& ctx) -> Task<Option<std::string>> {
    if (args_.file) {
      auto code_chunk = chunk::make_empty();
      if (auto err = read(args_.file->inner, code_chunk); err.valid()) {
        diagnostic::error(err)
          .note("failed to read code from file")
          .emit(ctx.dh());
        co_return None{};
      }
      co_return detail::strip_leading_indentation(std::string{
        reinterpret_cast<const char*>(code_chunk->data()), code_chunk->size()});
    }
    auto code = std::string{};
    auto requests = std::vector<secret_request>{
      make_secret_request("code", args_.code, code, ctx.dh())};
    auto result = co_await ctx.resolve_secrets(std::move(requests));
    if (not result) {
      co_return None{};
    }
    co_return detail::strip_leading_indentation(std::move(code));
  }

  auto ensure_child_running() -> Task<bool> {
    TENZIR_ASSERT(subprocess_);
    // `ensure_child_running` polls with `wait_timeout(0ms)` and never blocks.
    auto result
      = co_await subprocess_->wait_timeout(std::chrono::milliseconds{0});
    co_return result.running();
  }

  auto emit_subprocess_failure(diagnostic_handler& dh,
                               std::string_view fallback) -> Task<void> {
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    lifecycle_ = Lifecycle::done;
    auto error = std::string{};
    try {
      if (subprocess_) {
        auto result
          = co_await subprocess_->wait_timeout(std::chrono::milliseconds{0});
        if (result.running()) {
          static_cast<void>(
            co_await subprocess_->terminate_or_kill(std::chrono::seconds{1}));
        }
      }
      error = co_await drain_error_pipe();
    } catch (const std::exception&) {
      error.clear();
    }
    if (error.empty()) {
      error = std::string{fallback};
    }
    diagnostic::error("{}", error).emit(dh);
    cleanup_venv();
  }

  auto drain_error_pipe() -> Task<std::string> {
    if (not subprocess_) {
      co_return std::string{};
    }
    auto pipe = subprocess_->output_pipe(error_pipe_child_fd);
    if (pipe.is_none()) {
      co_return std::string{};
    }
    auto result = std::string{};
    while (true) {
      auto chunk = co_await (*pipe).read_chunk();
      if (chunk.is_none()) {
        break;
      }
      auto view = std::string_view{
        reinterpret_cast<const char*>((*chunk)->data()),
        (*chunk)->size(),
      };
      result.append(view);
    }
    boost::trim(result);
    co_return result;
  }

  auto read_output_batch(ReadPipe& pipe)
    -> Task<Option<std::shared_ptr<arrow::RecordBatch>>> {
    auto fd = pipe.native_fd();
    auto batch = co_await spawn_blocking(
      [fd]() -> Option<std::shared_ptr<arrow::RecordBatch>> {
        auto file = arrow_fd_wrapper{fd};
        auto reader = arrow::ipc::RecordBatchStreamReader::Open(
          &file, arrow_ipc_read_options());
        if (not reader.ok()) {
          return None{};
        }
        auto result_batch = (*reader)->ReadNext();
        if (not result_batch.ok() or not result_batch->batch) {
          return None{};
        }
        auto closing = (*reader)->ReadNext();
        if (not closing.ok()) {
          return None{};
        }
        static_cast<void>((*reader)->Close());
        return result_batch->batch;
      });
    co_return batch;
  }

  auto serialize_input(table_slice input, diagnostic_handler& dh)
    -> failure_or<chunk_ptr> {
    auto batch = to_record_batch(input);
    auto stream = check(
      arrow::io::BufferOutputStream::Create(4096, tenzir::arrow_memory_pool()));
    auto ipc_write_opts = arrow::ipc::IpcWriteOptions::Defaults();
    ipc_write_opts.memory_pool = tenzir::arrow_memory_pool();
    auto writer = check(arrow::ipc::MakeStreamWriter(
      stream, input.schema().to_arrow_schema(), ipc_write_opts));
    if (not writer->WriteRecordBatch(*batch).ok()) {
      diagnostic::error("failed to convert input batch to Arrow format")
        .note("failed to write in conversion from input batch to Arrow format")
        .emit(dh);
      return failure::promise();
    }
    if (auto status = writer->Close(); not status.ok()) {
      diagnostic::error("{}", status.message())
        .note("failed to close writer in conversion from input batch to Arrow "
              "format")
        .emit(dh);
      return failure::promise();
    }
    auto result = stream->Finish();
    if (not result.status().ok()) {
      diagnostic::error("{}", result.status().message())
        .note("failed to flush in conversion from input batch to Arrow format")
        .emit(dh);
      return failure::promise();
    }
    auto bytes = std::span{
      reinterpret_cast<const std::byte*>((*result)->data()),
      detail::narrow<size_t>((*result)->size()),
    };
    return chunk::copy(bytes);
  }

  auto cleanup_venv() -> void {
    if (not venv_) {
      return;
    }
    auto ec = std::error_code{};
    if (std::filesystem::exists(*venv_, ec)) {
      std::filesystem::remove_all(*venv_, ec);
      if (ec) {
        TENZIR_WARN("python operator failed to remove venv at {}: {}", *venv_,
                    ec);
      }
    }
    venv_ = None{};
  }

  Lifecycle lifecycle_ = Lifecycle::starting;
  config config_;
  PythonArgs args_;
  Option<Subprocess> subprocess_ = None{};
  Option<std::filesystem::path> venv_ = None{};
};

class plugin final : public virtual operator_plugin2<python_operator>,
                     public virtual OperatorPlugin {
public:
  struct config config = {};

  auto initialize(const record& plugin_config, const record&)
    -> caf::error override {
    auto create_virtualenv
      = try_get_or<bool>(plugin_config, "create-venvs", true);
    if (not create_virtualenv) {
      return create_virtualenv.error();
    }
    config.create_venvs = *create_virtualenv;
    if (auto const* implicit_requirements
        = get_if<std::string>(&plugin_config, "implicit-requirements")) {
      config.implicit_requirements = *implicit_requirements;
    }
    return {};
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto requirements = std::optional<std::string>{};
    auto code = std::optional<located<secret>>{};
    auto path = std::optional<located<std::string>>{};
    auto parser = argument_parser2::operator_("python")
                    .positional("code", code)
                    .named("file", path)
                    .named("requirements", requirements);
    TRY(parser.parse(inv, ctx));
    if (not path && not code) {
      diagnostic::error("must have either the `file` argument or inline code")
        .primary(inv.self)
        .usage(parser.usage())
        .docs(parser.docs())
        .emit(ctx);
      return failure::promise();
    }
    if (path && code) {
      diagnostic::error("cannot have `file` argument together with inline code")
        .primary(path->source)
        .primary(code->source)
        .usage(parser.usage())
        .docs(parser.docs())
        .emit(ctx);
      return failure::promise();
    }
    auto code_or_path = code_or_path_t{};
    if (code) {
      code_or_path.inner = std::move(code->inner);
      code_or_path.source = code->source;
    } else {
      code_or_path.inner = std::filesystem::path{path->inner};
      code_or_path.source = path->source;
    }
    if (not requirements) {
      requirements = "";
    }
    return std::make_unique<python_operator>(config, std::move(*requirements),
                                             std::move(code_or_path));
  }

  auto describe() const -> Description override {
    auto d = Describer<PythonArgs>{};
    auto code = d.optional_positional("code", &PythonArgs::code);
    auto file = d.named("file", &PythonArgs::file);
    d.named("requirements", &PythonArgs::requirements);
    d.spawner([config = this->config, code, file]<class Input>(DescribeCtx& ctx)
                -> failure_or<Option<SpawnWith<PythonArgs, Input>>> {
      auto validate = [&](DescribeCtx& ctx) -> failure_or<void> {
        auto has_code = ctx.get(code).has_value();
        auto has_file = ctx.get(file).has_value();
        if (has_code and has_file) {
          if (auto code_loc = ctx.get_location(code)) {
            auto diag = diagnostic::error("cannot have `file` argument "
                                          "together with inline code")
                          .primary(*code_loc, "");
            if (auto file_loc = ctx.get_location(file)) {
              std::move(diag).primary(*file_loc, "").emit(ctx);
            } else {
              std::move(diag).emit(ctx);
            }
          } else if (auto file_loc = ctx.get_location(file)) {
            diagnostic::error(
              "cannot have `file` argument together with inline code")
              .primary(*file_loc, "")
              .emit(ctx);
          } else {
            diagnostic::error(
              "cannot have `file` argument together with inline code")
              .emit(ctx);
          }
          return failure::promise();
        }
        if (not has_code and not has_file) {
          diagnostic::error(
            "must have either the `file` argument or inline code")
            .emit(ctx);
          return failure::promise();
        }
        return {};
      };
      TRY(validate(ctx));
      if constexpr (not std::same_as<Input, table_slice>) {
        return None{};
      } else {
        return SpawnWith<PythonArgs, table_slice>{
          [config](PythonArgs args) -> Box<Operator<table_slice, table_slice>> {
            return Box<Operator<table_slice, table_slice>>{
              Python{config, std::move(args)}};
          }};
      }
    });
    return d.without_optimize();
  }
};

} // namespace
} // namespace tenzir::plugins::python

TENZIR_REGISTER_PLUGIN(tenzir::plugins::python::plugin)
