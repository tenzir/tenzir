//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/as_bytes.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/concept/parseable/string/quoted_string.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/installdirs.hpp>
#include <tenzir/detail/overload.hpp>
#include <tenzir/detail/posix.hpp>
#include <tenzir/detail/preserved_fds.hpp>
#include <tenzir/detail/strip_leading_indentation.hpp>
#include <tenzir/error.hpp>
#include <tenzir/generator.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
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
#include <boost/process.hpp>
#include <caf/detail/scope_guard.hpp>

#include <filesystem>
#include <mutex>
#include <queue>
#include <thread>

namespace bp = boost::process;

namespace tenzir::plugins::python {
namespace {

/// Arrow InputStream API implementation over a file descriptor.
class arrow_fd_wrapper : public ::arrow::io::InputStream {
public:
  explicit arrow_fd_wrapper(int fd) : fd_{fd} {
  }

  auto Close() -> ::arrow::Status override {
    int result = ::close(fd_);
    fd_ = -1;
    if (result != 0) {
      return ::arrow::Status::IOError("close(2): ", detail::describe_errno());
    }
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
    if (!bytes_read) {
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

auto PYTHON_SCAFFOLD = R"_(
from tenzir.tools.python_operator_executor import main

main()
)_";

struct config {
  // Implicit arguments passed to every invocation of `pip install`.
  std::string implicit_requirements = {};

  // Base path for virtual environments.
  std::optional<std::string> venv_base_dir = {};

  template <class Inspector>
  friend auto inspect(Inspector& f, config& x) -> bool {
    auto venv_base_dir_str = std::string{};
    auto result
      = f.object(x)
          .pretty_name("tenzir.plugins.python.config")
          .fields(f.field("implicit-requirements", x.implicit_requirements),
                  f.field("venv-base-dir", x.venv_base_dir));
    return result;
  }
};

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

class python_operator final : public crtp_operator<python_operator> {
public:
  python_operator() = default;

  explicit python_operator(struct config config, std::string requirements,
                           std::variant<std::filesystem::path, std::string> code)
    : config_{std::move(config)},
      requirements_{std::move(requirements)},
      code_{std::move(code)} {
  }

  auto execute(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    try {
      // Get the code to be executed.
      auto maybe_code = std::visit(
        detail::overload{
          [](std::filesystem::path path) -> caf::expected<std::string> {
            auto code_chunk = chunk::make_empty();
            if (auto err = read(path, code_chunk)) {
              return diagnostic::error(err)
                .note("failed to read code from file")
                .to_error();
            }
            return std::string{
              reinterpret_cast<const char*>(code_chunk->data()),
              code_chunk->size()};
          },
          [](std::string inline_code) -> caf::expected<std::string> {
            return inline_code;
          }},
        code_);
      if (!maybe_code) {
        diagnostic::error(maybe_code.error())
          .note("failed to obtain code")
          .emit(ctrl.diagnostics());
        co_return;
      }
      auto code = detail::strip_leading_indentation(std::string{*maybe_code});
      // Setup python prerequisites.
      bp::pipe std_out;
      bp::pipe std_in;
      bp::ipstream std_err;
      auto python_executable = bp::search_path("python3");
      auto env = bp::environment{boost::this_process::environment()};
      // Automatically create a virtualenv with all requirements preinstalled,
      // unless disabled by node config.
      if (config_.venv_base_dir) {
        auto venv_id
          = hash(config_.implicit_requirements, requirements_, getuid());
        auto venv_path = std::filesystem::path{config_.venv_base_dir.value()}
                         / fmt::format("{:x}", venv_id);
        auto venv = venv_path.string();
        env["VIRTUAL_ENV"] = venv;
        auto ec = std::error_code{};
        // We want to make sure that the venv is only created once on every
        // host. For this we use a system wide semaphore that starts with the
        // count one and gets decremented and then incremented by every peer.
        // In between these calls the thread is in the critical section and it
        // can create the venv in case it does not exist yet. Only the first
        // thread to enter the critical section will create the venv.
        // ---
        // Boost sometimes prepends a directory separator depending on which
        // underlying implementation is used for the semaphore. Thankfully it
        // is smart enough to check if one is already present. We prevent this
        // hidden modification by starting the semaphore name with a slash.
        // This ensures that the truncation logic below is correct.
        auto sem_name = fmt::format("/tnz-python-{:x}", venv_id);
        // The semaphore name is restricted to a maximum length of 31 characters
        // (including the '\0') on macOS. This length has been experimentally
        // verified. We truncate it to this length unconditionally for
        // consistency.
        constexpr auto semaphore_name_max_length = 30u;
        if (sem_name.size() > semaphore_name_max_length) {
          sem_name.erase(semaphore_name_max_length);
        }
        // The initial venv creation tends to take a very long time, and often
        // causes the pipeline creation to take longer then what our FE tolerate
        // in terms of wait time. As a workaround we yield early, so that the
        // pipeline appears as created and do the actual venv setup on first
        // call. At that point the delay is not problematic any more because
        // `/serve` takes care of it gracefully.
        co_yield {};
        auto sem = boost::interprocess::named_semaphore{
          boost::interprocess::open_or_create, sem_name.c_str(), 1u};
        const auto wait_ok = sem.timed_wait(std::chrono::system_clock::now()
                                            + std::chrono::seconds{60});
        if (not wait_ok) {
          diagnostic::error("failed to initialize python venv")
            .note("could not acquire named semaphore '{}' within 60 seconds",
                  sem_name)
            .emit(ctrl.diagnostics());
          boost::interprocess::named_semaphore::remove(sem_name.c_str());
          co_return;
        }
        auto sem_guard = caf::detail::scope_guard{[&] {
          sem.post();
        }};
        // TODO: Handle broken venvs. Maybe there is a way to check whether the
        // list of requirements is installed correctly?
        if (!exists(venv_path, ec)) {
          // The default size of the pipe buffer is 64k on Linux, we assume
          // (hope) that this is enough. A possible solution would be to wrap
          // the invocation in a script that drains the pipe continuously but
          // only forwards the first n bytes.
          if (bp::system(python_executable, "-m", "venv", venv,
                         bp::std_err > std_err,
                         detail::preserved_fds{{STDERR_FILENO}},
                         bp::detail::limit_handles_{})
              != 0) {
            auto venv_error = drain_pipe(std_err);
            // We need to delete the potentially broken venv here to make sure
            // that it doesn't stick around to break later runs of the python
            // operator.
            std::filesystem::remove_all(venv, ec);
            if (ec) {
              diagnostic::error("failed to create virtualenv: {}", venv_error)
                .note("failed to delete broken virtualenv: {}", ec.message())
                .hint("please remove `{}`", venv)
                .emit(ctrl.diagnostics());
              co_return;
            }
            diagnostic::error("failed to create virtualenv: {}", venv_error)
              .emit(ctrl.diagnostics());
            co_return;
          }
          auto pip_invocation = std::vector<std::string>{
            venv_path / "bin" / "pip",
            "install",
            "--disable-pip-version-check",
            "-q",
          };
          // `split` creates an empty token in case the input was entirely
          // empty, but we don't want that so we need an extra guard.
          if (!config_.implicit_requirements.empty()) {
            auto implicit_requirements_vec
              = detail::split_escaped(config_.implicit_requirements, " ", "\\");
            pip_invocation.insert(pip_invocation.end(),
                                  implicit_requirements_vec.begin(),
                                  implicit_requirements_vec.end());
          }
          if (!requirements_.empty()) {
            auto requirements_vec = detail::split(requirements_, " ");
            pip_invocation.insert(pip_invocation.end(),
                                  requirements_vec.begin(),
                                  requirements_vec.end());
          }
          std_err = bp::ipstream{};
          TENZIR_VERBOSE("installing python modules with: '{}'",
                         fmt::join(pip_invocation, "' '"));
          if (bp::system(pip_invocation, env, bp::std_err > std_err,
                         detail::preserved_fds{{STDOUT_FILENO, STDERR_FILENO}},
                         bp::detail::limit_handles_{})
              != 0) {
            auto pip_error = drain_pipe(std_err);
            diagnostic::error("{}", pip_error)
              .note("failed to install pip requirements")
              .emit(ctrl.diagnostics());
            co_return;
          }
        }
        python_executable = venv_path / "bin" / "python3";
        TENZIR_VERBOSE("python operator utilizes virtual environment {}", venv);
      }
      bp::opstream codepipe; // pipe to transmit the code
      // If we redirect stderr to get error information, we need to switch to a
      // select()-style read loop to ensure python (or a child process) doesn't
      // deadlock when trying to write to stderr. So we use a separate pipe
      // that's only used by the python executor and has well-defined semantics.
      bp::ipstream errpipe;
      auto child = bp::child{
        boost::process::filesystem::path{python_executable},
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
      if (code.empty()) {
        // The current implementation always expects a non-empty input.
        // Otherwise, it blocks forever on a `read` call.
        codepipe << " ";
      } else {
        codepipe << code;
      }
      codepipe.close();
      ::close(errpipe.pipe().native_sink());
      co_yield {}; // signal successful startup
      for (auto&& slice : input) {
        if (!child.running()) {
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
        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(
                        stream, slice.schema().to_arrow_schema())
                        .ValueOrDie();
        if (!writer->WriteRecordBatch(*batch).ok()) {
          diagnostic::error("failed to convert input batch to Arrow format")
            .note(
              "failed to write in conversion from input batch to Arrow format")
            .emit(ctrl.diagnostics());
          co_return;
        }
        if (auto status = writer->Close(); !status.ok()) {
          diagnostic::error("{}", status.message())
            .note("failed to close writer in conversion from input batch to "
                  "Arrow format")
            .emit(ctrl.diagnostics());
          co_return;
        }
        auto result = stream->Finish();
        if (!result.status().ok()) {
          diagnostic::error("{}", result.status().message())
            .note(
              "failed to flush in conversion from input batch to Arrow format")
            .emit(ctrl.diagnostics());
          co_return;
        }
        std_in.write(reinterpret_cast<const char*>((*result)->data()),
                     detail::narrow<int>((*result)->size()));
        auto file = arrow_fd_wrapper{std_out.native_source()};
        auto reader = arrow::ipc::RecordBatchStreamReader::Open(&file);
        if (!reader.status().ok()) {
          auto python_error = drain_pipe(errpipe);
          diagnostic::error("{}", python_error)
            .note("python process exited with error")
            .emit(ctrl.diagnostics());
          co_return;
        }
        auto result_batch = (*reader)->ReadNext();
        if (!result_batch.status().ok()) {
          auto python_error = drain_pipe(errpipe);
          diagnostic::error("{}", python_error)
            .note("python process exited with error")
            .emit(ctrl.diagnostics());
          co_return;
        }
        // The writer on the other side writes an invalid record batch as
        // end-of-stream marker; we have to read it now to remove it from
        // the pipe.
        if (auto result = (*reader)->ReadNext(); !result.ok()) {
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
      child.wait();
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

  auto optimize(expression const& /*filter*/, event_order /*order*/) const
    -> optimize_result override {
    // Note: The `unordered` means that we do not necessarily return the first
    // `limit_` events.
    return optimize_result{std::nullopt, event_order::unordered, copy()};
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
  std::string requirements_ = {};
  std::variant<std::filesystem::path, std::string> code_ = {};
};

class plugin final : public virtual operator_plugin<python_operator>,
                     public virtual operator_factory_plugin {
public:
  struct config config = {};

  auto initialize(const record& plugin_config, const record& global_config)
    -> caf::error override {
    auto create_virtualenv
      = try_get_or<bool>(plugin_config, "create-venvs", true);
    if (!create_virtualenv) {
      return create_virtualenv.error();
    }
    if (!(*create_virtualenv)) {
      config.venv_base_dir = std::nullopt;
    } else if (const auto* cache_dir = get_if<std::string>(
                 &global_config, "tenzir.cache-directory")) {
      config.venv_base_dir
        = (std::filesystem::path{*cache_dir} / "python" / "venvs").string();
    } else {
      config.venv_base_dir = (std::filesystem::temp_directory_path() / "tenzir"
                              / "python" / "venvs")
                               .string();
    }
    auto implicit_requirements_default = std::string{
      detail::install_datadir() / "python"
      / fmt::format("tenzir-{}.{}.{}-py3-none-any.whl[operator]",
                    version::major, version::minor, version::patch)};
    config.implicit_requirements = get_or(
      plugin_config, "implicit-requirements", implicit_requirements_default);
    return {};
  }

  auto signature() const -> operator_signature override {
    return {
      .transformation = true,
    };
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto command = std::optional<located<std::string>>{};
    auto requirements = std::string{};
    auto filename = std::optional<located<std::string>>{};
    auto parser = argument_parser{"python", "https://docs.tenzir.com/"
                                            "operators/python"};
    parser.add("-r,--requirements", requirements, "<requirements>");
    parser.add("-f,--file", filename, "<filename>");
    parser.add(command, "<command>");
    parser.parse(p);
    if (!filename && !command) {
      diagnostic::error("must have either the `--file` argument or inline code")
        .throw_();
    }
    if (filename && command) {
      diagnostic::error(
        "cannot have `--file` argument together with inline code")
        .primary(filename->source)
        .primary(command->source)
        .throw_();
    }
    auto code = std::variant<std::filesystem::path, std::string>{};
    if (command.has_value()) {
      code = command->inner;
    } else {
      code = std::filesystem::path{filename->inner};
    }
    return std::make_unique<python_operator>(config, std::move(requirements),
                                             std::move(code));
  }

  auto make(invocation inv, session ctx) const -> operator_ptr override {
    auto requirements = std::optional<std::string>{};
    auto code = std::optional<located<std::string>>{};
    auto path = std::optional<located<std::string>>{};
    auto code_or_path = std::variant<std::filesystem::path, std::string>{};
    argument_parser2::operator_("python")
      .add(code, "<expr>")
      .add("file", path)
      .add("requirements", requirements)
      .parse(inv, ctx);
    if (!path && !code) {
      diagnostic::error("must have either the `--file` argument or inline code")
        .throw_();
    }
    if (path && code) {
      diagnostic::error(
        "cannot have `--file` argument together with inline code")
        .primary(path->source)
        .primary(code->source)
        .throw_();
    }
    if (code) {
      code_or_path = code->inner;
    } else {
      code_or_path = std::filesystem::path{path->inner};
    }
    if (!requirements) {
      requirements = "";
    }
    return std::make_unique<python_operator>(config, std::move(*requirements),
                                             std::move(code_or_path));
  }
};

} // namespace
} // namespace tenzir::plugins::python

TENZIR_REGISTER_PLUGIN(tenzir::plugins::python::plugin)
