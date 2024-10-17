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
#include <caf/actor_system_config.hpp>
#include <caf/detail/scope_guard.hpp>
#include <caf/settings.hpp>

#include <filesystem>

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

constexpr auto PYTHON_SCAFFOLD = R"_(
from tenzir.tools.python_operator_executor import main

main()
)_";

struct config {
  // Implicit arguments passed to every invocation of `pip install`.
  std::optional<std::string> implicit_requirements = {};

  // Whether to create a virtualenv environment for the python
  // operator.
  bool create_venvs = true;

  template <class Inspector>
  friend auto inspect(Inspector& f, config& x) -> bool {
    auto venv_base_dir_str = std::string{};
    auto result
      = f.object(x)
          .pretty_name("tenzir.plugins.python.config")
          .fields(f.field("implicit-requirements", x.implicit_requirements),
                  f.field("create-venvs", x.create_venvs));
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
      // Compute some config values delayed at runtime, because
      // `detail::install_datadir` and the venv base dir may be different
      // between the client and node process.
      auto implicit_requirements = std::string{};
      if (config_.implicit_requirements) {
        implicit_requirements = *config_.implicit_requirements;
      } else {
        implicit_requirements = std::string{
          detail::install_datadir() / "python"
          / fmt::format("tenzir-{}.{}.{}-py3-none-any.whl[operator]",
                        version::major, version::minor, version::patch)};
      }
      auto venv_base_dir = std::optional<std::filesystem::path>{};
      if (!config_.create_venvs) {
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
      auto maybe_venv = std::optional<std::filesystem::path>{};
      auto venv_cleanup = [&] {
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
        return caf::detail::scope_guard([maybe_venv] {
          if (maybe_venv) {
            std::error_code ec;
            auto exists = std::filesystem::exists(*maybe_venv, ec);
            if (ec) {
              // ctrl can already be gone, so we can't emit a diagnostic here.
              TENZIR_WARN("python operator failed to check for venv at {}: {}",
                          *maybe_venv, ec);
              return;
            }
            if (!exists) {
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
        auto uv_executable = bp::search_path("uv");
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
        auto pip_invocation = std::vector<std::string>{
          uv_executable.string(),
          "pip",
          "install",
          "-q",
        };
        // `split` creates an empty token in case the input was entirely
        // empty, but we don't want that so we need an extra guard.
        if (!implicit_requirements.empty()) {
          auto implicit_requirements_vec
            = detail::split_escaped(implicit_requirements, " ", "\\");
          pip_invocation.insert(pip_invocation.end(),
                                implicit_requirements_vec.begin(),
                                implicit_requirements_vec.end());
        }
        if (!requirements_.empty()) {
          auto requirements_vec = detail::split(requirements_, " ");
          pip_invocation.insert(pip_invocation.end(), requirements_vec.begin(),
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
            .throw_();
        }
        python_executable
          = std::filesystem::path{*maybe_venv} / "bin" / "python3";
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
      if (!child.running()) {
        auto python_error = drain_pipe(errpipe);
        diagnostic::error("{}", python_error)
          .note("python process exited with error")
          .throw_();
      }
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
  std::string requirements_ = {};
  std::variant<std::filesystem::path, std::string> code_ = {};
};

class plugin final : public virtual operator_plugin<python_operator>,
                     public virtual operator_factory_plugin {
public:
  struct config config = {};

  auto initialize(const record& plugin_config,
                  const record& /*global_config*/) -> caf::error override {
    auto create_virtualenv
      = try_get_or<bool>(plugin_config, "create-venvs", true);
    if (!create_virtualenv) {
      return create_virtualenv.error();
    }
    config.create_venvs = *create_virtualenv;
    if (auto const* implicit_requirements
        = get_if<std::string>(&plugin_config, "implicit-requirements")) {
      config.implicit_requirements = *implicit_requirements;
    }
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

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto requirements = std::optional<std::string>{};
    auto code = std::optional<located<std::string>>{};
    auto path = std::optional<located<std::string>>{};
    auto code_or_path = std::variant<std::filesystem::path, std::string>{};
    auto parser = argument_parser2::operator_("python")
                    .add(code, "<expr>")
                    .add("file", path)
                    .add("requirements", requirements);
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
    if (code) {
      code_or_path = code->inner;
    } else {
      code_or_path = std::filesystem::path{path->inner};
    }
    if (not requirements) {
      requirements = "";
    }
    return std::make_unique<python_operator>(config, std::move(*requirements),
                                             std::move(code_or_path));
  }
};

} // namespace
} // namespace tenzir::plugins::python

TENZIR_REGISTER_PLUGIN(tenzir::plugins::python::plugin)
