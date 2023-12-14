//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/strip_leading_indentation.hpp"
#include "tenzir/format/arrow.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/as_bytes.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/concept/parseable/string/quoted_string.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/detail/installdirs.hpp>
#include <tenzir/error.hpp>
#include <tenzir/generator.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/type.hpp>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
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

auto PYTHON_SCAFFOLD = R"_(
from tenzir.tools.python_operator_executor import main

main()
)_";

struct config {
  // Implicit arguments passed to every invocation of `pip install`.
  std::string implicit_requirements = {};

  // Base path for virtual environments.
  std::optional<std::filesystem::path> venv_base_dir = {};
};

auto drain_pipe(bp::ipstream& pipe) -> std::string {
  auto result = std::string{};
  if (pipe.peek() == bp::ipstream::traits_type::eof())
    return result;
  auto line = std::string{};
  while (std::getline(pipe, line)) {
    if (not result.empty()) {
      result += '\n';
    }
    result += line;
  }
  return result;
}

class python_operator final : public crtp_operator<python_operator> {
public:
  python_operator() = default;

  explicit python_operator(const config* config, std::string requirements,
                           std::string code)
    : config_{config}, requirements_{std::move(requirements)} {
    if (code.empty()) {
      code_ = "pass";
    } else {
      code_ = std::move(code);
    }
  }

  auto execute(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    TENZIR_ASSERT(config_);
    try {
      bp::pipe std_out;
      bp::pipe std_in;
      bp::pipe std_err;
      auto python_executable = bp::search_path("python3");
      auto env = boost::this_process::environment();
      // Automatically create a virtualenv with all requirements preinstalled,
      // unless disabled by node config.
      if (config_->venv_base_dir) {
        auto venv_id = hash(config_->implicit_requirements, requirements_);
        auto venv_path
          = config_->venv_base_dir.value() / fmt::format("{:x}", venv_id);
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
        // hidden modification by starting the seamphore name with a slash.
        // This ensures that the truncation logic below is correct.
        auto sem_name = fmt::format("/tnz-python-{:x}", venv_id);
        // The semaphore name is restricted to a maximum length of 31 characters
        // (including the '\0') on macOS. This length has been experimentally
        // verified. We truncate it to this length unconditionally for
        // consistency.
        constexpr auto semaphore_name_max_length = 30u;
        if (sem_name.size() > semaphore_name_max_length)
          sem_name.erase(semaphore_name_max_length);
        auto sem = boost::interprocess::named_semaphore{
          boost::interprocess::open_or_create, sem_name.c_str(), 1u};
        sem.wait();
        auto sem_guard = caf::detail::scope_guard{[&] {
          sem.post();
        }};
        // TODO: Handle broken venvs. Maybe there is a way to check whether the
        // list of requirements is installed correctly?
        if (!exists(venv_path, ec)) {
          if (bp::system(python_executable, "-m", "venv", venv)) {
            ctrl.abort(ec::system_error,
                       "failed to create virtualenv (see log for details)");
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
          if (!config_->implicit_requirements.empty()) {
            auto implicit_requirements_vec = detail::split_escaped(
              config_->implicit_requirements, " ", "\\");
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
          TENZIR_VERBOSE("installing python modules with: '{}'",
                         fmt::join(pip_invocation, "' '"));
          if (bp::system(pip_invocation, env)) {
            ctrl.abort(ec::system_error, "failed to install pip requirements "
                                         "(see log for details)");
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
      auto child
        = bp::child{boost::process::filesystem::path{python_executable},
                    "-c",
                    PYTHON_SCAFFOLD,
                    fmt::to_string(codepipe.pipe().native_source()),
                    fmt::to_string(errpipe.pipe().native_sink()),
                    env,
                    bp::std_out > std_out,
                    bp::std_in < std_in};
      codepipe << detail::strip_leading_indentation(std::string{code_});
      codepipe.close();
      ::close(errpipe.pipe().native_sink());
      co_yield {}; // signal successful startup
      for (auto&& slice : input) {
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
          ctrl.abort(ec::system_error,
                     "failed to convert input batch to arrow format");
          co_return;
        }
        if (auto status = writer->Close(); !status.ok()) {
          ctrl.abort(ec::system_error, "failed to convert input: {}",
                     status.message());
          co_return;
        }
        auto result = stream->Finish();
        if (!result.status().ok()) {
          ctrl.abort(ec::system_error, "failed to finish input buffer: {}",
                     result.status().message());
          co_return;
        }
        std_in.write(reinterpret_cast<const char*>((*result)->data()),
                     detail::narrow<int>((*result)->size()));
        auto file = format::arrow::arrow_fd_wrapper{std_out.native_source()};
        auto reader = arrow::ipc::RecordBatchStreamReader::Open(&file);
        if (!reader.status().ok()) {
          auto python_error = drain_pipe(errpipe);
          ctrl.abort(ec::logic_error, "python error: {}", python_error);
          co_return;
        }
        auto result_batch = (*reader)->ReadNext();
        if (!result_batch.status().ok()) {
          TENZIR_WARN("failed to read data from python: {}",
                      result_batch.status().message());
          auto python_error = drain_pipe(errpipe);
          ctrl.abort(ec::logic_error, "python error: {}", python_error);
          co_return;
        }
        // The writer on the other side writes an invalid record batch as
        // end-of-stream marker; we have to read it now to remove it from
        // the pipe.
        if (auto result = (*reader)->ReadNext(); !result.ok()) {
          ctrl.abort(ec::logic_error,
                     "python error: failed to read closing bytes");
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
      ctrl.abort(ec::logic_error, "{}", ex.what());
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

  friend auto inspect(auto& f, python_operator& x) {
    return f.object(x)
      .pretty_name("tenzir.plugins.python.python-operator")
      .fields(f.field("requirements", x.requirements_),
              f.field("code", x.code_));
  }

private:
  const config* config_ = nullptr;
  std::string requirements_ = {};
  std::string code_ = {};
};

class plugin final : public virtual operator_plugin<python_operator> {
public:
  struct config config = {};

  auto initialize(const record& plugin_config, const record& global_config)
    -> caf::error override {
    auto create_virtualenv
      = try_get_or<bool>(plugin_config, "create-venvs", true);
    if (!create_virtualenv)
      return create_virtualenv.error();
    if (!(*create_virtualenv))
      config.venv_base_dir = std::nullopt;
    else if (const auto* cache_dir
             = get_if<std::string>(&global_config, "tenzir.cache-directory"))
      config.venv_base_dir
        = std::filesystem::path{*cache_dir} / "python" / "venvs";
    else
      config.venv_base_dir = std::filesystem::temp_directory_path() / "tenzir"
                             / "python" / "venvs";
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
    auto command = std::string{};
    auto requirements = std::string{};
    auto parser = argument_parser{"python", "https://docs.tenzir.com/next/"
                                            "operators/transformations/python"};
    parser.add("-r,--requirements", requirements, "<requirements>");
    parser.add(command, "<command>");
    parser.parse(p);
    return std::make_unique<python_operator>(&config, std::move(requirements),
                                             std::move(command));
  }
};

} // namespace
} // namespace tenzir::plugins::python

TENZIR_REGISTER_PLUGIN(tenzir::plugins::python::plugin)
