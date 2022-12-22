//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/settings.hpp"
#include "vast/detail/signal_handlers.hpp"
#include "vast/event_types.hpp"
#include "vast/factory.hpp"
#include "vast/format/reader_factory.hpp" // IWYU pragma: keep
#include "vast/format/writer_factory.hpp" // IWYU pragma: keep
#include "vast/logger.hpp"
#include "vast/module.hpp"
#include "vast/plugin.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/application.hpp"
#include "vast/system/default_configuration.hpp"
#include "vast/system/make_pipelines.hpp"
#include "vast/system/signal_reflector.hpp"

#include <arrow/util/compression.h>
#include <caf/actor_system.hpp>

#include <csignal>
#include <cstdlib>
#include <iostream>
#include <string_view>
#include <thread>

namespace {

/// Try to handle deprecation warnings, or return an exit code if that is
/// impossible.
std::optional<int>
try_handle_deprecations(vast::system::default_configuration& cfg) {
  using namespace vast;
  const auto meta_index_fp_rate
    = caf::get_if<double>(&cfg, "vast.meta-index-fp-rate");
  const auto catalog_fp_rate
    = caf::get_if<double>(&cfg, "vast.catalog-fp-rate");
  const auto index_default_fp_rate
    = caf::get_if<double>(&cfg, "vast.index.default-fp-rate");
  if (meta_index_fp_rate || catalog_fp_rate) {
    if (index_default_fp_rate || (meta_index_fp_rate && catalog_fp_rate)) {
      VAST_ERROR("the 'vast.meta-index-fp-rate' and 'vast.catalog-fp-rate' "
                 "options are deprecated; please remove them from your "
                 "configuration and use 'vast.index.default-fp-rate' "
                 "instead");
      return EXIT_FAILURE;
    }
    VAST_WARN("the 'vast.meta-index-fp-rate' and 'vast.catalog-fp-rate' "
              "options are deprecated; automatically setting their "
              "replacement 'vast.index.default-fp-rate' instead");
    caf::put(cfg.content, "vast.index.default-fp-rate",
             meta_index_fp_rate ? *meta_index_fp_rate : *catalog_fp_rate);
  }
  if (caf::get_or(cfg, "vast.store-backend", "feather") == "segment-store") {
    VAST_WARN("the 'vast.store-backend' option 'segment-store' is deprecated; "
              "automatically using 'feather' instead");
    caf::put(cfg.content, "vast.store-backend", "feather");
  }
  const auto transforms
    = caf::get_if<caf::config_value::dictionary>(&cfg, "vast.transforms");
  const auto pipelines
    = caf::get_if<caf::config_value::dictionary>(&cfg, "vast.pipelines");
  if (transforms) {
    if (pipelines) {
      VAST_ERROR("the 'vast.transforms' key is deprecated; please remove it "
                 "from your configuration and use 'vast.pipelines' instead");
      return EXIT_FAILURE;
    }
    VAST_WARN("key 'vast.transforms' is deprecated; automatically setting the "
              "replacement 'vast.pipelines' instead");
    caf::put(cfg.content, "vast.pipelines", *transforms);
  }
  const auto transform_triggers
    = caf::get_if<caf::config_value::dictionary>(&cfg, "vast.transform-"
                                                       "triggers");
  const auto pipeline_triggers
    = caf::get_if<caf::config_value::dictionary>(&cfg, "vast.pipeline-"
                                                       "triggers");
  if (transform_triggers) {
    if (pipeline_triggers) {
      VAST_ERROR("the 'vast.transform-triggers' key is deprecated; please "
                 "remove it from your configuration and use "
                 "'vast.pipeline-triggers' instead");
      return EXIT_FAILURE;
    }
    VAST_WARN("key 'vast.transform-triggers' is deprecated; automatically "
              "setting the replacement 'vast.pipeline-triggers' instead");
    caf::put(cfg.content, "vast.pipeline-triggers", *transform_triggers);
  }
  return std::nullopt;
}

} // namespace

int main(int argc, char** argv) {
  using namespace vast;
  // Set a signal handler for fatal conditions. Prints a backtrace if support
  // for that is enabled.
  std::signal(SIGSEGV, fatal_handler);
  std::signal(SIGABRT, fatal_handler);
  // Mask SIGINT and SIGTERM so we can handle those in a dedicated thread.
  auto sigset = system::termsigset();
  pthread_sigmask(SIG_BLOCK, &sigset, nullptr);
  // Set up our configuration, e.g., load of YAML config file(s).
  system::default_configuration cfg;
  if (auto err = cfg.parse(argc, argv)) {
    std::cerr << "failed to parse configuration: " << to_string(err)
              << std::endl;
    return EXIT_FAILURE;
  }
  auto loaded_plugin_paths = plugins::load({VAST_BUNDLED_PLUGINS}, cfg);
  if (!loaded_plugin_paths) {
    fmt::print(stderr, "{}\n", loaded_plugin_paths.error());
    return EXIT_FAILURE;
  }
  // Initialize factories.
  factory<format::reader>::initialize();
  factory<format::writer>::initialize();
  // Application setup.
  auto [root, root_factory] = system::make_application(argv[0]);
  if (!root)
    return EXIT_FAILURE;
  // Parse the CLI.
  auto invocation
    = parse(*root, cfg.command_line.begin(), cfg.command_line.end());
  if (!invocation) {
    if (invocation.error()) {
      system::render_error(*root, invocation.error(), std::cerr);
      return EXIT_FAILURE;
    }
    // Printing help/documentation texts returns caf::no_error, and we want to
    // indicate success when printing the help/documentation texts.
    return EXIT_SUCCESS;
  }
  // Merge the options from the CLI into the options from the configuration.
  // From here on, options from the command line can be used.
  detail::merge_settings(invocation->options, cfg.content,
                         policy::merge_lists::yes);
  // Tweak CAF parameters in case we're running a client command.
  bool is_server = invocation->full_name == "start"
                   || caf::get_or(cfg.content, "vast.node", false);
  std::string_view max_threads_key = "caf.scheduler.max-threads";
  if (!is_server
      && !caf::holds_alternative<caf::config_value::integer>(cfg,
                                                             max_threads_key))
    cfg.set(max_threads_key, 2);
  // Create log context as soon as we know the correct configuration.
  auto log_context = create_log_context(*invocation, cfg.content);
  if (!log_context)
    return EXIT_FAILURE;
  // Print the configuration file(s) that were loaded.
  if (!cfg.config_file_path.empty())
    cfg.config_files.emplace_back(std::move(cfg.config_file_path));
  for (const auto& file : system::loaded_config_files())
    VAST_INFO("loaded configuration file: {}", file);
  // Print the plugins that were loaded, and errors that occured during loading.
  for (const auto& file : *loaded_plugin_paths)
    VAST_INFO("loaded plugin: {}", file);
  // Initialize successfully loaded plugins.
  if (auto err = plugins::initialize(cfg)) {
    VAST_ERROR("failed to initialize plugins: {}", err);
    return EXIT_FAILURE;
  }
  // Issue deprecation warnings.
  if (auto exit_code = try_handle_deprecations(cfg))
    return *exit_code;
  // Eagerly verify that the Arrow libraries we're using have Zstd support so
  // we can assert this works when serializing record batches.
  {
    const auto default_compression_level
      = arrow::util::Codec::DefaultCompressionLevel(arrow::Compression::ZSTD);
    if (!default_compression_level.ok()) {
      VAST_ERROR("failed to configure Zstd codec for Apache Arrow: {}",
                 default_compression_level.status().ToString());
      return EXIT_FAILURE;
    }
    auto compression_level
      = caf::get_or(cfg, "vast.zstd-compression-level",
                    default_compression_level.ValueUnsafe());
    auto min_level
      = arrow::util::Codec::MinimumCompressionLevel(arrow::Compression::ZSTD);
    auto max_level
      = arrow::util::Codec::MaximumCompressionLevel(arrow::Compression::ZSTD);
    if (!min_level.ok()) {
      VAST_ERROR("unable to configure Zstd codec for Apache Arrow: {}",
                 min_level.status().ToString());
      return EXIT_FAILURE;
    }
    if (!max_level.ok()) {
      VAST_ERROR("unable to configure Zstd codec for Apache Arrow: {}",
                 max_level.status().ToString());
      return EXIT_FAILURE;
    }
    if (compression_level < min_level.ValueUnsafe()
        || compression_level > max_level.ValueUnsafe()) {
      VAST_ERROR("Zstd compression level '{}' outside of valid range [{}, {}]",
                 compression_level, min_level.ValueUnsafe(),
                 max_level.ValueUnsafe());
      return EXIT_FAILURE;
    }
    auto codec
      = arrow::util::Codec::Create(arrow::Compression::ZSTD, compression_level);
    if (!codec.ok()) {
      VAST_ERROR("failed to create Zstd codec for Apache Arrow: {}",
                 codec.status().ToString());
      return EXIT_FAILURE;
    }
  }
  // Eagerly verify the export transform configuration, to avoid hidden
  // configuration errors that pop up the first time a user tries to run
  // `vast export`.
  if (auto export_transforms
      = make_pipelines(system::pipelines_location::server_export, cfg.content);
      !export_transforms) {
    VAST_ERROR("invalid export transform configuration: {}",
               export_transforms.error());
    return EXIT_FAILURE;
  }
  // Set up the event types singleton.
  if (auto module = load_module(cfg)) {
    event_types::init(*std::move(module));
  } else {
    VAST_ERROR("failed to read schema dirs: {}", module.error());
    return EXIT_FAILURE;
  }
  // Lastly, initialize the actor system context, and execute the given
  // command. From this point onwards, do not execute code that is not
  // thread-safe.
  auto sys = caf::actor_system{cfg};
  // The reflector scope variable cleans up the reflector on destruction.
  scope_linked<system::signal_reflector_actor> reflector{
    sys.spawn<caf::detached>(system::signal_reflector)};
  std::atomic<bool> stop = false;
  // clang-format off
  auto signal_monitoring_thread = std::thread([&]()
#if VAST_GCC
      // Workaround for an ASAN bug that only occurs with GCC.
      // https://gcc.gnu.org/bugzilla//show_bug.cgi?id=101476
      __attribute__((no_sanitize_address))
#endif
      {
        int signum = 0;
        sigwait(&sigset, &signum);
        VAST_DEBUG("received signal {}", signum);
        if (!stop)
          caf::anon_send<caf::message_priority::high>(
            reflector.get(), atom::internal_v, atom::signal_v, signum);
      });
  // clang-format on
  // Put it into the actor registry so any actor can communicate with it.
  sys.registry().put("signal-reflector", reflector.get());
  auto run_error = caf::error{};
  if (auto result = run(*invocation, sys, root_factory); !result)
    run_error = std::move(result.error());
  else
    caf::message_handler{[&](caf::error& err) {
      run_error = std::move(err);
    }}(*result);
  sys.registry().erase("signal-reflector");
  stop = true;
  if (pthread_cancel(signal_monitoring_thread.native_handle()) != 0)
    VAST_ERROR("failed to cancel signal monitoring thread");
  signal_monitoring_thread.join();
  pthread_sigmask(SIG_UNBLOCK, &sigset, nullptr);
  if (run_error) {
    system::render_error(*root, run_error, std::cerr);
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
