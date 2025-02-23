//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/application.hpp"
#include "tenzir/concept/convertible/to.hpp"
#include "tenzir/default_configuration.hpp"
#include "tenzir/detail/posix.hpp"
#include "tenzir/detail/scope_guard.hpp"
#include "tenzir/detail/settings.hpp"
#include "tenzir/detail/signal_handlers.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/module.hpp"
#include "tenzir/modules.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/scope_linked.hpp"
#include "tenzir/session.hpp"
#include "tenzir/signal_reflector.hpp"
#include "tenzir/tql/parser.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/resolve.hpp"

#include <arrow/util/compression.h>
#include <caf/actor_registry.hpp>
#include <caf/actor_system.hpp>
#include <caf/anon_mail.hpp>
#include <caf/fwd.hpp>
#include <sys/resource.h>

#include <csignal>
#include <cstdlib>
#include <iostream>
#include <string_view>
#include <thread>
#include <unordered_map>

auto main(int argc, char** argv) -> int {
  using namespace tenzir;
  // Set a signal handler for fatal conditions. Prints a backtrace if support
  // for that is enabled.
  if (SIG_ERR == std::signal(SIGSEGV, fatal_handler)) [[unlikely]] {
    fmt::print(stderr, "failed to set signal handler for SIGSEGV\n");
    return EXIT_FAILURE;
  }
  if (SIG_ERR == std::signal(SIGABRT, fatal_handler)) [[unlikely]] {
    fmt::print(stderr, "failed to set signal handler for SIGABRT\n");
    return EXIT_FAILURE;
  }
  // Mask SIGINT and SIGTERM so we can handle those in a dedicated thread.
  auto sigset = termsigset();
  pthread_sigmask(SIG_BLOCK, &sigset, nullptr);
  // Set up our configuration, e.g., load of YAML config file(s).
  default_configuration cfg;
  if (auto err = cfg.parse(argc, argv)) {
    fmt::print(stderr, "failed to parse configuration: {}\n", err);
    return EXIT_FAILURE;
  }
  auto loaded_plugin_paths = plugins::load({TENZIR_BUNDLED_PLUGINS}, cfg);
  if (!loaded_plugin_paths) {
    fmt::print(stderr, "{}\n", loaded_plugin_paths.error());
    return EXIT_FAILURE;
  }
  // Make sure to deinitialize all plugins at the end. This guard has to be
  // created before the call to `make_application`, as the return value of that
  // can reference dynamically loaded command plugins, which must not be
  // unloaded before the destructor of the return value.
  auto plugin_guard = detail::scope_guard([&]() noexcept {
    plugins::get_mutable().clear();
  });
  // Application setup.
  auto [root, root_factory] = make_application(argv[0]);
  if (!root) {
    return EXIT_FAILURE;
  }
  // Parse the CLI.
  auto invocation
    = parse(*root, cfg.command_line.begin(), cfg.command_line.end());
  if (!invocation) {
    if (invocation.error()) {
      render_error(*root, invocation.error(), std::cerr);
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
  const auto app_path = std::string_view{argv[0]};
  const auto last_slash = app_path.find_last_of('/');
  const auto app_name = last_slash == std::string_view::npos
                          ? app_path
                          : app_path.substr(last_slash + 1);
  bool is_server = (app_name == "tenzir-node");
  // Create log context as soon as we know the correct configuration.
  auto log_context = create_log_context(is_server, *invocation, cfg.content);
  if (!log_context) {
    return EXIT_FAILURE;
  }
  if (!is_server) {
    // Force the use of $TMPDIR as cache directory when running as a client.
    auto ec = std::error_code{};
    auto previous_value
      = get_if<std::string>(&cfg.content, "tenzir.cache-directory");
    auto tmp = std::filesystem::temp_directory_path(ec);
    if (ec) {
      TENZIR_ERROR("failed to determine location of temporary directory");
      return EXIT_FAILURE;
    }
    auto path = tmp / fmt::format("tenzir-client-cache-{:}", getuid());
    put(cfg.content, "tenzir.cache-directory", path.string());
    if (previous_value) {
      TENZIR_VERBOSE("using {} as cache directory instead of configured value "
                     "{}",
                     path, *previous_value);
    }
  }
#if TENZIR_POSIX
  struct rlimit rlimit{};
  if (::getrlimit(RLIMIT_NOFILE, &rlimit) < 0) {
    TENZIR_ERROR("failed to get RLIMIT_NOFILE: {}", detail::describe_errno());
    return -errno;
  }
  TENZIR_DEBUG("raising soft limit of open file descriptors from {} to {}",
               rlimit.rlim_cur, rlimit.rlim_max);
  rlimit.rlim_cur = rlimit.rlim_max;
  if (::setrlimit(RLIMIT_NOFILE, &rlimit) < 0) {
    TENZIR_ERROR("failed to raise soft limit of open file descriptors: {}",
                 detail::describe_errno());
    return -errno;
  }
#endif
  // Copy CAF detected default config file paths.
  for (const auto& path : cfg.config_file_paths()) {
    cfg.config_files.emplace_back(path);
  }
  // Clear the CAF based default config file paths to avoid duplicates.
  cfg.config_file_paths({});
  // Print the configuration file(s) that were loaded.
  for (const auto& file : loaded_config_files()) {
    TENZIR_VERBOSE("loaded configuration file: {}", file.path);
  }
  // Print the plugins that were loaded, and errors that occured during loading.
  for (const auto& file : *loaded_plugin_paths) {
    TENZIR_DEBUG("loaded plugin: {}", file);
  }
  // Initialize successfully loaded plugins.
  if (auto err = plugins::initialize(cfg)) {
    render_error(
      *root,
      diagnostic::error(err).note("failed to initialize plugins").to_error(),
      std::cerr);
    return EXIT_FAILURE;
  }
  // Eagerly verify that the Arrow libraries we're using have Zstd support so
  // we can assert this works when serializing record batches.
  {
    const auto default_compression_level
      = arrow::util::Codec::DefaultCompressionLevel(arrow::Compression::ZSTD);
    if (!default_compression_level.ok()) {
      TENZIR_ERROR("failed to configure Zstd codec for Apache Arrow: {}",
                   default_compression_level.status().ToString());
      return EXIT_FAILURE;
    }
    auto compression_level
      = caf::get_or(cfg, "tenzir.zstd-compression-level",
                    default_compression_level.ValueUnsafe());
    auto min_level
      = arrow::util::Codec::MinimumCompressionLevel(arrow::Compression::ZSTD);
    auto max_level
      = arrow::util::Codec::MaximumCompressionLevel(arrow::Compression::ZSTD);
    if (!min_level.ok()) {
      TENZIR_ERROR("unable to configure Zstd codec for Apache Arrow: {}",
                   min_level.status().ToString());
      return EXIT_FAILURE;
    }
    if (!max_level.ok()) {
      TENZIR_ERROR("unable to configure Zstd codec for Apache Arrow: {}",
                   max_level.status().ToString());
      return EXIT_FAILURE;
    }
    if (compression_level < min_level.ValueUnsafe()
        || compression_level > max_level.ValueUnsafe()) {
      TENZIR_ERROR(
        "Zstd compression level '{}' outside of valid range [{}, {}]",
        compression_level, min_level.ValueUnsafe(), max_level.ValueUnsafe());
      return EXIT_FAILURE;
    }
    auto codec
      = arrow::util::Codec::Create(arrow::Compression::ZSTD, compression_level);
    if (!codec.ok()) {
      TENZIR_ERROR("failed to create Zstd codec for Apache Arrow: {}",
                   codec.status().ToString());
      return EXIT_FAILURE;
    }
  }
  // Set up the modules singleton.
  auto module = load_module(cfg);
  if (not module) {
    TENZIR_ERROR("failed to read schema dirs: {}", module.error());
    return EXIT_FAILURE;
  }
  auto taxonomies = load_taxonomies(cfg);
  if (not taxonomies) {
    TENZIR_ERROR("failed to load concepts: {}", taxonomies.error());
    return EXIT_FAILURE;
  }
  modules::init(*module, std::move(taxonomies->concepts));
  // Set up pipeline aliases.
  using namespace std::literals;
  auto aliases = std::unordered_map<std::string, std::string>{};
  if (auto const* settings
      = caf::get_if<caf::settings>(&cfg, "tenzir.operators")) {
    auto r = to<record>(*settings);
    if (!r) {
      TENZIR_ERROR("could not load `tenzir.operators`: invalid record");
      return EXIT_FAILURE;
    }
    auto force_tql2 = get_or(cfg, "tenzir.tql2", false);
    auto dh = make_diagnostic_printer(std::nullopt, color_diagnostics::yes,
                                      std::cerr);
    auto provider = session_provider::make(*dh);
    auto ctx = provider.as_session();
    for (auto&& [name, value] : *r) {
      auto* definition = try_as<std::string>(&value);
      if (!definition) {
        TENZIR_ERROR("could not load `tenzir.operators`: alias `{}` does not "
                     "resolve to a string",
                     name);
        return EXIT_FAILURE;
      }
      auto use_tql2 = force_tql2 or definition->starts_with("// tql2");
      if (use_tql2) {
        auto pipe = parse_pipeline_with_bad_diagnostics(*definition, ctx);
        if (not pipe) {
          TENZIR_ERROR("parsing of user-defined operator `{}` failed", name);
          return EXIT_FAILURE;
        }
        // We already resolve entities here. This means that we can provide
        // earlier errors, but that it's impossible to form cyclic references.
        // We do not resolve `let` bindings yet in order to delay their
        // evaluation in cases such as `let $t = now()`.
        if (not resolve_entities(*pipe, ctx)) {
          TENZIR_ERROR("entity resolving in user-defined operator `{}` failed",
                       name);
          return EXIT_FAILURE;
        }
        global_registry_mut().add(entity_pkg::cfg, name,
                                  user_defined_operator{std::move(*pipe)});
      } else {
        aliases.emplace(std::move(name), *definition);
      }
    }
  }
  tql::set_operator_aliases(std::move(aliases));
  // Lastly, initialize the actor system context, and execute the given
  // command. From this point onwards, do not execute code that is not
  // thread-safe.
  auto sys = caf::actor_system{cfg};
  // The reflector scope variable cleans up the reflector on destruction.
  scope_linked<signal_reflector_actor> reflector{
    sys.spawn<caf::detached + caf::hidden>(signal_reflector)};
  std::atomic<bool> stop = false;
  // clang-format off
  auto signal_monitoring_thread = std::thread([&]()
#if TENZIR_GCC
      // Workaround for an ASAN bug that only occurs with GCC.
      // https://gcc.gnu.org/bugzilla//show_bug.cgi?id=101476
      __attribute__((no_sanitize_address))
#endif
      {
        int signum = 0;
        sigwait(&sigset, &signum);
        TENZIR_DEBUG("received signal {}", signum);
        if (!stop) {
          caf::anon_mail(atom::internal_v, atom::signal_v, signum)
            .urgent().send(reflector.get());
        }
      });
  // clang-format on
  // Put it into the actor registry so any actor can communicate with it.
  sys.registry().put("signal-reflector", reflector.get());
  auto run_error = caf::error{};
  if (auto result = run(*invocation, sys, root_factory); !result) {
    run_error = std::move(result.error());
  } else {
    caf::message_handler{[&](caf::error& err) {
      run_error = std::move(err);
    }}(*result);
  }
  sys.registry().erase("signal-reflector");
  stop = true;
  if (pthread_cancel(signal_monitoring_thread.native_handle()) != 0) {
    TENZIR_ERROR("failed to cancel signal monitoring thread");
  }
  signal_monitoring_thread.join();
  pthread_sigmask(SIG_UNBLOCK, &sigset, nullptr);
  if (is_server) {
    sys.await_actors_before_shutdown(false);
    auto actors_gone = sys.registry().await_running_count_equal(0, std::chrono::seconds{2});
    if (not actors_gone) {
      TENZIR_INFO(
        "waiting 58 more seconds for leftover components to terminate");
      actors_gone = sys.registry().await_running_count_equal(0, std::chrono::seconds{58});
      if (not actors_gone) {
        TENZIR_WARN("Unclean shutdown, leftover components: {}", sys.registry().named_running());
      }
    }
  }
  if (run_error) {
    render_error(*root, run_error, std::cerr);
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
