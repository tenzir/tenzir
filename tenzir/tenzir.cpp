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
#include "tenzir/diagnostics.hpp"
#include "tenzir/legacy_type.hpp" // IWYU pragma: keep
#include "tenzir/logger.hpp"
#include "tenzir/module.hpp"
#include "tenzir/modules.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/scope_linked.hpp"
#include "tenzir/session.hpp"
#include "tenzir/signal_reflector.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/resolve.hpp"

#include <arrow/compute/api.h>
#include <arrow/util/compression.h>
#include <arrow/util/utf8.h>
#include <caf/actor_registry.hpp>
#include <caf/actor_system.hpp>
#include <caf/anon_mail.hpp>
#include <caf/detail/actor_system_access.hpp>
#include <caf/fwd.hpp>
#include <caf/telemetry/metric_family_impl.hpp>
#include <caf/telemetry/metric_registry.hpp>
#include <caf/thread_owner.hpp>
#include <sys/resource.h>

#include <csignal>
#include <cstdlib>
#include <iostream>
#include <ranges>
#include <string_view>
#include <thread>
#include <unordered_map>

namespace {

auto is_server_from_app_path(std::string_view app_path) {
  const auto last_slash = app_path.find_last_of('/');
  const auto app_name = last_slash == std::string_view::npos
                          ? app_path
                          : app_path.substr(last_slash + 1);
  return app_name == "tenzir-node";
}

} // namespace

auto main(int argc, char** argv) -> int try {
  using namespace tenzir;
  // Ensure the signal handler object file is linked (needed for static builds).
  signal_handlers_anchor();
  arrow::util::InitializeUTF8();
#if ARROW_VERSION_MAJOR >= 21
  if (auto status = arrow::compute::Initialize(); not status.ok()) {
    fmt::println(stderr, "failed to initialize arrow compute functions: {}",
                 status.message());
    return EXIT_FAILURE;
  }
#endif
  // Tweak CAF parameters in case we're running a client command.
  const auto is_server = is_server_from_app_path(argv[0]);
  // Mask SIGINT and SIGTERM so we can handle those in a dedicated thread.
  auto sigset = termsigset();
  if (is_server) {
    pthread_sigmask(SIG_BLOCK, &sigset, nullptr);
  }
  // Set up our configuration, e.g., load of YAML config file(s).
  default_configuration cfg;
  if (auto err = cfg.parse(argc, argv)) {
    fmt::print(stderr, "failed to parse configuration: {}\n", err);
    return EXIT_FAILURE;
  }
  auto loaded_plugin_paths = plugins::load({TENZIR_BUNDLED_PLUGINS}, cfg);
  if (not loaded_plugin_paths) {
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
  if (not root) {
    return EXIT_FAILURE;
  }
  // Parse the CLI.
  auto invocation
    = parse(*root, cfg.command_line.begin(), cfg.command_line.end());
  if (not invocation) {
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
  // Create log context as soon as we know the correct configuration.
  auto log_context = create_log_context(is_server, *invocation, cfg.content);
  if (not log_context) {
    return EXIT_FAILURE;
  }
  if (not is_server) {
    // Force the use of $TMPDIR as cache directory when running as a client.
    auto ec = std::error_code{};
    const auto* previous_value
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
    if (not default_compression_level.ok()) {
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
    if (not min_level.ok()) {
      TENZIR_ERROR("unable to configure Zstd codec for Apache Arrow: {}",
                   min_level.status().ToString());
      return EXIT_FAILURE;
    }
    if (not max_level.ok()) {
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
    if (not codec.ok()) {
      TENZIR_ERROR("failed to create Zstd codec for Apache Arrow: {}",
                   codec.status().ToString());
      return EXIT_FAILURE;
    }
  }
  // Set up the modules singleton.
  auto symbols = load_symbols(cfg);
  if (not symbols) {
    TENZIR_ERROR("failed to read schema dirs: {}", symbols.error());
    return EXIT_FAILURE;
  }
  auto taxonomies = load_taxonomies(cfg);
  if (not taxonomies) {
    TENZIR_ERROR("failed to load concepts: {}", taxonomies.error());
    return EXIT_FAILURE;
  }
  modules::init(std::move(*symbols), std::move(taxonomies->concepts));
  // Set up pipeline aliases.
  using namespace std::literals;
  auto aliases = std::unordered_map<std::string, std::string>{};
  if (auto const* settings
      = caf::get_if<caf::settings>(&cfg, "tenzir.operators")) {
    auto r = to<record>(*settings);
    if (not r) {
      TENZIR_ERROR("could not load `tenzir.operators`: invalid record");
      return EXIT_FAILURE;
    }
    auto dh = make_diagnostic_printer(std::nullopt, color_diagnostics::yes,
                                      std::cerr);
    auto provider = session_provider::make(*dh);
    auto ctx = provider.as_session();
    auto udos = std::unordered_map<std::string, ast::pipeline>{};
    for (auto&& [name, value] : *r) {
      auto* definition = try_as<std::string>(&value);
      if (not definition) {
        TENZIR_ERROR("could not load `tenzir.operators`: alias `{}` does not "
                     "resolve to a string",
                     name);
        return EXIT_FAILURE;
      }
      auto pipe = parse_pipeline_with_bad_diagnostics(*definition, ctx);
      if (not pipe) {
        TENZIR_ERROR("parsing of user-defined operator `{}` failed", name);
        return EXIT_FAILURE;
      }
      TENZIR_ASSERT(not udos.contains(name));
      udos[name] = std::move(*pipe);
    }
    // We parse user-defined operators in a loop; if in one iteration not a
    // single operator resolved, we know that the definition is invalid.
    // Note that this algorithm has a worst-case complexity of O(n^2), but that
    // should be a non-issue in practice as the number of UDOs defined is
    // usually rather small.
    while (not udos.empty()) {
      auto resolved = std::vector<std::string>{};
      auto unresolved_diags = std::vector<diagnostic>{};
      for (auto& udo : udos) {
        auto resolve_dh = collecting_diagnostic_handler{};
        auto resolve_provider = session_provider::make(resolve_dh);
        auto resolve_ctx = resolve_provider.as_session();
        // We already resolve entities here. This means that we can provide
        // earlier errors, but that it's impossible to form cyclic references.
        // We do not resolve `let` bindings yet in order to delay their
        // evaluation in cases such as `let $t = now()`.
        if (not resolve_entities(udo.second, resolve_ctx)) {
          std::ranges::move(std::move(resolve_dh).collect(),
                            std::back_inserter(unresolved_diags));
          continue;
        }
        for (auto diag : std::move(resolve_dh).collect()) {
          dh->emit(std::move(diag));
        }
        resolved.push_back(udo.first);
      }
      if (resolved.empty()) {
        for (auto& diag : unresolved_diags) {
          dh->emit(std::move(diag));
        }
        TENZIR_ERROR("failed to resolve user-defined operators: `{}`",
                     fmt::join(udos | std::ranges::views::keys, "`, `"));
        return EXIT_FAILURE;
      }
      {
        auto to_add = std::vector<std::pair<std::string, ast::pipeline>>{};
        to_add.reserve(resolved.size());
        for (const auto& name : resolved) {
          auto it = udos.find(name);
          TENZIR_ASSERT(it != udos.end());
          to_add.emplace_back(it->first, std::move(it->second));
        }
        auto guard = begin_registry_update();
        auto base = guard.current();
        auto next = base->clone();
        for (auto& [name, def] : to_add) {
          next->add(std::string{entity_pkg_cfg}, name,
                    user_defined_operator{std::move(def), {}, {}});
        }
        guard.publish(std::shared_ptr<const registry>{std::move(next)});
      }
      for (const auto& name : std::exchange(resolved, {})) {
        udos.erase(name);
      }
    }
  }
  // Lastly, initialize the actor system context, and execute the given
  // command. From this point onwards, do not execute code that is not
  // thread-safe.
  auto sys = caf::actor_system{cfg};
  auto run_error = caf::error{};
  if (is_server) {
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
          TENZIR_WARN("received signal {}", signum);
          if (!stop) {
            caf::anon_mail(atom::internal_v, atom::signal_v, signum)
              .urgent().send(reflector.get());
          }
        });
    auto signal_monitoring_joiner = detail::scope_guard{[&]() noexcept {
      stop = true;
      if (pthread_cancel(signal_monitoring_thread.native_handle()) != 0) {
        TENZIR_ERROR("failed to cancel signal monitoring thread");
      }
      signal_monitoring_thread.join();
    }};
    // clang-format on
    // Put it into the actor registry so any actor can communicate with it.
    sys.registry().put("signal-reflector", reflector.get());
    if (auto result = run(*invocation, sys, root_factory); not result) {
      run_error = std::move(result.error());
    } else {
      caf::message_handler{[&](caf::error& err) {
        run_error = std::move(err);
      }}(*result);
    }
    signal_monitoring_joiner.trigger();
    sys.registry().erase("signal-reflector");
    pthread_sigmask(SIG_UNBLOCK, &sigset, nullptr);
    sys.await_actors_before_shutdown(false);
    if (sys.running_actors_count() > 0) {
      std::this_thread::sleep_for(std::chrono::seconds{1});
    }
    std::unordered_map<std::string, int64_t> zombies = {};
    auto collector = [&](const caf::telemetry::metric_family* /*family*/,
                         const caf::telemetry::metric* instance,
                         const caf::telemetry::int_gauge* wrapped) {
      if (wrapped->value() != 0) {
        zombies[std::string{instance->labels()[0].value()}] = wrapped->value();
      }
    };
    for (int cnt = 10; cnt > 0 and sys.running_actors_count() > 0; cnt--) {
      zombies.clear();
      sys.running_actors_metric_family()->collect(collector);
      TENZIR_INFO("waiting {} more seconds for leftover components to "
                  "terminate: {}",
                  cnt, zombies);
      std::this_thread::sleep_for(std::chrono::seconds{1});
    }
    if (sys.running_actors_count() > 0) {
      zombies.clear();
      sys.running_actors_metric_family()->collect(collector);
      TENZIR_WARN("Unclean shutdown, leftover components: {}", zombies);
    }
  } else {
    if (auto result = run(*invocation, sys, root_factory); not result) {
      run_error = std::move(result.error());
    } else {
      caf::message_handler{[&](caf::error& err) {
        run_error = std::move(err);
      }}(*result);
    }
  }
  if (not run_error.empty()) {
    render_error(*root, run_error, std::cerr);
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
} catch (tenzir::panic_exception& e) {
  auto diagnostic = to_diagnostic(e);
  const auto is_server = is_server_from_app_path(argv[0]);
  if (not is_server) {
    auto dh = make_diagnostic_printer(
      std::nullopt, tenzir::color_diagnostics::yes, std::cerr);
    dh->emit(std::move(diagnostic));
  } else {
    auto buffer = std::stringstream{};
    buffer << "internal error\n";
    auto printer = make_diagnostic_printer(
      std::nullopt, tenzir::color_diagnostics::no, buffer);
    printer->emit(diagnostic);
    auto string = std::move(buffer).str();
    if (not string.empty() and string.back() == '\n') {
      string.pop_back();
    }
    fmt::println(stderr, "{}", string);
  }
  return EXIT_FAILURE;
}
