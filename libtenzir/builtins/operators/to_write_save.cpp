//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/concept/convertible/data.hpp>
#include <tenzir/concept/convertible/to.hpp>
#include <tenzir/detail/loader_saver_resolver.hpp>
#include <tenzir/element_type.hpp>
#include <tenzir/error.hpp>
#include <tenzir/parser_interface.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql/parser.hpp>
#include <tenzir/type.hpp>

#include <arrow/type.h>
#include <caf/expected.hpp>

#include <algorithm>
#include <string>
#include <utility>

namespace tenzir::plugins::write_to_print_save {

namespace {

enum class show_progress : bool {
  no,
  yes,
};

[[noreturn]] void throw_printer_not_found(located<std::string_view> x) {
  auto available = std::vector<std::string>{};
  for (auto const* p : plugins::get<printer_parser_plugin>()) {
    available.push_back(p->name());
  }
  diagnostic::error("printer `{}` could not be found", x.inner)
    .primary(x.source)
    .hint("must be one of {}", fmt::join(available, ", "))
    .docs("https://docs.tenzir.com/next/formats")
    .throw_();
}

[[noreturn]] void
throw_saver_not_found(located<std::string_view> x, bool use_uri_schemes) {
  auto available = std::vector<std::string>{};
  for (auto p : plugins::get<saver_parser_plugin>()) {
    if (use_uri_schemes) {
      available.push_back(p->supported_uri_scheme());
    } else {
      available.push_back(p->name());
    }
  }
  if (use_uri_schemes)
    diagnostic::error("saver for `{}` scheme could not be found", x.inner)
      .primary(x.source)
      .hint("must be one of {}", fmt::join(available, ", "))
      .docs("https://docs.tenzir.com/next/connectors")
      .throw_();
  diagnostic::error("saver `{}` could not be found", x.inner)
    .primary(x.source)
    .hint("must be one of {}", fmt::join(available, ", "))
    .docs("https://docs.tenzir.com/next/connectors")
    .throw_();
}

struct write_and_save_state {
  std::unique_ptr<printer_instance> printer;
  std::function<void(chunk_ptr)> saver;
};

class write_operator final : public crtp_operator<write_operator> {
public:
  write_operator() = default;

  write_operator(std::unique_ptr<plugin_printer> printer) noexcept
    : printer_{std::move(printer)} {
  }

  auto operator()(generator<table_slice> input,
                  operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    if (printer_->allows_joining()) {
      auto p = printer_->instantiate(type{}, ctrl);
      if (!p) {
        diagnostic::error(p.error())
          .note("failed to instantiate printer")
          .emit(ctrl.diagnostics());
        co_return;
      }
      for (auto&& slice : input) {
        for (auto&& chunk : (*p)->process(std::move(slice))) {
          co_yield std::move(chunk);
        }
      }
      for (auto&& chunk : (*p)->finish()) {
        co_yield std::move(chunk);
      }
    } else {
      auto state
        = std::optional<std::pair<std::unique_ptr<printer_instance>, type>>{};
      for (auto&& slice : input) {
        if (slice.rows() == 0) {
          co_yield {};
          continue;
        }
        if (!state) {
          auto p = printer_->instantiate(slice.schema(), ctrl);
          if (!p) {
            diagnostic::error(p.error())
              .note("failed to initialize printer")
              .emit(ctrl.diagnostics());
            co_return;
          }
          state = std::pair{std::move(*p), slice.schema()};
        } else if (state->second != slice.schema()) {
          diagnostic::error("`{}` printer does not support heterogeneous "
                            "outputs",
                            printer_->name())
            .note("cannot initialize for schema `{}` after schema `{}`",
                  slice.schema(), state->second)
            .emit(ctrl.diagnostics());
          co_return;
        }
        for (auto&& chunk : state->first->process(std::move(slice))) {
          co_yield std::move(chunk);
        }
      }
      if (state)
        for (auto&& chunk : state->first->finish()) {
          co_yield std::move(chunk);
        }
    }
  }

  auto name() const -> std::string override {
    return "write";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter, (void)order;
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, write_operator& x) -> bool {
    return plugin_inspect(f, x.printer_);
  }

protected:
  auto infer_type_impl(operator_type input) const
    -> caf::expected<operator_type> override {
    if (input.is<table_slice>()) {
      return tag_v<chunk_ptr>;
    }
    // TODO: Fuse this check with crtp_operator::instantiate()
    return caf::make_error(ec::type_clash,
                           fmt::format("'{}' does not accept {} as input",
                                       name(), operator_type_name(input)));
  }

private:
  std::unique_ptr<plugin_printer> printer_;
};

class write_plugin final : public virtual operator_plugin<write_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto usage = "write <printer> <args>...";
    auto docs = "https://docs.tenzir.com/operators/write";
    auto name = p.accept_shell_arg();
    if (!name) {
      diagnostic::error("expected printer name")
        .primary(p.current_span())
        .usage(usage)
        .docs(docs)
        .throw_();
    }
    auto plugin = plugins::find<printer_parser_plugin>(name->inner);
    if (!plugin) {
      throw_printer_not_found(*name);
    }
    auto printer = plugin->parse_printer(p);
    TENZIR_DIAG_ASSERT(printer);
    return std::make_unique<write_operator>(std::move(printer));
  }
};

/// The operator for saving data that will have to be joined later
/// during pipeline execution.
template <show_progress ShowProgress>
class save_operator final : public crtp_operator<save_operator<ShowProgress>> {
public:
  save_operator() = default;

  explicit save_operator(std::unique_ptr<plugin_saver> saver) noexcept
    : saver_{std::move(saver)} {
  }

  using output_type = std::conditional_t<ShowProgress == show_progress::yes,
                                         table_slice, std::monostate>;

  auto
  operator()(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> generator<output_type> {
    // TODO: Extend API to allow schema-less make_saver().
    auto new_saver = saver_->instantiate(ctrl, std::nullopt);
    if (!new_saver) {
      diagnostic::error(new_saver.error())
        .note("failed to instantiate saver")
        .emit(ctrl.diagnostics());
      co_return;
    }
    co_yield {};
    if constexpr (ShowProgress == show_progress::yes) {
      auto builder = series_builder{};
      auto bytes = uint64_t{};
      for (auto&& x : input) {
        if (not x) {
          co_yield builder.finish_assert_one_slice("tenzir.progress");
          continue;
        }
        bytes += x->size();
        auto progress = builder.record();
        progress.field("bytes", bytes);
        (*new_saver)(std::move(x));
      }
      co_yield builder.finish_assert_one_slice("tenzir.progress");
    } else {
      for (auto&& x : input) {
        if (not x) {
          co_yield {};
          continue;
        }
        (*new_saver)(std::move(x));
      }
    }
  }

  auto detached() const -> bool override {
    return true;
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto name() const -> std::string override {
    return fmt::format("internal-save-{}-progress",
                       ShowProgress == show_progress::yes ? "with" : "without");
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter, (void)order;
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, save_operator& x) -> bool {
    return plugin_inspect(f, x.saver_);
  }

protected:
  auto infer_type_impl(operator_type input) const
    -> caf::expected<operator_type> override {
    if (input.is<chunk_ptr>()) {
      if constexpr (ShowProgress == show_progress::yes) {
        return tag_v<table_slice>;
      } else {
        return tag_v<void>;
      }
    }
    // TODO: Fuse this check with crtp_operator::instantiate()
    return caf::make_error(ec::type_clash,
                           fmt::format("'{}' does not accept {} as input",
                                       name(), operator_type_name(input)));
  }

private:
  std::unique_ptr<plugin_saver> saver_;
};

auto get_saver(parser_interface& p, const char* usage, const char* docs)
  -> std::tuple<std::unique_ptr<plugin_saver>, located<std::string>,
                show_progress> {
  auto arg = p.accept_shell_arg();
  auto show_progress = show_progress::no;
  if (not arg) {
    diagnostic::error("expected saver name")
      .primary(p.current_span())
      .usage(usage)
      .docs(docs)
      .throw_();
  }
  if (arg->inner.starts_with("--")) {
    if (arg->inner != "--progress") {
      diagnostic::error("unsupported option `{}`", arg->inner)
        .primary(arg->source)
        .usage(usage)
        .docs(docs)
        .throw_();
    }
    show_progress = show_progress::yes;
    arg = p.accept_shell_arg();
    if (not arg) {
      diagnostic::error("expected saver name")
        .primary(p.current_span())
        .usage(usage)
        .docs(docs)
        .throw_();
    }
  }
  auto [saver, name, path, is_uri] = detail::resolve_saver(p, *arg);
  if (not saver) {
    throw_saver_not_found(name, is_uri);
  }
  return {
    std::move(saver),
    std::move(path),
    show_progress,
  };
}

class save_plugin final : public virtual operator_parser_plugin {
public:
  auto name() const -> std::string override {
    return "save";
  }

  auto signature() const -> operator_signature override {
    // Technically, if --progress is set, the save operator is a transformation
    // rather than a sink. However, we do not want to advertise this, as it is
    // more distracting than helpful.
    return {
      .sink = true,
    };
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto usage = "save [--progress] <saver> <args>...";
    auto docs = "https://docs.tenzir.com/operators/save";
    auto [saver, _, show_progress] = get_saver(p, usage, docs);
    TENZIR_DIAG_ASSERT(saver);
    if (show_progress == show_progress::yes) {
      return std::make_unique<save_operator<show_progress::yes>>(
        std::move(saver));
    }
    return std::make_unique<save_operator<show_progress::no>>(std::move(saver));
  }
};

/// The operator for printing and saving data without joining.
template <show_progress ShowProgress>
class write_and_save_operator final
  : public schematic_operator<
      write_and_save_operator<ShowProgress>, write_and_save_state,
      std::conditional_t<ShowProgress == show_progress::yes, table_slice,
                         std::monostate>> {
public:
  write_and_save_operator() = default;

  explicit write_and_save_operator(std::unique_ptr<plugin_printer> printer,
                                   std::unique_ptr<plugin_saver> saver) noexcept
    : printer_{std::move(printer)}, saver_{std::move(saver)} {
  }

  auto initialize(const type& schema, operator_control_plane& ctrl) const
    -> caf::expected<write_and_save_state> override {
    auto p = printer_->instantiate(schema, ctrl);
    if (not p) {
      return std::move(p.error());
    }
    auto s = saver_->instantiate(
      ctrl, printer_info{.input_schema = schema, .format = printer_->name()});
    if (not s) {
      return std::move(s.error());
    }
    return write_and_save_state{
      .printer = std::move(*p),
      .saver = std::move(*s),
    };
  }

  auto process(table_slice slice, write_and_save_state& state) const
    -> std::conditional_t<ShowProgress == show_progress::yes, table_slice,
                          std::monostate> override {
    if constexpr (ShowProgress == show_progress::yes) {
      auto builder = series_builder{};
      auto progress = builder.record();
      for (auto&& x : state.printer->process(std::move(slice))) {
        if (not x) {
          continue;
        }
        bytes += x->size();
        state.saver(std::move(x));
      }
      progress.field("bytes", bytes);
      return builder.finish_assert_one_slice("tenzir.progress");
    } else {
      for (auto&& x : state.printer->process(std::move(slice))) {
        state.saver(std::move(x));
      }
      return {};
    }
  }

  auto detached() const -> bool override {
    return true;
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto name() const -> std::string override {
    return fmt::format("internal-write-save-{}-progress",
                       ShowProgress == show_progress::yes ? "with" : "without");
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter, (void)order;
    return optimize_result{std::nullopt, event_order::schema, this->copy()};
  }

  friend auto inspect(auto& f, write_and_save_operator& x) -> bool {
    return plugin_inspect(f, x.printer_) && plugin_inspect(f, x.saver_);
  }

protected:
  auto infer_type_impl(operator_type input) const
    -> caf::expected<operator_type> override {
    if (input.is<table_slice>()) {
      if constexpr (ShowProgress == show_progress::yes) {
        return tag_v<table_slice>;
      } else {
        return tag_v<void>;
      }
    }
    // TODO: Fuse this check with crtp_operator::instantiate()
    return caf::make_error(ec::type_clash,
                           fmt::format("'{}' does not accept {} as input",
                                       name(), operator_type_name(input)));
  }

private:
  std::unique_ptr<plugin_printer> printer_;
  std::unique_ptr<plugin_saver> saver_;

  // This will anger @jachris if he sees it, but @eliaskosunen said it was okay.
  // -- @dominiklohmann, half jokingly, during a Hackathon.
  mutable uint64_t bytes = 0;
};

class to_plugin final : public virtual operator_parser_plugin {
public:
  auto name() const -> std::string override {
    return "to";
  };

  auto signature() const -> operator_signature override {
    // Technically, if --progress is set, the to operator is a transformation
    // rather than a sink. However, we do not want to advertise this, as it is
    // more distracting than helpful.
    return {
      .sink = true,
    };
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto usage
      = "to [--progress] <saver> <args>... [write <printer> <args>...]";
    auto docs = "https://docs.tenzir.com/operators/to";
    auto q = until_keyword_parser{"write", p};
    auto [saver, saver_path, show_progress] = get_saver(q, usage, docs);
    TENZIR_DIAG_ASSERT(saver);
    TENZIR_DIAG_ASSERT(q.at_end());
    auto compress = operator_ptr{};
    auto printer = std::unique_ptr<plugin_printer>{};
    if (p.at_end()) {
      std::tie(compress, printer)
        = detail::resolve_printer(saver_path, saver->default_printer());
    } else {
      compress = detail::resolve_compressor(saver_path);
      auto read = p.accept_identifier();
      TENZIR_DIAG_ASSERT(read && read->name == "write");
      auto p_name = p.accept_shell_arg();
      if (!p_name) {
        diagnostic::error("expected printer name")
          .primary(p.current_span())
          .note(usage)
          .docs(docs)
          .throw_();
      }
      auto p_plugin = plugins::find<printer_parser_plugin>(p_name->inner);
      if (!p_plugin) {
        throw_printer_not_found(*p_name);
      }
      printer = p_plugin->parse_printer(p);
      TENZIR_DIAG_ASSERT(printer);
    }
    // If the saver does not want to join different schemas, we cannot use a
    // single `write_operator` here, because its output would be joined. Thus,
    // we use `write_and_save_operator`, which does printing and saving in one
    // go. Note that it could be that `printer->allows_joining()` returns false,
    // but `saver->is_joining()` is true. The implementation of `write_operator`
    // contains the necessary check that it is only passed one single schema in
    // that case, and it otherwise aborts the execution.
    if (not saver->is_joining() && not compress) {
      if (show_progress == show_progress::yes) {
        return std::make_unique<write_and_save_operator<show_progress::yes>>(
          std::move(printer), std::move(saver));
      }
      return std::make_unique<write_and_save_operator<show_progress::no>>(
        std::move(printer), std::move(saver));
    }
    auto ops = std::vector<operator_ptr>{};
    ops.push_back(std::make_unique<write_operator>(std::move(printer)));
    if (compress)
      ops.push_back(std::move(compress));
    if (show_progress == show_progress::yes) {
      ops.push_back(
        std::make_unique<save_operator<show_progress::yes>>(std::move(saver)));
    } else {
      ops.push_back(
        std::make_unique<save_operator<show_progress::no>>(std::move(saver)));
    }
    return std::make_unique<pipeline>(std::move(ops));
  }
};

using save_plugin_with_progress
  = operator_inspection_plugin<save_operator<show_progress::yes>>;
using save_plugin_without_progress
  = operator_inspection_plugin<save_operator<show_progress::no>>;
using write_and_save_plugin_with_progress
  = operator_inspection_plugin<write_and_save_operator<show_progress::yes>>;
using write_and_save_plugin_without_progress
  = operator_inspection_plugin<write_and_save_operator<show_progress::no>>;

} // namespace

} // namespace tenzir::plugins::write_to_print_save

TENZIR_REGISTER_PLUGIN(tenzir::plugins::write_to_print_save::to_plugin)
TENZIR_REGISTER_PLUGIN(
  tenzir::plugins::write_to_print_save::save_plugin_with_progress)
TENZIR_REGISTER_PLUGIN(
  tenzir::plugins::write_to_print_save::save_plugin_without_progress)
TENZIR_REGISTER_PLUGIN(
  tenzir::plugins::write_to_print_save::write_and_save_plugin_with_progress)
TENZIR_REGISTER_PLUGIN(
  tenzir::plugins::write_to_print_save::write_and_save_plugin_without_progress)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::write_to_print_save::save_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::write_to_print_save::write_plugin)
