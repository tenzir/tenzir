//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/data.hpp>
#include <tenzir/make_byte_reader.hpp>
#include <tenzir/parser_interface.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/to_lines.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::bitz {
namespace {

static constexpr auto BITZ_MAGIC = std::array<char, 4>{'T', 'N', 'Z', '1'};

class bitz_parser final : public plugin_parser {
public:
  bitz_parser() = default;

  auto name() const -> std::string override {
    return "bitz";
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    return std::invoke(
      [](auto byte_reader,
         operator_control_plane& ctrl) -> generator<table_slice> {
        while (true) {
          auto magic = byte_reader(BITZ_MAGIC.size());
          while (not magic) {
            co_yield {};
            magic = byte_reader(BITZ_MAGIC.size());
          }
          if (magic->size() < BITZ_MAGIC.size()) {
            if (magic->size() != 0) {
              diagnostic::error("unexpected BITZ magic length {}",
                                magic->size())
                .note("expected {}", BITZ_MAGIC.size())
                .emit(ctrl.diagnostics());
            }
            co_return;
          }
          if (std::memcmp(magic->data(), BITZ_MAGIC.data(), BITZ_MAGIC.size())
              != 0) {
            diagnostic::error("unexpected BITZ magic")
              .note("expected {}",
                    std::string_view{BITZ_MAGIC.data(), BITZ_MAGIC.size()})
              .emit(ctrl.diagnostics());
          }
          auto header = byte_reader(sizeof(uint64_t));
          while (not header) {
            co_yield {};
            header = byte_reader(sizeof(uint64_t));
          }
          if (header->size() < sizeof(uint64_t)) {
            if (header->size() != 0) {
              diagnostic::error("unexpected BITZ header length {}",
                                header->size())
                .note("expected {}", sizeof(uint64_t))
                .emit(ctrl.diagnostics());
            }
            co_return;
          }
          auto message_length = uint64_t{};
          std::memcpy(&message_length, header->data(), sizeof(uint64_t));
          message_length = detail::to_host_order(message_length);
          auto message = byte_reader(message_length);
          while (not message) {
            co_yield {};
            message = byte_reader(message_length);
          }
          if (message->size() < message_length) {
            diagnostic::error("unexpected message length {}", message->size())
              .note("expected {}", message_length)
              .emit(ctrl.diagnostics());
            co_return;
          }
          auto parser
            = check(pipeline::internal_parse_as_operator("read feather"));
          TENZIR_ASSERT(parser);
          auto untyped_instance = check(parser->instantiate(
            [](auto chunk) -> generator<chunk_ptr> {
              co_yield std::move(chunk);
            }(std::move(message)),
            ctrl));
          auto* instance
            = std::get_if<generator<table_slice>>(&untyped_instance);
          TENZIR_ASSERT(instance);
          while (auto result = instance->next()) {
            if (size(*result) == 0) {
              continue;
            }
            co_yield std::move(*result);
          }
        }
      },
      make_byte_reader(std::move(input)), ctrl);
  }

  friend auto inspect(auto& f, bitz_parser& x) -> bool {
    return f.object(x).fields();
  }
};

class bitz_printer final : public plugin_printer {
public:
  bitz_printer() = default;

  auto name() const -> std::string override {
    return "bitz";
  }

  auto instantiate(type input_schema, operator_control_plane& ctrl) const
    -> caf::expected<std::unique_ptr<printer_instance>> override {
    (void)input_schema;
    return printer_instance::make(
      // NOLINTNEXTLINE(cppcoreguidelines-avoid-capturing-lambda-coroutines)
      [&ctrl](table_slice slice) -> generator<chunk_ptr> {
        if (slice.rows() == 0) {
          co_yield {};
          co_return;
        }
        auto printer
          = check(pipeline::internal_parse_as_operator("write feather"));
        TENZIR_ASSERT(printer);
        auto untyped_instance = check(printer->instantiate(
          [](auto slice) -> generator<table_slice> {
            co_yield std::move(slice);
          }(std::move(slice)),
          ctrl));
        auto* instance = std::get_if<generator<chunk_ptr>>(&untyped_instance);
        TENZIR_ASSERT(instance);
        auto total_size = uint64_t{0};
        auto results = std::vector<chunk_ptr>{};
        while (auto result = instance->next()) {
          const auto chunk_size = size(*result);
          if (chunk_size == 0) {
            continue;
          }
          total_size += size(*result);
          results.push_back(std::move(*result));
        }
        TENZIR_ASSERT(not results.empty());
        TENZIR_ASSERT(total_size > 0);
        total_size = detail::to_network_order(total_size);
        co_yield chunk::copy(BITZ_MAGIC.data(), BITZ_MAGIC.size());
        co_yield chunk::copy(&total_size, sizeof(total_size));
        for (auto&& result : results) {
          co_yield std::move(result);
        }
      });
  }

  auto allows_joining() const -> bool override {
    return true;
  };

  auto prints_utf8() const -> bool override {
    return false;
  }

  friend auto inspect(auto& f, bitz_printer& x) -> bool {
    return f.object(x).fields();
  }
};

class plugin final : public virtual parser_plugin<bitz_parser>,
                     public virtual printer_plugin<bitz_printer> {
  auto name() const -> std::string override {
    return "bitz";
  }

  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{"bitz", "https://docs.tenzir.com/"
                                          "formats/bitz"};
    parser.parse(p);
    return std::make_unique<bitz_parser>();
  }

  auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer> override {
    auto parser = argument_parser{"bitz", "https://docs.tenzir.com/"
                                          "formats/bitz"};
    parser.parse(p);
    return std::make_unique<bitz_printer>();
  }
};

class read_bitz_plugin final : public virtual operator_factory_plugin {
  auto name() const -> std::string override {
    return "read_bitz";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TENZIR_UNUSED(inv, ctx);
    return check(pipeline::internal_parse_as_operator("read bitz"));
  }
};

class write_bitz_plugin final : public virtual operator_factory_plugin {
  auto name() const -> std::string override {
    return "write_bitz";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TENZIR_UNUSED(inv, ctx);
    return check(pipeline::internal_parse_as_operator("write bitz"));
  }
};

} // namespace
} // namespace tenzir::plugins::bitz

TENZIR_REGISTER_PLUGIN(tenzir::plugins::bitz::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::bitz::read_bitz_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::bitz::write_bitz_plugin)
