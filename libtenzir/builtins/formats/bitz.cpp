//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// The BITZ format is a size-prefixed dump of Tenzir's wire format as laid out
// in the tenzir.fbs.FlatTableSlice FlatBuffers table. The size prefix occupies
// 64 bit and is stored in network byte order.

#include <tenzir/argument_parser.hpp>
#include <tenzir/data.hpp>
#include <tenzir/make_byte_reader.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/to_lines.hpp>

namespace tenzir::plugins::bitz {
namespace {

class bitz_parser final : public plugin_parser {
public:
  bitz_parser() = default;

  auto name() const -> std::string override {
    return "bitz";
  }

  auto instantiate(generator<chunk_ptr> input, exec_ctx ctx) const
    -> std::optional<generator<table_slice>> override {
    return std::invoke(
      [](auto byte_reader, exec_ctx ctx) -> generator<table_slice> {
        while (true) {
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
          auto deserializer = caf::binary_deserializer{nullptr, *message};
          auto result = table_slice{};
          const auto ok = deserializer.apply(result);
          if (not ok) [[unlikely]] {
            diagnostic::warning("failed to deserialize BITZ message")
              .emit(ctrl.diagnostics());
          }
          co_yield std::move(result);
        }
      },
      make_byte_view_reader(std::move(input)), ctrl);
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

  auto instantiate(type input_schema, exec_ctx ctx) const
    -> caf::expected<std::unique_ptr<printer_instance>> override {
    (void)input_schema;
    (void)ctrl;
    return printer_instance::make(
      [&ctrl](table_slice slice) -> generator<chunk_ptr> {
        if (slice.rows() == 0) {
          co_yield {};
          co_return;
        }
        auto buffer = caf::byte_buffer{};
        buffer.resize(sizeof(uint64_t));
        auto serializer = caf::binary_serializer{nullptr, buffer};
        const auto ok = serializer.apply(slice);
        if (not ok) [[unlikely]] {
          diagnostic::warning("failed to serialize BITZ message")
            .note("skipping {} events with schema {}", slice.rows(),
                  slice.schema())
            .emit(ctrl.diagnostics());
          co_return;
        }
        const auto size = detail::to_network_order(
          detail::narrow_cast<uint64_t>(buffer.size() - sizeof(uint64_t)));
        TENZIR_ASSERT(size > 0);
        std::memcpy(buffer.data(), &size, sizeof(uint64_t));
        co_yield chunk::make(std::move(buffer));
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

} // namespace
} // namespace tenzir::plugins::bitz

TENZIR_REGISTER_PLUGIN(tenzir::plugins::bitz::plugin)
