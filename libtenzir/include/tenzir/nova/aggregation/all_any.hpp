//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/nova/aggregation.hpp>

#include <concepts>

namespace tenzir::plugins::nova_all_any {

struct AllAnyArgs {
  nova::ValueArgument x;
};

/// The fold of `all` (`All = true`) and `any` (`All = false`) over `bool`s.
/// A `null` makes the result `null` unless another value decides it, and any
/// other type warns and makes it `null` for good.
template <bool All>
class AllAny {
public:
  /// Folds the values of `bits` at `rows`.
  auto add(nova::storage::BitMap const& bits, nova::storage::BitMap const& rows)
    -> void {
    if (failed_) {
      return;
    }
    if constexpr (All) {
      result_ = result_ and not rows.and_not(bits).any();
    } else {
      result_ = result_ or (rows & bits).any();
    }
  }

  template <class Tag>
  auto add(nova::RowView<Tag> value, location source, diagnostic_handler& dh)
    -> void {
    if constexpr (std::same_as<Tag, nova::Bool>) {
      if (not failed_) {
        result_ = All ? result_ and *value : result_ or *value;
      }
    } else if constexpr (std::same_as<Tag, nova::Null>) {
      add_null();
    } else {
      add_invalid(nova::Type<Tag>::static_name, source, dh);
    }
  }

  auto add_null() -> void {
    nulled_ = true;
  }

  auto add_invalid(std::string_view type_name, location source,
                   diagnostic_handler& dh) -> void {
    if (failed_) {
      return;
    }
    diagnostic::warning("expected `bool`, got `{}`", type_name)
      .primary(source)
      .emit(dh);
    failed_ = true;
  }

  auto failed() const -> bool {
    return failed_;
  }

  auto get() const -> nova::Data {
    if (failed_) {
      return nova::Data{};
    }
    // A `null` could have been either value, so it only matters if the
    // others did not decide the result.
    if (nulled_ and result_ == All) {
      return nova::Data{};
    }
    return nova::Data{result_};
  }

private:
  bool result_ = All;
  bool nulled_ = false;
  bool failed_ = false;
};

template <bool All>
class AllAnyFunction final {
public:
  static auto eval(AllAnyArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    return nova::aggregate_lists(args.x, frame,
                                 [&](nova::ListElements const& elements,
                                     nova::ArrayBuilder<nova::Data>& builder) {
                                   auto state = AllAny<All>{};
                                   elements.for_each([&](auto value) {
                                     state.add(value, args.x.source, frame);
                                   });
                                   nova::append_data(builder, state.get());
                                 });
  }

  auto update(AllAnyArgs const& args, nova::EvalFrame frame) -> void {
    auto const add = [&]<class Tag>(nova::Array<Tag> const& array,
                                    nova::storage::BitMap const& rows) {
      if constexpr (std::same_as<Tag, nova::Bool>) {
        state_.add(as<nova::storage::BitMap>(array.storage()), rows);
      } else if constexpr (std::same_as<Tag, nova::Null>) {
        state_.add_null();
      } else {
        state_.add_invalid(nova::Type<Tag>::static_name, args.x.source, frame);
      }
    };
    match(
      args.x.data,
      [&]<nova::data_type Tag>(nova::Array<Tag> const& array) {
        add(array, frame.mask());
      },
      [&](nova::UnionArray const& u) {
        for (auto const& field : u.fields()) {
          auto const rows = frame.mask() & field.present;
          if (rows.any()) {
            match(field.data,
                  [&]<nova::data_type Tag>(nova::Array<Tag> const& array) {
                    add(array, rows);
                  });
          }
        }
      });
  }

  auto get() const -> nova::Data {
    return state_.get();
  }

  auto reset() -> void {
    state_ = {};
  }

private:
  AllAny<All> state_;
};

} // namespace tenzir::plugins::nova_all_any
