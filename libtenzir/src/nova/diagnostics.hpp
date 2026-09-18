//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/diagnostics.hpp"
#include "tenzir/option.hpp"

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

namespace tenzir::nova::_ {

/// One diagnostic route for preparation, including nested constant evaluation.
class DiagnosticHandler final : public diagnostic_handler {
public:
  explicit DiagnosticHandler(diagnostic_handler& inner) : inner_{inner} {
  }

  auto emit(diagnostic d) -> void override {
    if (d.severity == severity::error) {
      ++errors_;
      if (metadata_) {
        for (auto const& note : *metadata_) {
          if (std::ranges::find(d.notes, note.kind, &diagnostic_note::kind)
              == d.notes.end()) {
            d.notes.push_back(note);
          }
        }
      }
    }
    inner_.emit(std::move(d));
  }

private:
  friend class DiagnosticScope;

  diagnostic_handler& inner_;
  size_t errors_ = 0;
  Option<std::vector<diagnostic_note>> metadata_;
};

/// Nested operations reuse the handler and checkpoint its error count.
/// Function metadata is restored when leaving a call, including on failure.
class DiagnosticScope {
public:
  explicit DiagnosticScope(diagnostic_handler& dh)
    : handler_{dynamic_cast<DiagnosticHandler*>(&dh)} {
    if (not handler_) {
      owned_.emplace(dh);
      handler_ = &*owned_;
    }
    errors_ = handler_->errors_;
  }

  /// Like above, but errors emitted within the scope additionally carry the
  /// given usage and documentation notes, unless they already have them.
  DiagnosticScope(diagnostic_handler& dh, std::string usage, std::string docs)
    : DiagnosticScope{dh} {
    auto notes = std::vector<diagnostic_note>{};
    if (not usage.empty()) {
      notes.emplace_back(diagnostic_note_kind::usage, std::move(usage));
    }
    if (not docs.empty()) {
      notes.emplace_back(diagnostic_note_kind::docs, std::move(docs));
    }
    metadata_ = std::exchange(handler_->metadata_, std::move(notes));
    restore_metadata_ = true;
  }

  DiagnosticScope(DiagnosticScope const&) = delete;
  DiagnosticScope(DiagnosticScope&&) = delete;
  auto operator=(DiagnosticScope const&) -> DiagnosticScope& = delete;
  auto operator=(DiagnosticScope&&) -> DiagnosticScope& = delete;

  ~DiagnosticScope() {
    if (restore_metadata_) {
      handler_->metadata_ = std::move(metadata_);
    }
  }

  auto handler() const -> diagnostic_handler& {
    return *handler_;
  }

  auto failed() const -> bool {
    return handler_->errors_ != errors_;
  }

private:
  Option<DiagnosticHandler> owned_;
  DiagnosticHandler* handler_;
  size_t errors_ = 0;
  Option<std::vector<diagnostic_note>> metadata_;
  bool restore_metadata_ = false;
};

} // namespace tenzir::nova::_
