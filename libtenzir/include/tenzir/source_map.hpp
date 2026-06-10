//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/location.hpp"

#include <span>
#include <string>
#include <vector>

namespace tenzir {

/// Identifies an entry in `SourceMap::sources()`.
///
/// Matches `location::source_index`, where `0` refers to the main source.
using SourceId = uint32_t;

/// Identifies an entry in `SourceMap::call_sites()`.
///
/// Matches `location::callsite_index`, where `0` means top-level. Hence,
/// valid call site ids start at `1`.
using CallSiteId = uint32_t;

/// Maps locations in compiled IR back to their originating sources.
///
/// The source map is populated during compilation from AST to IR. It records
/// all sources that took part in the compilation, as well as all call sites
/// of user-defined operators whose bodies were expanded into the result.
class SourceMap {
public:
  /// A TQL source text that was used during compilation.
  struct Source {
    /// The TQL source text.
    std::string text;

    /// A description of where the text comes from, e.g., `<input>`, a config
    /// file path, or a package name.
    std::string origin;

    friend auto inspect(auto& f, Source& x) -> bool {
      return f.object(x).pretty_name("source").fields(
        f.field("text", x.text), f.field("origin", x.origin));
    }
  };

  /// A call to a user-defined operator encountered during compilation.
  struct CallSite {
    /// The location of the invocation. Its `source_index` identifies the
    /// caller's source, and its `callsite_index` the parent call site for
    /// nested calls (`0` means top-level).
    location call;

    /// The source containing the definition of the called operator.
    SourceId definition{};

    /// The name of the invoked user-defined operator.
    std::string operator_name;

    friend auto inspect(auto& f, CallSite& x) -> bool {
      return f.object(x)
        .pretty_name("call_site")
        .fields(f.field("call", x.call), f.field("definition", x.definition),
                f.field("operator_name", x.operator_name));
    }
  };

  /// Register a source and return its id.
  ///
  /// The main source is expected to be registered first, such that its id
  /// matches `location::source_index == 0`.
  auto add_source(Source source) -> SourceId {
    sources_.push_back(std::move(source));
    return detail::narrow_cast<SourceId>(sources_.size() - 1);
  }

  /// Register a call site and return its id.
  auto add_call_site(CallSite call_site) -> CallSiteId {
    call_sites_.push_back(std::move(call_site));
    return detail::narrow_cast<CallSiteId>(call_sites_.size());
  }

  /// Return the source for the given id.
  auto source(SourceId id) const -> const Source& {
    TENZIR_ASSERT(id < sources_.size());
    return sources_[id];
  }

  /// Return the call site for the given id, which must not be `0`.
  auto call_site(CallSiteId id) const -> const CallSite& {
    TENZIR_ASSERT(id >= 1);
    TENZIR_ASSERT(id <= call_sites_.size());
    return call_sites_[id - 1];
  }

  /// Return all registered sources.
  auto sources() const -> std::span<const Source> {
    return sources_;
  }

  /// Return all registered call sites.
  auto call_sites() const -> std::span<const CallSite> {
    return call_sites_;
  }

  friend auto inspect(auto& f, SourceMap& x) -> bool {
    return f.object(x)
      .pretty_name("source_map")
      .fields(f.field("sources", x.sources_),
              f.field("call_sites", x.call_sites_));
  }

private:
  std::vector<Source> sources_;
  std::vector<CallSite> call_sites_;
};

} // namespace tenzir
