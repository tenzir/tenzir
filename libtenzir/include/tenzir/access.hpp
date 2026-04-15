//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

namespace tenzir {

/// Wrapper to encapsulate the implementation of concepts requiring access to
/// private state.
struct access {
  template <class>
  struct state;

  template <class>
  struct parser_base;

  template <class>
  struct printer;
};

template <class T>
concept access_state = requires { access::state<T>{}; };

template <class T>
concept access_parser = requires { access::parser_base<T>{}; };

template <class T>
concept access_printer = requires { access::printer<T>{}; };

} // namespace tenzir
