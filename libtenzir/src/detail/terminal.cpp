//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/terminal.hpp"

#include "tenzir/detail/posix.hpp"
#include "tenzir/error.hpp"
#include "tenzir/logger.hpp"

#include <caf/expected.hpp>
#include <sys/select.h>

#include <cstdio>
#include <cstdlib>
#include <termios.h>
#include <unistd.h>

namespace tenzir {
namespace detail {
namespace terminal {

namespace {

bool initialized = false;
struct termios backup;
struct termios current;

void restore() {
  if (initialized) {
    tcsetattr(::fileno(stdin), TCSANOW, &backup);
  }
}

bool initialize() {
  if (not ::isatty(::fileno(stdin))) {
    return false;
  }
  std::atexit(&restore);
  if (initialized) {
    return false;
  }
  if (tcgetattr(0, &current) < 0 or tcgetattr(0, &backup) < 0) {
    return false;
  }
  initialized = true;
  return true;
}

} // namespace

unbufferer::unbufferer() {
  unbuffer();
}

unbufferer::~unbufferer() {
  buffer();
}

bool unbuffer() {
  if (not(initialized or initialize())) {
    return false;
  }
  current.c_lflag &= ~(ICANON | ECHO);
  current.c_cc[VMIN] = 1;
  current.c_cc[VTIME] = 0;
  return true;
}

bool buffer() {
  if (not(initialized or initialize())) {
    return false;
  }
  current.c_lflag |= ICANON | ECHO;
  current.c_cc[VMIN] = backup.c_cc[VMIN];
  current.c_cc[VTIME] = backup.c_cc[VTIME];
  return tcsetattr(::fileno(stdin), TCSANOW, &current) < 0;
}

bool disable_echo() {
  if (not(initialized or initialize())) {
    return false;
  }
  current.c_lflag &= ~ECHO;
  return tcsetattr(::fileno(stdin), TCSANOW, &current) < 0;
}

bool enable_echo() {
  if (not(initialized or initialize())) {
    return false;
  }
  current.c_lflag |= ECHO;
  return tcsetattr(::fileno(stdin), TCSANOW, &current) < 0;
}

bool get(char& c, int timeout) {
  auto ready = rpoll(::fileno(stdin), timeout);
  if (not ready) {
    TENZIR_ERROR("{} {}", __func__, render(ready.error()));
    return false;
  }
  if (not *ready) {
    return false;
  }
  auto i = ::fgetc(stdin);
  if (::feof(stdin)) {
    return false;
  }
  c = static_cast<char>(i);
  return true;
}

} // namespace terminal
} // namespace detail
} // namespace tenzir
