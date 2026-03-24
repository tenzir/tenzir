//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

// This is just a temporary debugging solution.
#define TENZIR_DEBUG_ASYNC 1
#if TENZIR_DEBUG_ASYNC
#  define LOGV(...) TENZIR_VERBOSE(__VA_ARGS__)
#  define LOGD(...) TENZIR_DEBUG(__VA_ARGS__)
#  define LOGI(...) TENZIR_INFO(__VA_ARGS__)
#  define LOGW(...) TENZIR_WARN(__VA_ARGS__)
#  define LOGE(...) TENZIR_ERROR(__VA_ARGS__)
#else
#  define LOGV(...)
#  define LOGD(...)
#  define LOGI(...)
#  define LOGW(...)
#  define LOGE(...)
#endif
