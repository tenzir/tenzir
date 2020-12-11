/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

// This file comes from a 3rd party and has been adapted to fit into the VAST
// code base. Details about the original file:
//
// - Repository:       https://github.com/facebook/folly
// - Commit:           d2c64d94c7e892925a02a080c886ab3df3f5c937
// - Copyright Holder: Facebook, Inc. and its affiliates.
// - Path:             tracing/StaticTracepoint.h
//                     tracing/StaticTracepoint-ELFx86.h
// - Created:          Nov 23, 2016
// - License:          Apache 2.0
//
// (note that a very similar implementation also exists under CC0 license in
//  the sys/sdt.h header from systemtap-sdt-dev)
//
// Notable changes from the upstream version:
//  * Add a written overview to the top of the file.
//  * Add `VAST_TRACEPOINT()` convenience macro.
//  * Use `VAST_DISABLE_SDT` macro instead of `FOLLY_DISABLE_SDT` to disable
//    this feature.
//  * Add pragmas to ignore warnings for GNU extensions when using clang.

#pragma once

// # Overview
//
// A USDT (userspace statically-defined tracepoint) is a code instrumentation
// mechanism provided by the kernel to allow tracing software to measure
// and account specific developer-defined events in user space code and
// libraries. Historically, the idea originated with DTrace tool in Solaris
// and was adapted for the linux kernel around 2015.
//
// On a high level, it works by inserting interrupts at specific points in the
// code to jump to a kernel handler, which generates trace events, optionally
// records some context, and asynchronously forwards these events to tracing
// programs like `perf` or `bpftrace`.
//
// The main entry point for users is the `VAST_TRACEPOINT()` macro defined at
// the bottom of this file.
//
//
// # Inner Workings
//
// In the code path itself, a single additional `nop` instruction is generated
// at the place where the macro is invoked. If the USDT has additional
// arguments, additional code is generated to move all arguments into registers.
//
// Additionally, an additional section called "stapsdt" is embedded into the
// generated ELF file (all of this is linux-only). This section records the
// location of the `nop` byte as well as the name and the number of arguments
// of the tracepoint it belongs to.
//
// When *enabling* a trace point, the byte is replaced by an `int3` instruction,
// ie. an interrupt that gives control back to the kernel. (note that debugger
// breakpoints are implemented using the same technique) This can happen either
// live for a specific running process, or on the file containing the USDT. In
// the latter case, the kernel will do the replacement whenever the file is
// loaded into memory for execution.
//
// The kernel has a mapping of which instruction address corresponds to which
// trace point, so on the interrupt code path it can update the statistics,
// gather arguments from user space or even run attached BCC programs or collect
// data from userspace.
//
// To enable a USDT, one can either use the raw kernel API at
// `/sys/kernel/debug/tracing/uprobe_events` or more conveniently with a
// command like `perf probe`.
//
//
// # Related Links
//
// https://www.kernel.org/doc/Documentation/trace/uprobetracer.txt
// https://leezhenghui.github.io/linux/2019/03/05/exploring-usdt-on-linux.html

// clang-format off
#include <cstddef>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wgnu-zero-variadic-macro-arguments"
#endif // __clang__

// Default constraint for the probe arguments as operands.
#ifndef FOLLY_SDT_ARG_CONSTRAINT
#define FOLLY_SDT_ARG_CONSTRAINT      "nor"
#endif

// Instruction to emit for the probe.
#define FOLLY_SDT_NOP                 nop

// Note section properties.
#define FOLLY_SDT_NOTE_NAME           "stapsdt"
#define FOLLY_SDT_NOTE_TYPE           3

// Semaphore variables are put in this section
#define FOLLY_SDT_SEMAPHORE_SECTION   ".probes"

// Size of address depending on platform.
#ifdef __LP64__
#define FOLLY_SDT_ASM_ADDR            .8byte
#else
#define FOLLY_SDT_ASM_ADDR            .4byte
#endif

// Assembler helper Macros.
#define FOLLY_SDT_S(x)                #x
#define FOLLY_SDT_ASM_1(x)            FOLLY_SDT_S(x) "\n"
#define FOLLY_SDT_ASM_2(a, b)         FOLLY_SDT_S(a) "," FOLLY_SDT_S(b) "\n"
#define FOLLY_SDT_ASM_3(a, b, c)      FOLLY_SDT_S(a) "," FOLLY_SDT_S(b) ","    \
                                      FOLLY_SDT_S(c) "\n"
#define FOLLY_SDT_ASM_STRING(x)       FOLLY_SDT_ASM_1(.asciz FOLLY_SDT_S(x))

// Helper to determine the size of an argument.
#define FOLLY_SDT_IS_ARRAY_POINTER(x)  ((__builtin_classify_type(x) == 14) ||  \
                                        (__builtin_classify_type(x) == 5))
#define FOLLY_SDT_ARGSIZE(x)  (FOLLY_SDT_IS_ARRAY_POINTER(x)                   \
                               ? sizeof(void*)                                 \
                               : sizeof(x))

// Format of each probe arguments as operand.
// Size of the arugment tagged with FOLLY_SDT_Sn, with "n" constraint.
// Value of the argument tagged with FOLLY_SDT_An, with configured constraint.
#define FOLLY_SDT_ARG(n, x)                                                    \
  [FOLLY_SDT_S##n] "n"                ((size_t)FOLLY_SDT_ARGSIZE(x)),          \
  [FOLLY_SDT_A##n] FOLLY_SDT_ARG_CONSTRAINT (x)

// Templates to append arguments as operands.
#define FOLLY_SDT_OPERANDS_0()        [__sdt_dummy] "g" (0)
#define FOLLY_SDT_OPERANDS_1(_1)      FOLLY_SDT_ARG(1, _1)
#define FOLLY_SDT_OPERANDS_2(_1, _2)                                           \
  FOLLY_SDT_OPERANDS_1(_1), FOLLY_SDT_ARG(2, _2)
#define FOLLY_SDT_OPERANDS_3(_1, _2, _3)                                       \
  FOLLY_SDT_OPERANDS_2(_1, _2), FOLLY_SDT_ARG(3, _3)
#define FOLLY_SDT_OPERANDS_4(_1, _2, _3, _4)                                   \
  FOLLY_SDT_OPERANDS_3(_1, _2, _3), FOLLY_SDT_ARG(4, _4)
#define FOLLY_SDT_OPERANDS_5(_1, _2, _3, _4, _5)                               \
  FOLLY_SDT_OPERANDS_4(_1, _2, _3, _4), FOLLY_SDT_ARG(5, _5)
#define FOLLY_SDT_OPERANDS_6(_1, _2, _3, _4, _5, _6)                           \
  FOLLY_SDT_OPERANDS_5(_1, _2, _3, _4, _5), FOLLY_SDT_ARG(6, _6)
#define FOLLY_SDT_OPERANDS_7(_1, _2, _3, _4, _5, _6, _7)                       \
  FOLLY_SDT_OPERANDS_6(_1, _2, _3, _4, _5, _6), FOLLY_SDT_ARG(7, _7)
#define FOLLY_SDT_OPERANDS_8(_1, _2, _3, _4, _5, _6, _7, _8)                   \
  FOLLY_SDT_OPERANDS_7(_1, _2, _3, _4, _5, _6, _7), FOLLY_SDT_ARG(8, _8)
#define FOLLY_SDT_OPERANDS_9(_1, _2, _3, _4, _5, _6, _7, _8, _9)               \
  FOLLY_SDT_OPERANDS_8(_1, _2, _3, _4, _5, _6, _7, _8), FOLLY_SDT_ARG(9, _9)

// Templates to reference the arguments from operands in note section.
#define FOLLY_SDT_ARGFMT(no)        %n[FOLLY_SDT_S##no]@%[FOLLY_SDT_A##no]
#define FOLLY_SDT_ARG_TEMPLATE_0    /*No arguments*/
#define FOLLY_SDT_ARG_TEMPLATE_1    FOLLY_SDT_ARGFMT(1)
#define FOLLY_SDT_ARG_TEMPLATE_2    FOLLY_SDT_ARG_TEMPLATE_1 FOLLY_SDT_ARGFMT(2)
#define FOLLY_SDT_ARG_TEMPLATE_3    FOLLY_SDT_ARG_TEMPLATE_2 FOLLY_SDT_ARGFMT(3)
#define FOLLY_SDT_ARG_TEMPLATE_4    FOLLY_SDT_ARG_TEMPLATE_3 FOLLY_SDT_ARGFMT(4)
#define FOLLY_SDT_ARG_TEMPLATE_5    FOLLY_SDT_ARG_TEMPLATE_4 FOLLY_SDT_ARGFMT(5)
#define FOLLY_SDT_ARG_TEMPLATE_6    FOLLY_SDT_ARG_TEMPLATE_5 FOLLY_SDT_ARGFMT(6)
#define FOLLY_SDT_ARG_TEMPLATE_7    FOLLY_SDT_ARG_TEMPLATE_6 FOLLY_SDT_ARGFMT(7)
#define FOLLY_SDT_ARG_TEMPLATE_8    FOLLY_SDT_ARG_TEMPLATE_7 FOLLY_SDT_ARGFMT(8)
#define FOLLY_SDT_ARG_TEMPLATE_9    FOLLY_SDT_ARG_TEMPLATE_8 FOLLY_SDT_ARGFMT(9)

// Semaphore define, declare and probe note format

#define FOLLY_SDT_SEMAPHORE(provider, name)                                    \
  folly_sdt_semaphore_##provider##_##name

#define FOLLY_SDT_DEFINE_SEMAPHORE(provider, name)                             \
  extern "C" {                                                                 \
    volatile unsigned short FOLLY_SDT_SEMAPHORE(provider, name)                \
    __attribute__((section(FOLLY_SDT_SEMAPHORE_SECTION), used)) = 0;           \
  }

#define FOLLY_SDT_DECLARE_SEMAPHORE(provider, name)                            \
  extern "C" volatile unsigned short FOLLY_SDT_SEMAPHORE(provider, name)

#define FOLLY_SDT_SEMAPHORE_NOTE_0(provider, name)                             \
  FOLLY_SDT_ASM_1(     FOLLY_SDT_ASM_ADDR 0) /*No Semaphore*/                  \

#define FOLLY_SDT_SEMAPHORE_NOTE_1(provider, name)                             \
  FOLLY_SDT_ASM_1(FOLLY_SDT_ASM_ADDR FOLLY_SDT_SEMAPHORE(provider, name))

// Structure of note section for the probe.
#define FOLLY_SDT_NOTE_CONTENT(provider, name, has_semaphore, arg_template)    \
  FOLLY_SDT_ASM_1(990: FOLLY_SDT_NOP)                                          \
  FOLLY_SDT_ASM_3(     .pushsection .note.stapsdt,"","note")                   \
  FOLLY_SDT_ASM_1(     .balign 4)                                              \
  FOLLY_SDT_ASM_3(     .4byte 992f-991f, 994f-993f, FOLLY_SDT_NOTE_TYPE)       \
  FOLLY_SDT_ASM_1(991: .asciz FOLLY_SDT_NOTE_NAME)                             \
  FOLLY_SDT_ASM_1(992: .balign 4)                                              \
  FOLLY_SDT_ASM_1(993: FOLLY_SDT_ASM_ADDR 990b)                                \
  FOLLY_SDT_ASM_1(     FOLLY_SDT_ASM_ADDR 0) /*Reserved for Base Address*/     \
  FOLLY_SDT_SEMAPHORE_NOTE_##has_semaphore(provider, name)                     \
  FOLLY_SDT_ASM_STRING(provider)                                               \
  FOLLY_SDT_ASM_STRING(name)                                                   \
  FOLLY_SDT_ASM_STRING(arg_template)                                           \
  FOLLY_SDT_ASM_1(994: .balign 4)                                              \
  FOLLY_SDT_ASM_1(     .popsection)

// Main probe Macro.
#define FOLLY_SDT_PROBE(provider, name, has_semaphore, n, arglist)             \
    __asm__ __volatile__ (                                                     \
      FOLLY_SDT_NOTE_CONTENT(                                                  \
        provider, name, has_semaphore, FOLLY_SDT_ARG_TEMPLATE_##n)             \
      :: FOLLY_SDT_OPERANDS_##n arglist                                        \
    )                                                                          \

// Helper Macros to handle variadic arguments.
#define FOLLY_SDT_NARG_(_0, _1, _2, _3, _4, _5, _6, _7, _8, _9, N, ...) N
#define FOLLY_SDT_NARG(...)                                                    \
  FOLLY_SDT_NARG_(__VA_ARGS__, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
#define FOLLY_SDT_PROBE_N(provider, name, has_semaphore, N, ...)               \
  FOLLY_SDT_PROBE(provider, name, has_semaphore, N, (__VA_ARGS__))

#define FOLLY_SDT(provider, name, ...) \
  FOLLY_SDT_PROBE_N(                   \
      provider, name, 0, FOLLY_SDT_NARG(0, ##__VA_ARGS__), ##__VA_ARGS__)
// Use FOLLY_SDT_DEFINE_SEMAPHORE(provider, name) to define the semaphore
// as global variable before using the FOLLY_SDT_WITH_SEMAPHORE macro
#define FOLLY_SDT_WITH_SEMAPHORE(provider, name, ...) \
  FOLLY_SDT_PROBE_N(                                  \
      provider, name, 1, FOLLY_SDT_NARG(0, ##__VA_ARGS__), ##__VA_ARGS__)
#define FOLLY_SDT_IS_ENABLED(provider, name) \
  (FOLLY_SDT_SEMAPHORE(provider, name) > 0)

#ifdef __clang__
#pragma clang diagnostic pop
#endif // __clang__

// -----------------------------------------------------------------------------


#if defined(__ELF__) && (defined(__x86_64__) || defined(__i386__)) && \
    !VAST_DISABLE_SDT

/// Defines a USDT trace point for provider 'vast' with given parameters.
/// @param name The name of the trace point. Different tracing tools use
///             different naming conventions on how to refer to a USDT that
///             was creating using the invocation `VAST_TRACEPOINT(foo)`:
///
///               perf probe:        `sdt_vast:foo` or `%foo`
///               bpftrace:          `usdt:/path/to/libvast.so:vast:foo`
///               bpftrace (<= 0.8): `usdt:/path/to/libvast.so:foo`
///               bcc:               `USDT("/path/to/libvast.so")
///                                     .enable_probe("foo", "foo_handler")`
///
/// @param args Further arguments. These must be "simple" arguments like
///             integers or pointers, and no more than the number of available
///             registers.
#define VAST_TRACEPOINT(name, ...) \
  FOLLY_SDT(vast, name, __VA_ARGS__)

// NOTE: There is a mechanism called a "USDT semaphore" that can be used to
// allow applications to know whether a given tracepoint is being monitored or
// not, for example to allow preparation of expensive tracepoint arguments.
//
// This works by adding an additional section called ".probes" into the ELF
// file, that contains space for one number per tracepoint, initialized to 0.
// Every tracer that monitors a given tracepoint is supposed to increase that
// number by one, and to decrease it again when it is finished.
//
// Note that USDTs with semaphores can only be enabled at runtime and not at
// file level, since the semaphore count will be specific to the running
// process. Due to this, we currently don't wrap the API, however when a use
// case arises the following macros can be used as needed:
//
// #define VAST_SDT_WITH_SEMAPHORE(name)
//   (vast, name, __VA_ARGS__)
//
// #define VAST_SDT_IS_ENABLED(name)
//   (FOLLY_SDT_SEMAPHORE(vast, name) > 0)

#else

#include "vast/detail/discard.hpp"

#define VAST_TRACEPOINT(name, ...) \
  VAST_DISCARD_ARGS(__VA_ARGS__)

#endif
