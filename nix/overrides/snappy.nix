{
  lib,
  stdenv,
  snappy,
}:
snappy.overrideAttrs (
  orig:
  lib.optionalAttrs stdenv.hostPlatform.isStatic {
    # `libsnappy.a` carries `snappy-c.cc.o`, which exports the upstream C API
    # (`snappy_compress` and friends). fluent-bit vendors an unrelated pure-C
    # reimplementation and installs it next to its other static libs as
    # `libsnappy-c.a`, which `Findfluentbit.cmake` globs onto the link line.
    # The two archives export the same four names with incompatible
    # signatures: upstream's `snappy_compress` takes `(input, len, out,
    # *outlen)` while the vendored one takes an additional leading
    # `struct snappy_env *`, and their `snappy_uncompress` differ likewise.
    #
    # `libsnappy-c.a` also holds `snappy_init_env`/`snappy_free_env`, which
    # upstream does not have, so fluent-bit always drags that member in; if
    # `libsnappy.a` is searched first, the linker binds fluent-bit's
    # `flb_snappy.c` to upstream's incompatible symbols and loads both members.
    # Apple's `ld` then aborts with duplicate symbols, while GNU `ld` takes the
    # first definition and silently produces a mismatched call.
    #
    # Nothing else in Tenzir's closure wants the C API: arrow-cpp, apache-orc
    # and avro-cpp all reach for the C++ `snappy::Compress`/`snappy::Uncompress`
    # instead. Drop the member so the vendored archive is the only definition
    # on the line.
    postFixup = (orig.postFixup or "") + ''
      archive="$out/lib/libsnappy.a"
      if ! "$AR" t "$archive" | grep -qxF snappy-c.cc.o; then
        echo "snappy-c.cc.o is missing from $archive; this override needs an update" >&2
        exit 1
      fi
      "$AR" d "$archive" snappy-c.cc.o
      "$RANLIB" "$archive"
    '';
  }
)
