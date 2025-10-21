{ mimalloc }:
mimalloc.overrideAttrs (oldAttrs: {
  # Disable malloc override so we can use our own override in malloc.cpp
  cmakeFlags = (oldAttrs.cmakeFlags or [ ]) ++ [
    "-DMI_OVERRIDE=OFF"
  ];
})
