{
  fetchpatch2,
  rabbitmq-c,
}:
rabbitmq-c.overrideAttrs (orig: {
  patches = (orig.patches or [ ]) ++ [
    (fetchpatch2 {
      name = "rabbitmq-c-fix-include-path.patch";
      url = "https://github.com/alanxz/rabbitmq-c/commit/819e18271105f95faba99c3b2ae4c791eb16a664.patch";
      hash = "sha256-/c4y+CvtdyXgfgHExOY8h2q9cJNhTUupsa22tE3a1YI=";
    })
  ];
})
