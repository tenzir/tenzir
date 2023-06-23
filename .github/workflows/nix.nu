#!/usr/bin/env nu

# The expected input schema:
let config = {
  editions: list<{
    name: string
    static: bool
    upload-package-to-github: bool
    package-stores: list<string>
    image-registries: list<string>
  }>
  aliases: list<string>
  tags: list<string>
}

def upload_packages [
  name: string
  package_stores: list<string>
  aliases # annotation breaks in nu 0.78 : list<string>
  copy: bool
] {
  let pkg_dir = (nix --accept-flake-config --print-build-logs build $".#($name)^package" --no-link --print-out-paths)
  let debs = (glob $"($pkg_dir)/*.deb")
  let tgzs = (glob $"($pkg_dir)/*.tar.gz")
  for store in $package_stores {
    let type = ($store | split row ':' | get 0)
    # rclone expects remote ids to be capitalized in environment variables.
    with-env { $"RCLONE_CONFIG_($type | str screaming-snake-case)_TYPE": $type } {
      let run = {|label: string xs: list|
        for x in $xs {
          let dest = $"($store)/($label)/($x | path basename)"
          print $"::notice copying artifact to ($dest)"
          rclone -q copyto $x $dest
          for alias in $aliases {
            # TODO: Add a suffix to alias paths in case we have more than one
            # artifact.
            let alias_path = $"($store)/($label)/($name)-($alias).($x | path parse | get extension)"
            print $"::notice copying artifact to ($alias_path)"
            rclone -q copyto $dest $alias_path
          }
        }
      }
      do $run "debian" $debs
      do $run "tarball" $tgzs
    }
  }
  if $copy {
    print $"::notice copying artifacts to /packages/{debian,tarball}"
    mkdir ./packages/debian ./packages/tarball
    for deb in $debs {
      cp -v $deb ./packages/debian
    }
    for tgz in $tgzs {
      cp -v $tgz ./packages/tarball
    }
  }
  if $copy {
    mkdir ./release/debian ./release/tarball
    cp ($debs | get 0) ./packages/debian/tenzir-linux-static.deb
    cp ($tgzs | get 0) ./packages/tarball/tenzir-linux-static.tar.gz
  }
}

def push_images [
  name: string
  image_registries: list<string>
  tags # annotation breaks in nu 0.78 : list<string>
] {
  if ($image_registries == [] or $tags == []) {
    return
  }
  let image_name = ($name | str replace "static" "slim")
  let repo_name = ($name | str replace "-static" "")
  nix run $".#stream-($image_name)-image" | zstd -fo image.tar.zst
  for reg in $image_registries {
    for tag in $tags {
      let dest = $"docker://($reg)/tenzir/($repo_name):($tag)"
      print $"::notice pushing ($dest)"
      skopeo copy docker-archive:./image.tar.zst $dest
    }
  }
}

def attribute_name [
  edition: record
] {
  $"($edition.name)(if $edition.static {"-static"} else {""})"
}

export def run [
  cfg: record<
    editions: list<record<
      name: string
      static: bool
      upload-package-to-github: bool
      package-stores: list<string>
      image-registries: list<string>
    >>
    aliases: list<string>
    tags: list<string>
  >
] {
  # Run local effects by building all requested editions.
  let targets = ($cfg.editions | each {|e| $".#(attribute_name $e)" })
  print $"::notice building ($targets)"
  nix --print-build-logs build --no-link $targets
  # Run remote effects by uploading packages and images.
  for e in $cfg.editions {
    let stores = (if ($e.package-stores? == null) {[]} else {$e.package-stores})
    let aliases = (if ($cfg.aliases? == null) {[]} else $cfg.aliases)
    upload_packages (attribute_name $e) $stores $aliases $e.upload-package-to-github
    let registries = (if ($e.image-registries? == null) {[]} else {$e.image-registries})
    let tags = (if ($cfg.tags? == null) {[]} else {$cfg.tags})
    push_images (attribute_name $e) $registries $tags
  }
}

def main [
  cfg?: string
] {
  let jcfg = (if ($cfg == null or $cfg == "-") {
    cat
  } else {
    $cfg
  } | from json)
  run $jcfg
}
