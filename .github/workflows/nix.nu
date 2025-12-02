#!/usr/bin/env nu

# The expected input schema:
#let config = {
#  editions: list<{
#    name: string
#    attribute: string
#    upload-package-to-github: bool
#    package-stores: list<string>
#    image-registries: list<string>
#  }>
#  aliases: list<string>
#  container-tags: list<string>
#  release-tag: string
#}

def upload_packages [
  name: string
  package_stores: list<string>
  aliases # annotation breaks in nu 0.78 : list<string>
  copy: bool = false
  git_tag = null
] {
  let pkg_dir = (nix --accept-flake-config --print-build-logs build $".#($name)^package" ...($env.extra_options | split row " ") --no-link --print-out-paths)
  let rpms = (glob $"($pkg_dir)/*.rpm")
  let debs = (glob $"($pkg_dir)/*.deb")
  let pkgs = (glob $"($pkg_dir)/*.pkg")
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
            let alias_basename = $x | path basename | str replace --regex '[0-9]+\.[0-9]+\.[0-9]+' $alias
            let alias_path = $"($store)/($label)/($alias_basename)"
            print $"::notice copying artifact to ($alias_path)"
            rclone -q copyto $dest $alias_path
          }
        }
      }
      do $run "rpm" $rpms
      do $run "debian" $debs
      do $run "macOS" $pkgs
      do $run "tarball" $tgzs
    }
  }
  print $"::notice copying artifacts to /packages/{debian,tarball}"
  mkdir ./packages/rpm ./packages/debian ./packages/tarball ./packages/macOS
  for rpm in $rpms {
    cp -v $rpm ./packages/rpm
  }
  for deb in $debs {
    cp -v $deb ./packages/debian
  }
  for pkg in $pkgs {
    cp -v $pkg ./packages/macOS
  }
  for tgz in $tgzs {
    cp -v $tgz ./packages/tarball
  }
  if $copy {
    if $git_tag != null {
      let pkgs = (glob $"($pkg_dir)/*")
      for pkg in $pkgs {
        print $"::attaching ($pkg) to ($git_tag)"
        gh release upload $git_tag $pkg --clobber
      }
    }
  }
}

def push_images [
  repo_name: string
  attribute: string
  image_registries: list<string>
  container_tags # annotation breaks in nu 0.78 : list<string>
] {
  let os = (uname | get kernel-name)
  if ($os != "Linux" or $image_registries == [] or $container_tags == []) {
    return
  }
  # We always push two images: `tenzir` and `tenzir-node`.
  let node_repo_name = ($repo_name | str replace "tenzir" "tenzir-node")
  let tag_suffix = if ($attribute | str contains "-static") {"-slim"} else {""}
  for reg in $image_registries {
    for repo in [$repo_name $node_repo_name] {
      for tag in $container_tags {
        let dest = $"docker://($reg)/tenzir/($repo):($tag)($tag_suffix)"
        print $"::notice pushing ($dest)"
        nix --accept-flake-config run $".#($attribute).asImage.($repo).copyTo" ...($env.extra_options | split row " ")  -- $dest
      }
    }
  }
}

export def run [
  cfg: record
  # The type checker became stricter between 0.89 and 0.92 and does not allow missing values any more.
  #<
  #  editions: list<record<
  #    name: string
  #    attribute: string
  #    upload-package-to-github: bool
  #    package-stores: list<string>
  #    image-registries: list<string>
  #  >>
  #  aliases: list<string>
  #  container-tags: list<string>
  #  git-tag: string
  #>
] {
  # Update the tenzir-plugins submodule source info.
  nix/update-plugins.sh
  # Run local effects by building all requested editions.
  if ($cfg.git-tag? != null) {
    $env.extra_options = "--override-input isReleaseBuild github:boolean-option/true"
  } else {
    $env.extra_options = "--override-input isReleaseBuild github:boolean-option/false"
  }
  let targets = ($cfg.editions | each {|e| $".#($e.attribute)" })
  print $"::notice building ($targets)"
  nix --accept-flake-config --print-build-logs build --no-link ...($env.extra_options | split row " ") ...$targets
  # Run remote effects by uploading packages and images.
  for e in $cfg.editions {
    let stores = (if ($e.package-stores? == null) {[]} else {$e.package-stores})
    let aliases = (if ($cfg.aliases? == null) {[]} else $cfg.aliases)
    let copy = (if ($e.upload-package-to-github? == null) {false} else {$e.upload-package-to-github})
    upload_packages $e.attribute $stores $aliases $copy $cfg.git-tag?
    let registries = (if ($e.image-registries? == null) {[]} else {$e.image-registries})
    let container_tags = (if ($cfg.container-tags? == null) {[]} else {$cfg.container-tags})
    push_images $e.name $e.attribute $registries $container_tags
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
