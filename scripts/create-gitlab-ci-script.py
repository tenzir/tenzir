#!/usr/bin/env python3

import itertools

def get_skeleton():
    return '''
stages:
  - dependencies
  - build
  - unit-test
  - coverage
  - deploy

### Templates
.build_caf_template: &build_caf_template
  stage: dependencies
  script:
    - scripts/gitlab caf aux/caf caf_install_folder
  cache:
    key: "${CI_JOB_NAME}-${CI_PROJECT_DIR}"
    paths:
      - aux/caf/
  artifacts:
    paths:
    - caf_install_folder

.build_vast_template: &build_vast_template
  stage: build
  script:
    - scripts/gitlab vast caf_install_folder vast_install_folder
  cache:
    key: "${CI_JOB_NAME}-${CI_PROJECT_DIR}"
    paths:
      - build/
  artifacts:
    paths:
      - vast_install_folder

.test_template: &test_template
  stage: unit-test
  script:
    - scripts/gitlab test caf_install_folder vast_install_folder

### Code Coverage
# see: https://tenzir.gitlab.io/vast
build_vast:osx:clang:gcov:
  stage: coverage
  script:
    - scripts/gitlab vast caf_install_folder vast_install_folder
  cache:
    key: "${CI_JOB_NAME}-${CI_PROJECT_DIR}"
    paths:
      - build/
  artifacts:
    paths:
      - build/coverage.html
  variables:
    BUILD: "gcov"
    COMPILER: "clang"
  dependencies:
    - build_caf:osx:clang:debug
  tags:
    - osx
    - clang

pages:
  stage: deploy
  dependencies:
    - build_vast:osx:clang:gcov
  script:
    - mkdir public
    - mv build/coverage.html public/index.html
    - ls public/
  artifacts:
    paths:
      - public
  #only:
    #- master

####################
### Matrix Build ###
####################
    '''.strip()

class config:
    def __init__(self, os, compiler, build):
        self.os = os
        self.compiler = compiler
        self.build = build

def get_env(x):
    return  \
'''  variables:
    BUILD: "{build}"
    COMPILER: "{compiler}"'''.format(build = x.build, compiler = x.compiler)

def get_tags(x):
    return \
'''  tags:
    - {os}
    - {compiler}'''.format(os = x.os, compiler = x.compiler)

def get_dependency_job(x):
    return \
'''build_caf:{os}:{compiler}:{build}:
  <<: *build_caf_template
'''.format(os=x.os, compiler=x.compiler, build=x.build) + \
    get_env(x) + "\n" + \
    get_tags(x)

def get_build_job(x):
    return \
'''build_vast:{os}:{compiler}:{build}:
  <<: *build_vast_template'''.format(os=x.os, compiler=x.compiler, build=x.build) + "\n" + \
    get_env(x) + "\n" + \
    get_tags(x) + "\n" + \
'''  dependencies:
    - build_caf:{os}:{compiler}:{build}'''.format(os=x.os, compiler=x.compiler, build=x.build)

def get_test_job(x):
    return \
'''test:{os}:{compiler}:{build}:
  <<: *test_template
  dependencies:
    - build_caf:{os}:{compiler}:{build}
    - build_vast:{os}:{compiler}:{build}
'''.format(os=x.os, compiler=x.compiler, build=x.build) + \
    get_tags(x)

def build_matrix(*params):
    for x in itertools.product(*params):
        yield config(*x)

def write_to_file(filename, output):
    f = open(filename, "w")
    f.write(output)

def run():
    filename = ".gitlab-ci.yml"
    oss = ["osx"]
    compilers = ["clang"]
    builds = ["release", "debug"]
    output = get_skeleton() + "\n\n"
    output += "### Dependency Stages\n"
    for x in build_matrix(oss, compilers, builds):
        output += get_dependency_job(x) + "\n\n"
    output += "### Build Stages\n"
    for x in build_matrix(oss, compilers, builds):
        output += get_build_job(x) + "\n\n"
    output += "### Test Stages\n"
    for x in build_matrix(oss, compilers, builds):
        output += get_test_job(x) + "\n\n"
    write_to_file(filename, output)

if __name__ == "__main__":
    run()
