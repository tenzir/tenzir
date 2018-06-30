#!/usr/bin/env groovy

// Our build matrix. The keys are the operating system labels and the values
// are lists of tool labels.
buildMatrix = [
    // Release and debug builds for various OS/tool combinations.
    [ 'Linux', [
        builds: ['debug', 'release'],
        tools: ['gcc7.2'],
    ]],
    [ 'macOS', [
        builds: ['debug', 'release'],
        tools: ['clang'],
    ]],
    // Additional debug builds with ASAN enabled.
    [ 'Linux', [
        cmakeArgs: '-DCAF_ENABLE_ADDRESS_SANITIZER:BOOL=yes ' + // CAF
                   '-DENABLE_ADDRESS_SANITIZER:BOOL=yes',       // VAST
        builds: ['debug'],
        tools: ['gcc7.2'],
    ]],
    [ 'macOS', [
        cmakeArgs: '-DCAF_ENABLE_ADDRESS_SANITIZER:BOOL=yes ' + // CAF
                   '-DENABLE_ADDRESS_SANITIZER:BOOL=yes',       // VAST
        builds: ['debug'],
        tools: ['clang'],
    ]],
    // Additional builds with CAF trace logs enabled.
    ['macOS', [
        cmakeArgs: '-D CAF_LOG_LEVEL=4',
        builds: ['debug', 'release'],
        tools: ['clang'],
    ]],
]

// Optional environment variables for combinations of labels.
buildEnvironments = [
  'macOS && gcc': ['CXX=g++'],
  'Linux && clang': ['CXX=clang++'],
]

def gitSteps(name, url, branch = 'master') {
    def sourceDir = "$name-sources"
    // Checkout in subdirectory.
    dir("$sourceDir") {
        deleteDir()
        git([
            url: "$url",
            branch: "$branch"
        ])
    }
    // Make sources available for later stages.
    stash includes: "$sourceDir/**", name: "$sourceDir"
}

def buildSteps(name, buildType, cmakeArgs) {
    def sourceDir = "$name-sources"
    def installDir = "$WORKSPACE/$name-$buildType-install"
    // Make sure no old junk is laying around.
    dir("$installDir") {
        deleteDir()
    }
    // Separate builds by build type on the file system.
    dir("$name-$buildType") {
        // Make sure no old junk is laying around.
        deleteDir()
        // Get sources from previous stage.
        unstash "$sourceDir"
        // Build in subdirectory and run unit tests.
        dir("$sourceDir") {
            cmakeBuild([
                buildDir: 'build',
                buildType: "$buildType",
                cleanBuild: true,
                sourceDir: '.',
                installation: 'cmake in search path',
                cmakeArgs: "-DCMAKE_INSTALL_PREFIX=\"$installDir\" $cmakeArgs " +
                           "-DOPENSSL_ROOT_DIR=/usr/local/opt/openssl " +
                           "-DOPENSSL_INCLUDE_DIR=/usr/local/opt/openssl/include",
                steps: [[args: 'all install']],
            ])
            // We could think of moving testing to a different stage. However,
            // this is currently not feasible because 1) the vast-test binary
            // isn't available from the installation directory, and 2) the unit
            // tests currently have hard-wired paths to the test data.
            withEnv([
                "LD_LIBRARY_PATH=$WORKSPACE/caf-$buildType-install/lib;"
                + "$WORKSPACE/vast-$buildType-install/lib",
                "DYLD_LIBRARY_PATH=$WORKSPACE/caf-$buildType-install/lib;"
                + "$WORKSPACE/vast-$buildType-install/lib"
            ]) {
                ctest([
                    arguments: '--output-on-failure',
                    installation: 'cmake in search path',
                    workingDir: 'build',
                ])
            }
        }
    }
}

def buildAll(buildType, settings) {
    def cmakeArgs = settings['cmakeArgs'] ?: ''
    buildSteps('caf', buildType,
               "-DCAF_NO_TOOLS:BOOL=yes " +
               "-DCAF_NO_EXAMPLES:BOOL=yes " +
               "-DCAF_NO_PYTHON:BOOL=yes " +
               "-DCAF_NO_OPENCL:BOOL=yes " +
               "-DCAF_ENABLE_RUNTIME_CHECKS:BOOL=yes " +
               cmakeArgs)
    buildSteps('vast', buildType,
               "-D CAF_ROOT_DIR=\"$WORKSPACE/caf-$buildType-install\" "
               + cmakeArgs)
    (settings['extraSteps'] ?: []).each { fun -> "$fun"() }
}

// Builds a stage for given builds. Results in a parallel stage `if builds.size() > 1`.
def makeBuildStages(matrixIndex, builds, lblExpr, settings) {
    builds.collectEntries { buildType ->
        def id = "$matrixIndex $lblExpr: $buildType"
        [
            (id):
            {
                node(lblExpr) {
                    withEnv(buildEnvironments[lblExpr] ?: []) {
                        buildAll(buildType, settings)
                    }
                }
            }
        ]
    }
}

pipeline {
    agent none
    stages {
        // Checkout all involved repositories.
        stage('Checkouts') {
            agent { label 'master' }
            steps {
              deleteDir()
              // Checkout the main repository via default SCM
              dir('vast-sources') {
                checkout scm
              }
              stash includes: 'vast-sources/**', name: 'vast-sources'
              // Checkout dependencies manually via Git
              gitSteps('caf', 'https://github.com/actor-framework/actor-framework.git')
            }
        }
        // Start builds.
        stage('Builds') {
            agent { label 'master' }
            steps {
                script {
                    // Create stages for building everything in our build matrix in
                    // parallel.
                    def xs = [:]
                    buildMatrix.eachWithIndex { entry, index ->
                        def (os, settings) = entry
                        settings['tools'].eachWithIndex { tool, toolIndex ->
                            def matrixIndex = "[$index:$toolIndex]"
                            def builds = settings['builds']
                            def labelExpr = "$os && $tool"
                            xs << makeBuildStages(matrixIndex, builds, labelExpr, settings)
                        }
                    }
                    parallel xs
                }
            }
        }
    }
}

