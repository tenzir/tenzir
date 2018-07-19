#!/usr/bin/env groovy

cafDefaultFlags = [
  'CAF_NO_TOOLS:BOOL=yes',
  'CAF_NO_EXAMPLES:BOOL=yes',
  'CAF_NO_PYTHON:BOOL=yes',
  'CAF_NO_OPENCL:BOOL=yes',
]

vastDefaultFlags = [
  // nop
]

// Our build matrix. The keys are the operating system labels and the values
// are lists of tool labels.
buildMatrix = [
    // Release builds for various OS/tool combinations.
    [ 'Linux', [
        builds: ['release'],
        tools: ['gcc8'],
        cmakeArgs: [
            caf: cafDefaultFlags + [
                'CAF_ENABLE_RUNTIME_CHECKS:BOOL=yes',
            ],
            vast: vastDefaultFlags,
        ],
    ]],
    [ 'macOS', [
        builds: ['release'],
        tools: ['clang'],
        cmakeArgs: [
            caf: cafDefaultFlags + [
                'CAF_ENABLE_RUNTIME_CHECKS:BOOL=yes',
            ],
            vast: vastDefaultFlags,
        ],
    ]],
    // Debug builds with ASAN + trace logs.
    [ 'Linux', [
        builds: ['debug'],
        tools: ['gcc8'],
        cmakeArgs: [
            caf: cafDefaultFlags + [
                'CAF_ENABLE_RUNTIME_CHECKS:BOOL=yes',
                'CAF_ENABLE_ADDRESS_SANITIZER:BOOL=yes',
                'CAF_LOG_LEVEL:STRING=4',
            ],
            vast: vastDefaultFlags + [
                'CAF_ENABLE_RUNTIME_CHECKS:BOOL=yes',
                'ENABLE_ADDRESS_SANITIZER:BOOL=yes',
            ],
        ],
    ]],
    [ 'macOS', [
        builds: ['debug'],
        tools: ['clang'],
        cmakeArgs: [
            caf: cafDefaultFlags + [
                'CAF_ENABLE_ADDRESS_SANITIZER:BOOL=yes',
                'CAF_LOG_LEVEL:STRING=4',
            ],
            vast: vastDefaultFlags + [
                'ENABLE_ADDRESS_SANITIZER:BOOL=yes',
            ],
        ],
    ]],
    // One Additional build for coverage reports.
    ['unix', [
        builds: ['debug'],
        tools: ['gcc8 && gcovr'],
        extraSteps: ['coverageReport'],
        cmakeArgs: [
            caf: cafDefaultFlags + [
                'CAF_ENABLE_GCOV:BOOL=yes',
                'CAF_NO_EXCEPTIONS:BOOL=yes',
                'CAF_FORCE_NO_EXCEPTIONS:BOOL=yes',
            ],
            vast: vastDefaultFlags + [
                'ENABLE_GCOV:BOOL=yes',
                'NO_EXCEPTIONS:BOOL=yes',
            ],
        ],
    ]],
]

// Optional environment variables for combinations of labels.
buildEnvironments = [
    nop : [], // Dummy value for getting the proper types.
]

def coverageReport(buildType) {
    dir("vast-$buildType") {
        sh 'gcovr -e vast -e tools -e libvast/tests -x -r .. > coverage.xml'
        archiveArtifacts '**/coverage.xml'
        cobertura([
          autoUpdateHealth: false,
          autoUpdateStability: false,
          coberturaReportFile: '**/coverage.xml',
          conditionalCoverageTargets: '70, 0, 0',
          failUnhealthy: false,
          failUnstable: false,
          lineCoverageTargets: '80, 0, 0',
          maxNumberOfBuilds: 0,
          methodCoverageTargets: '80, 0, 0',
          onlyStable: false,
          sourceEncoding: 'ASCII',
          zoomCoverageChart: false,
        ])
    }
}

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
    echo "cmakeArgs: $cmakeArgs"
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

def makeFlags(xs) {
    xs.collect { x -> '-D' + x }.join(' ')
}

def buildAll(buildType, settings) {
    def cmakeArgs = settings['cmakeArgs']
    buildSteps('caf', buildType, makeFlags(cmakeArgs['caf']))
    buildSteps('vast', buildType, makeFlags(cmakeArgs['vast'] + [
        "CAF_ROOT_DIR=\"$WORKSPACE/caf-$buildType-install\"",
    ]))
    (settings['extraSteps'] ?: []).each { fun -> "$fun"(buildType) }
}

// Builds a stage for given builds. Results in a parallel stage `if builds.size() > 1`.
def makeBuildStages(matrixIndex, builds, lblExpr, settings) {
    builds.collectEntries { buildType ->
        def id = "$matrixIndex $lblExpr: $buildType"
        [
            (id):
            {
                node(lblExpr) {
                    echo "Trigger build on $NODE_NAME"
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
    post {
        success {
            emailext(
                subject: "✅ VAST build #${env.BUILD_NUMBER} succeeded for job ${env.JOB_NAME}",
                recipientProviders: [culprits(), developers(), requestor(), upstreamDevelopers()],
                body: "Check console output at ${env.BUILD_URL}.",
            )
        }
        failure {
            emailext(
                subject: "⛔️ VAST build #${env.BUILD_NUMBER} failed for job ${env.JOB_NAME}",
                attachLog: true,
                compressLog: true,
                recipientProviders: [culprits(), developers(), requestor(), upstreamDevelopers()],
                body: "Check console output at ${env.BUILD_URL} or see attached log.",
            )
        }
    }
}

