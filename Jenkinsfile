#!/usr/bin/env groovy

// Default CMake flags for release builds.
defaultReleaseBuildFlags = [
]

// CMake flags for debug builds.
defaultDebugBuildFlags = defaultReleaseBuildFlags + [
    'ENABLE_ADDRESS_SANITIZER:BOOL=yes',
]

defaultBuildFlags = [
  debug: defaultDebugBuildFlags,
  release: defaultReleaseBuildFlags,
]

// CMake flags by OS and build type.
buildFlags = [
    macOS: [
        debug: defaultDebugBuildFlags + [
            'OPENSSL_ROOT_DIR=/usr/local/opt/openssl',
            'OPENSSL_INCLUDE_DIR=/usr/local/opt/openssl/include',
        ],
        release: defaultReleaseBuildFlags + [
            'OPENSSL_ROOT_DIR=/usr/local/opt/openssl',
            'OPENSSL_INCLUDE_DIR=/usr/local/opt/openssl/include',
        ],
    ],
]

// Our build matrix. Keys are the operating system labels and values are build configurations.
buildMatrix = [
    // Debug builds with ASAN + trace logs.
    [ 'Linux', [
        builds: ['debug'],
        tools: ['gcc8'],
        extraSteps: ['coverageReport'],
    ]],
    [ 'macOS', [
        builds: ['debug'],
        tools: ['clang'],
    ]],
    ['FreeBSD', [
        builds: ['debug'],
        tools: ['clang'],
    ]],
    // Release builds for various OS/tool combinations.
    [ 'Linux', [
        builds: ['release'],
        tools: ['gcc8'],
    ]],
    [ 'macOS', [
        builds: ['release'],
        tools: ['clang'],
    ]],
]

// Optional environment variables for combinations of labels.
buildEnvironments = [
    nop : [], // Dummy value for getting the proper types.
]

// Adds additional context information to commits on GitHub.
def setBuildStatus(context, state, message) {
    echo "set ${context} result for commit ${env.GIT_COMMIT} to $state: $message"
    step([
        $class: 'GitHubCommitStatusSetter',
        commitShaSource: [
            $class: 'ManuallyEnteredShaSource',
            sha: env.GIT_COMMIT,
        ],
        reposSource: [
            $class: 'ManuallyEnteredRepositorySource',
            url: env.GIT_URL,
        ],
        contextSource: [
            $class: 'ManuallyEnteredCommitContextSource',
            context: context,
        ],
        errorHandlers: [[
            $class: 'ChangingBuildStatusErrorHandler',
            result: 'SUCCESS',
        ]],
        statusResultSource: [
            $class: 'ConditionalStatusResultSource',
            results: [[
                $class: 'AnyBuildResult',
                state: state,
                message: message,
            ]]
        ],
    ]);
}

// Creates coverage reports via the Cobertura plugin.
def coverageReport(buildId) {
    echo "Create coverage report for build ID $buildId"
    // Paths we wish to ignore in the coverage report.
    def excludePaths = [
        "/usr/",
        "$WORKSPACE/$buildId/",
        "$WORKSPACE/vast-sources/vast/",
        "$WORKSPACE/vast-sources/tools/",
        "$WORKSPACE/vast-sources/libvast_test/",
        "$WORKSPACE/vast-sources/libvast/test/",
    ]
    def excludePathsStr = excludePaths.join(',')
    dir('vast-sources') {
        try {
            withEnv(['ASAN_OPTIONS=verify_asan_link_order=false:detect_leaks=0']) {
                sh """
                    kcov --exclude-path=$excludePathsStr kcov-result ./build/bin/vast-test &> kcov_output.txt
                    find . -name 'coverage.json' -exec mv {} result.json \\;
                """
            }
            stash includes: 'result.json', name: 'coverage-result'
            archiveArtifacts '**/cobertura.xml'
            cobertura([
                autoUpdateHealth: false,
                autoUpdateStability: false,
                coberturaReportFile: '**/cobertura.xml',
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
        } catch (Exception e) {
            echo "exception: $e"
            sh 'ls -R .'
            archiveArtifacts 'kcov_output.txt'
        }
    }
}

// Compiles, installs and tests via CMake.
def cmakeSteps(buildType, cmakeArgs, buildId) {
    def installDir = "$WORKSPACE/$buildId"
    dir('vast-sources') {
        // Configure and build.
        cmakeBuild([
            buildDir: 'build',
            buildType: buildType,
            cmakeArgs: (cmakeArgs + [
              "CAF_ROOT_DIR=\"$installDir\"",
              "BROKER_ROOT_DIR=\"$installDir\"",
              "CMAKE_INSTALL_PREFIX=\"$installDir\"",
              "VAST_RELOCATEABLE_INSTALL=ON",
            ]).collect { x -> '-D' + x }.join(' '),
            installation: 'cmake in search path',
            sourceDir: '.',
            steps: [[
                args: '--target install',
                withCmake: true,
            ]],
        ])
        // Run unit tests.
        try {
            ctest([
                arguments: '--output-on-failure',
                installation: 'cmake in search path',
                workingDir: 'build',
            ])
            writeFile file: "${buildId}.success", text: "success\n"
        } catch (Exception) {
            writeFile file: "${buildId}.failure", text: "failure\n"
        }
        stash includes: "${buildId}.*", name: buildId
    }
    // Only generate artifacts for the master branch.
    if (PrettyJobBaseName == 'master') {
        zip([
            archive: true,
            dir: buildId,
            zipFile: "${buildId}.zip",
        ])
    }
}

// Runs all build steps.
def buildSteps(buildType, cmakeArgs, buildId) {
    echo "prepare build steps on stage $STAGE_NAME"
    deleteDir()
    dir(buildId) {
      // Create directory.
    }
    echo "get latest Broker/CAF build for $buildId"
    dir('upstream-import') {
        copyArtifacts([
            filter: "${buildId}.zip",
            projectName: 'Broker',
        ])
    }
    unzip([
        zipFile: "upstream-import/${buildId}.zip",
        dir: buildId,
        quiet: true,
    ])
    echo 'get sources from previous stage and run CMake'
    unstash 'vast-sources'
    cmakeSteps(buildType, cmakeArgs, buildId)
}

// Builds a stage for given builds. Results in a parallel stage if `builds.size() > 1`.
def makeBuildStages(matrixIndex, os, builds, lblExpr, settings) {
    builds.collectEntries { buildType ->
        def id = "$matrixIndex $lblExpr: $buildType"
        [
            (id):
            {
                node(lblExpr) {
                    stage(id) {
                        try {
                            def buildId = "${lblExpr}_${buildType}".replace(' && ', '_')
                            withEnv(buildEnvironments[lblExpr] ?: []) {
                              buildSteps(buildType, (buildFlags[os] ?: defaultBuildFlags)[buildType], buildId)
                              (settings['extraSteps'] ?: []).each { fun -> "$fun"(buildId) }
                            }
                        } finally {
                          cleanWs()
                        }
                    }
                }
            }
        ]
    }
}

// Declarative pipeline for triggering all stages.
pipeline {
    options {
        buildDiscarder(logRotator(numToKeepStr: '20', artifactNumToKeepStr: '5'))
    }
    agent none
    environment {
        LD_LIBRARY_PATH = "$WORKSPACE/vast-sources/build/lib;" +
                          "$WORKSPACE/caf-install/lib"
        DYLD_LIBRARY_PATH = "$WORKSPACE/vast-sources/build/lib;" +
                            "$WORKSPACE/caf-install/lib"
        ASAN_OPTIONS = 'detect_leaks=0'
        PrettyJobBaseName = env.JOB_BASE_NAME.replace('%2F', '/')
        PrettyJobName = "VAST build #${env.BUILD_NUMBER} for $PrettyJobBaseName"
    }
    stages {
        // Checkout all involved repositories.
        stage('Git Checkout') {
            agent { label 'master' }
            steps {
                deleteDir()
                dir('vast-sources') {
                  checkout scm
                }
                stash includes: 'vast-sources/**', name: 'vast-sources'
            }
        }
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
                            xs << makeBuildStages(matrixIndex, os, builds, labelExpr, settings)
                        }
                    }
                    parallel xs
                }
            }
        }
        stage('Check Test Results') {
            agent { label 'master' }
            steps {
                script {
                    dir('tmp') {
                        // Compute the list of all build IDs.
                        def buildIds = []
                        buildMatrix.each { entry ->
                            def (os, settings) = entry
                            settings['tools'].each { tool ->
                                settings['builds'].each {
                                    buildIds << "${os}_${tool}_${it}"
                                }
                            }
                        }
                        // Compute how many tests have succeeded
                        def builds = buildIds.size()
                        def successes = buildIds.inject(0) { result, buildId ->
                            try { unstash buildId }
                            catch (Exception) { }
                            result + (fileExists("${buildId}.success") ? 1 : 0)
                        }
                        echo "$successes unit tests tests of $builds were successful"
                        if (builds == successes) {
                            setBuildStatus('unit-tests', 'SUCCESS', 'All builds passed the unit tests')
                        } else {
                            def failures = builds - successes
                            setBuildStatus('unit-tests', 'FAILURE', "$failures/$builds builds failed to run the unit tests")
                        }
                        // Get the coverage result.
                        try {
                            unstash 'coverage-result'
                            if (fileExists('result.json')) {
                                def resultJson = readJSON file: 'result.json'
                                setBuildStatus('coverage', 'SUCCESS', resultJson['percent_covered'] + '% coverage')
                            } else {
                              setBuildStatus('coverage', 'FAILURE', 'Unable to get coverage report')
                            }
                        } catch (Exception) {
                            setBuildStatus('coverage', 'FAILURE', 'Unable to generate coverage report')
                        }
                    }
                }
            }
        }
    }
    post {
        success {
            emailext(
                subject: "✅ $PrettyJobName succeeded",
                to: 'engineering@tenzir.com',
                recipientProviders: [culprits()],
                body: "Check console output at ${env.BUILD_URL}.\n",
            )
        }
        failure {
            emailext(
                subject: "⛔️ $PrettyJobName failed",
                to: 'engineering@tenzir.com',
                recipientProviders: [culprits()],
                attachLog: true,
                compressLog: true,
                body: "Check console output at ${env.BUILD_URL} or see attached log.\n",
            )
        }
    }
}

