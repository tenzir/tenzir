#!/usr/bin/env groovy

// Default CMake flags for most builds (except coverage).
defaultBuildFlags = [
]

// CMake flags for release builds.
releaseBuildFlags = defaultBuildFlags + [
]

// CMake flags for debug builds.
debugBuildFlags =  defaultBuildFlags + [
    'ENABLE_ADDRESS_SANITIZER:BOOL=yes',
]

// Our build matrix. Keys are the operating system labels and values are build configurations.
buildMatrix = [
    // Debug builds with ASAN + trace logs.
    [ 'Linux', [
        builds: ['debug'],
        tools: ['gcc8'],
        cmakeArgs: debugBuildFlags,
    ]],
    [ 'macOS', [
        builds: ['debug'],
        tools: ['clang'],
        cmakeArgs: debugBuildFlags,
    ]],
    // Release builds for various OS/tool combinations.
    [ 'Linux', [
        builds: ['release'],
        tools: ['gcc8'],
        cmakeArgs: releaseBuildFlags,
    ]],
    [ 'macOS', [
        builds: ['release'],
        tools: ['clang'],
        cmakeArgs: releaseBuildFlags,
    ]],
    // One Additional build for coverage reports.
    ['Linux', [
        builds: ['debug'],
        tools: ['gcc8 && gcovr'],
        extraSteps: ['coverageReport'],
        cmakeArgs: defaultBuildFlags + [
            'ENABLE_GCOV:BOOL=yes',
            'NO_EXCEPTIONS:BOOL=yes',
        ],
    ]],
]

// Optional environment variables for combinations of labels.
buildEnvironments = [
    nop : [], // Dummy value for getting the proper types.
]

// Creates coverage reports via the Cobertura plugin.
def coverageReport() {
    dir("vast-sources") {
        sh 'gcovr -e vast -e tools -e ".*/test/.*" -x -r . > coverage.xml'
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

// Compiles, installs and tests via CMake.
def cmakeSteps(buildType, cmakeArgs, buildId) {
    def cafInstallDir = "$WORKSPACE/caf-install"
    def cafLibraryDir = "$WORKSPACE/caf-install/lib"
    def vastInstallDir = "$WORKSPACE/$buildId"
    dir('vast-sources') {
        // Configure and build.
        cmakeBuild([
            buildDir: 'build',
            buildType: buildType,
            cmakeArgs: (cmakeArgs + [
              "CAF_ROOT_DIR=\"$cafInstallDir\"",
              "CMAKE_INSTALL_PREFIX=\"$vastInstallDir\"",
            ]).collect { x -> '-D' + x }.join(' '),
            installation: 'cmake in search path',
            sourceDir: '.',
            steps: [[
                args: '--target install',
                withCmake: true,
            ]],
        ])
        // Run unit tests.
        ctest([
            arguments: '--output-on-failure',
            installation: 'cmake in search path',
            workingDir: 'build',
        ])
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
    echo "get latest CAF master build for $buildId"
    dir('caf-import') {
        copyArtifacts([
            filter: "${buildId}.zip",
            projectName: 'CAF/actor-framework/master/',
        ])
    }
    unzip([
        zipFile: "caf-import/${buildId}.zip",
        dir: 'caf-install',
        quiet: true,
    ])
    echo 'get sources from previous stage and run CMake'
    unstash 'vast-sources'
    cmakeSteps(buildType, cmakeArgs, buildId)
}

// Builds a stage for given builds. Results in a parallel stage if `builds.size() > 1`.
def makeBuildStages(matrixIndex, builds, lblExpr, settings) {
    builds.collectEntries { buildType ->
        def id = "$matrixIndex $lblExpr: $buildType"
        [
            (id):
            {
                node(lblExpr) {
                    stage(id) {
                        try {
                            def buildId = "$lblExpr && $buildType"
                            withEnv(buildEnvironments[lblExpr] ?: []) {
                              buildSteps(buildType, settings['cmakeArgs'], buildId)
                              (settings['extraSteps'] ?: []).each { fun -> "$fun"() }
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
    agent none
    environment {
        LD_LIBRARY_PATH = "$WORKSPACE/vast-sources/build/lib;" +
                          "$WORKSPACE/caf-install/lib"
        DYLD_LIBRARY_PATH = "$WORKSPACE/vast-sources/build/lib;" +
                            "$WORKSPACE/caf-install/lib"
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

