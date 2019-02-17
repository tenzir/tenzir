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
        extraSteps: ['integrationTests'],
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

def fileLinesOrEmptyList(fileName) {
    // Using a fileExists() + readFile() approach here fails for some mysterious reason.
    sh([
        script: """if [ -f "$fileName" ] ; then cat "$fileName" ; fi""",
        returnStdout: true,
    ]).trim().tokenize('\n')
}

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
    try { unstash buildId }
    catch (Exception) { }
    if (!fileExists("${buildId}.success")) {
        echo "Skip coverage report for build ID $buildId due to earlier failure"
        return
    }
    echo "Create coverage report for build ID $buildId"
    // Paths we wish to ignore in the coverage report.
    def excludePaths = [
        "/usr/",
        "$WORKSPACE/$buildId/",
        "$WORKSPACE/vast-sources/aux/",
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
                    kcov --exclude-path=$excludePathsStr kcov-result ./build/bin/vast-test &> kcov_output.txtt
                    find kcov-result -name 'cobertura.xml' -exec mv {} cobertura.xml \\;
                    find kcov-result -name 'coverage.json' -exec mv {} result.json \\;
                """
            }
            archiveArtifacts 'cobertura.xml'
            cobertura([
                autoUpdateHealth: false,
                autoUpdateStability: false,
                coberturaReportFile: 'cobertura.xml',
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
            def query = [
               "token=${CODECOV_TOKEN}",
               "commit=${env.GIT_COMMIT}",
               "branch=${env.GIT_BRANCH}",
               "build=${env.BUILD_NUMBER}",
               "build_url=${BUILD_URL}",
               "service=jenkins",
            ]
            def queryStr = query.join('&')
            sh "curl -X POST --data-binary @cobertura.xml \"https://codecov.io/upload/v2?${queryStr}\""
            stash includes: 'result.json', name: 'coverage-result'
        } catch (Exception e) {
            echo "exception: $e"
            sh 'ls -R .'
            archiveArtifacts 'kcov_output.txt'
        }
    }
}

def integrationTests(buildId) {
    // Any error here must not fail the build itself.
    dir('integration-tests') {
        deleteDir()
        try {
            def baseDir = "$WORKSPACE/vast-sources/scripts/integration"
            def envDir = pwd() + "python-environment"
            def app = "$WORKSPACE/$buildId/bin/vast"
            writeFile([
                file: 'all-integration-tests.txt',
                text: ''
            ])
            writeFile([
                file: 'failed-integration-tests.txt',
                text: ''
            ])
            sh """
                chmod +x "$app"
                export LD_LIBRARY_PATH="$WORKSPACE/$buildId/lib"
                python3 -m venv "$envDir"
                source "$envDir/bin/activate"
                pip install -r "$baseDir/requirements.txt"
                python3 "$baseDir/integration.py" -l | while read test ; do
                    echo "\$test" >> all-integration-tests.txt
                    python3 "$baseDir/integration.py" --app "$app" -t "\$test" || echo "\$test" >> failed-integration-tests.txt
                done
            """
            archiveArtifacts '*.txt'
            stash([
                includes: '*.txt',
                name: 'integration-result',
            ])
        } catch (Exception e) {
            echo "exception: $e"
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
    agent { label 'master' }
    environment {
        LD_LIBRARY_PATH = "$WORKSPACE/vast-sources/build/lib;" +
                          "$WORKSPACE/caf-install/lib"
        DYLD_LIBRARY_PATH = "$WORKSPACE/vast-sources/build/lib;" +
                            "$WORKSPACE/caf-install/lib"
        PrettyJobBaseName = env.JOB_BASE_NAME.replace('%2F', '/')
        PrettyJobName = "VAST/$PrettyJobBaseName #${env.BUILD_NUMBER}"
        ASAN_OPTIONS = 'detect_leaks=0'
        CODECOV_TOKEN=credentials('codecov-vast')
    }
    stages {
        // Checkout all involved repositories.
        stage('Checkout') {
            steps {
                echo "build branch ${env.GIT_BRANCH}"
                deleteDir()
                dir('vast-sources') {
                  checkout scm
                }
                stash includes: 'vast-sources/**', name: 'vast-sources'
                setBuildStatus('unit-tests', 'PENDING', '')
                setBuildStatus('integration', 'PENDING', '')
                setBuildStatus('coverage', 'PENDING', '')
            }
        }
        stage('Build') {
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
        stage('Notify') {
            steps {
                script {
                    dir('tmp') {
                        deleteDir()
                        // Collect headlines and summaries for the email notification.
                        def headlines = [
                            "✅ build",
                        ]
                        def summaries = [
                            "Successfully compiled $PrettyJobName.",
                        ]
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
                        def builds = buildIds.size()
                        // Compute how many unit tests have failed.
                        def testSuccesses = buildIds.inject(0) { result, buildId ->
                            try { unstash buildId }
                            catch (Exception) { }
                            result + (fileExists("${buildId}.success") ? 1 : 0)
                        }
                        if (builds == testSuccesses) {
                            headlines << "✅ tests"
                            summaries << "The unit tests succeeded on all $builds builds."
                            setBuildStatus('unit-tests', 'SUCCESS', "All $builds builds passed the unit tests")
                        } else {
                            def failures = builds - testSuccesses
                            def failRate = "$failures/$builds"
                            headlines << "⛔️ tests"
                            summaries << "The unit tests failed on $failRate builds."
                            setBuildStatus('unit-tests', 'FAILURE', "$failRate builds failed to run the unit tests")
                        }
                        // Compute how many integration tests have failed.
                        try { unstash 'integration-result' }
                        catch (Exception) { }
                        def allIntegrationTests = fileLinesOrEmptyList('all-integration-tests.txt')
                        def failedIntegrationTests = fileLinesOrEmptyList('failed-integration-tests.txt')
                        def allIntegrationTestsSize = allIntegrationTests.size()
                        if (allIntegrationTestsSize > 0 && failedIntegrationTests.isEmpty()) {
                            headlines << "✅ integration"
                            summaries << "All $allIntegrationTestsSize integration tests passed."
                            setBuildStatus('integration', 'SUCCESS', "All $allIntegrationTestsSize integration tests passed")
                        } else {
                            def failedIntegrationTestsSize = failedIntegrationTests.size()
                            def failRate = "$failedIntegrationTestsSize/$allIntegrationTestsSize"
                            headlines << "⛔️ integration"
                            if (allIntegrationTestsSize > 0) {
                                summaries << ("The following integration tests failed ($failRate):\n" + failedIntegrationTests.collect{'- ' + it}.join('\n'))
                                setBuildStatus('integration', 'FAILURE', "$failRate integration tests failed")
                            } else {
                                summaries << "Unable to run integration tests!"
                                setBuildStatus('integration', 'FAILURE', "Unable to run integration tests")
                            }
                        }
                        // Check coverage result.
                        try {
                            unstash 'coverage-result'
                            def coverageResult = readJSON('result.json')
                            writeFile([
                                file: 'coverage.txt',
                                text: coverageResult['percent_covered'],
                            ])
                            archiveArtifacts('coverage.txt')
                        }
                        catch (Exception) { }
                        if (fileExists('result.json')) {
                            headlines << "✅ coverage"
                            summaries << "The coverage report was successfully uploaded to codecov.io."
                            setBuildStatus('coverage', 'SUCCESS', 'Generate coverage report')
                        } else {
                            headlines << "⛔️ coverage"
                            summaries << "No coverage report was produced."
                            setBuildStatus('coverage', 'FAILURE', 'Unable to generate coverage report')
                        }
                        // Send email notification.
                        emailext(
                            subject: "$PrettyJobName: " + headlines.join(', '),
                            to: 'engineering@tenzir.com',
                            recipientProviders: [culprits()],
                            attachLog: true,
                            compressLog: true,
                            body: summaries.join('\n\n') + '\n',
                        )
                        // Set the status of this commit to unstable if any check failed to not trigger downstream jobs.
                        if (headlines.any{ it.contains("⛔️") })
                            currentBuild.result = "UNSTABLE"
                    }
                }
            }
        }
    }
    post {
        failure {
            emailext(
                subject: "$PrettyJobName: ⛔️ build, ⛔️ tests, ⛔️ integration, ⛔️ coverage",
                to: 'engineering@tenzir.com',
                recipientProviders: [culprits()],
                attachLog: true,
                compressLog: true,
                body: "Check console output at ${env.BUILD_URL} or see attached log.\n",
            )
            setBuildStatus('unit-tests', 'FAILURE', 'Unable to run unit tests')
            setBuildStatus('integration', 'FAILURE', 'Unable to run integration tests')
            setBuildStatus('coverage', 'FAILURE', 'Unable to generate coverage report')
        }
    }
}
