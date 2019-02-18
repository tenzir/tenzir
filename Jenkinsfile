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

// Configures what checks we report on at the end.
checks = [
    'build',
    'style',
    'tests',
    'integration',
    'coverage',
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
                    find kcov-result -name 'coverage.json' -exec mv {} coverage.json \\;
                """
            }
            archiveArtifacts 'cobertura.xml,coverage.json'
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
            stash includes: 'coverage.json', name: 'coverage-result'
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
                python "$baseDir/integration.py" -l | while read test ; do
                    echo "\$test" >> all-integration-tests.txt
                    python "$baseDir/integration.py" --app "$app" -t "\$test" || echo "\$test" >> failed-integration-tests.txt
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

// Reports the status of the build itself, i.e., compiling via CMake.
def buildStatus(buildIds) {
    [
        // We always report success here, because we won't reach the final stage otherwise.
        success: true,
        summary: "All ${buildIds.size()} builds compiled",
        text: "Successfully compiled all ${buildIds.size()} builds for $PrettyJobName.",
    ]
}

// Reports the status of the unit tests check.
def testsStatus(buildIds) {
    def failed = buildIds.findAll {
        try { unstash it }
        catch (Exception) { }
        !fileExists("${it}.success")
    }
    def numBuilds = buildIds.size()
    if (failed.isEmpty())
        return [
            success: true,
            summary: "All $numBuilds builds passed the unit tests",
            text: "The unit tests succeeded on all $numBuilds builds."
        ]
    def failRate = "${failed.size()}/$numBuilds"
    [
        success: false,
        summary: "$failRate builds failed to run the unit tests",
        text: "The unit tests failed on $failRate builds:\n" + failed.collect{"- $it"}.join('\n'),
    ]
}

// Reports the status of the integration tests check.
def integrationStatus(buildIds) {
    try { unstash 'integration-result' }
    catch (Exception) { }
    def all = fileLinesOrEmptyList('all-integration-tests.txt')
    def failed = fileLinesOrEmptyList('failed-integration-tests.txt')
    if (all.isEmpty())
        return [
            success: false,
            summary: 'Unable to run integration tests',
            text: 'Unable to run integration tests!',
        ]
    def numTests = all.size()
    if (failed.isEmpty())
        return [
            success: true,
            summary: "All $numTests integration tests passed",
            text: "All $numTests integration tests passed.",
        ]
    def failRate = "${failed.size()}/$numTests"
    [
        success: false,
        summary: "$failRate integration tests failed",
        text: "The following integration tests failed ($failRate):\n" + failed.collect{"- $it"}.join('\n')
    ]
}

def styleStatus(buildIds) {
    def clangFormatDiff = ''
    try {
        unstash 'clang-format-result'
        clangFormatDiff = readFile('clang-format-diff.txt')
    }
    catch (Exception) {
        return [
            success: false,
            summary: 'Unable to produce clang-format diff',
            text: 'Unable to produce clang-format diff!',
        ]
    }
    if (clangFormatDiff.isEmpty())
        return [
            success: true,
            summary: 'Code follows style conventions',
            text: 'Code follows style conventions and clang-format had no complaints.',
        ]
    [
        success: false,
        summary: 'Code violates style conventions',
        text: "Running the code through clang-format produces this diff:\n$clangFormatDiff",
    ]
}

// Reports the status of the coverage check.
def coverageStatus(buildIds) {
    try {
        unstash 'coverage-result'
        def coverageResult = readJSON('coverage.json')
        writeFile([
            file: 'coverage.txt',
            text: coverageResult['percent_covered'],
        ])
        archiveArtifacts('project-coverage.txt')
    }
    catch (Exception) { }
    if (fileExists('coverage.json'))
        return [
            success: true,
            summary: 'Generated coverage report',
            text: "The coverage report was successfully generated and uploaded to codecov.io.",
        ]
    [
        success: false,
        summary: 'Unable to generate coverage report',
        text: 'No coverage report was produced!',
    ]
}

def notifyAllChecks(result, message) {
    checks.each {
        if (it != 'build')
            setBuildStatus(it, result, message)
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
                    // Prepare the git_diff.txt file needed by the 'Check Style' stage.
                    sh """
                        if [ "${env.GIT_BRANCH}" == master ] ; then
                            # on master, we simply compare to the last commit
                            git diff -U0 --no-color HEAD^ > git_diff.txt
                        else
                            # in branches, we diff to the merge-base, because Jenkins might not see each commit individually
                            git fetch --no-tags ${env.GIT_URL} +refs/heads/master:refs/remotes/origin/master
                            git diff -U0 --no-color \$(git merge-base origin/master HEAD) > git_diff.txt
                        fi
                    """
                }
                stash includes: 'vast-sources/**', name: 'vast-sources'
                notifyAllChecks('PENDING', '')
            }
        }
        stage('Check Style') {
            agent { label 'clang-format' }
            steps {
                deleteDir()
                unstash 'vast-sources'
                dir('vast-sources') {
                    sh './scripts/clang-format-diff.py -p1 < git_diff.txt > clang-format-diff.txt'
                    stash([
                        includes: 'clang-format-diff.txt',
                        name: 'clang-format-result',
                    ])
                    archiveArtifacts('clang-format-diff.txt')
                }
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
                        // Collect headlines and summaries for the email notification.
                        def failedChecks = 0
                        def headlines = []
                        def texts = []
                        checks.each {
                            def checkResult = "${it}Status"(buildIds)
                            if (checkResult.success) {
                                headlines << "✅ ${it}"
                                texts << checkResult.text
                                // Don't set commit status for 'build', because Jenkins will do that anyway.
                                if (it != 'build')
                                    setBuildStatus(it, 'SUCCESS', checkResult.summary)
                            } else {
                                failedChecks += 1
                                headlines << "⛔️ ${it}"
                                texts << checkResult.text
                                setBuildStatus(it, 'FAILURE', checkResult.summary)
                            }
                        }
                        // Send email notification.
                        emailext(
                            subject: "$PrettyJobName: " + headlines.join(', '),
                            to: 'engineering@tenzir.com',
                            recipientProviders: [culprits()],
                            attachLog: true,
                            compressLog: true,
                            body: texts.join('\n\n') + '\n',
                        )
                        // Set the status of this commit to unstable if any check failed to not trigger downstream jobs.
                        if (failedChecks > 0)
                            currentBuild.result = "UNSTABLE"
                    }
                }
            }
        }
    }
    post {
        failure {
            emailext(
                subject: "$PrettyJobName: " + checks.collect{ "⛔️ ${it}" }.join(', '),
                to: 'engineering@tenzir.com',
                recipientProviders: [culprits()],
                attachLog: true,
                compressLog: true,
                body: "Check console output at ${env.BUILD_URL} or see attached log.\n",
            )
            notifyAllChecks('FAILURE', 'Failed due to earlier error')
        }
    }
}
