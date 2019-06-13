#!/usr/bin/env groovy

@Library('caf-continuous-integration') _

// Default CMake flags for release builds.
defaultReleaseBuildFlags = [
    'VAST_RELOCATEABLE_INSTALL=ON',
]

// Default CMake flags for debug builds.
defaultDebugBuildFlags = defaultReleaseBuildFlags + [
    'ENABLE_ADDRESS_SANITIZER:BOOL=yes',
]

// Configures the behavior of our stages.
config = [
    // GitHub path to repository.
    repository: 'vast-io/vast',
    // List of enabled checks for email notifications.
    checks: [
        'build',
        'style',
        'tests',
        'integration',
        'coverage',
    ],
    // Dependencies that we need to fetch before each build.
    dependencies: [
        artifact: [
            'Broker',
        ],
        cmakeRootVariables: [
            'CAF_ROOT_DIR',
            'BROKER_ROOT_DIR',
        ],
    ],
    // Our build matrix. Keys are the operating system labels and values are build configurations.
    buildMatrix: [
        [ 'macOS', [
            builds: ['debug', 'release'],
            tools: ['clang'],
        ]],
        [ 'Linux', [
            builds: ['debug'],
            tools: ['gcc8'],
            extraSteps: ['coverageReport'],
        ]],
        [ 'Linux', [
            builds: ['release'],
            tools: ['gcc8'],
            extraSteps: ['integrationTests'],
        ]],
        ['FreeBSD', [
            builds: ['debug', 'release'],
            tools: ['clang'],
        ]],
    ],
    // Platform-specific environment settings.
    buildEnvironments: [
        nop: [], // Dummy value for getting the proper types.
    ],
    // Default CMake flags by build type.
    defaultBuildFlags: [
        debug: defaultDebugBuildFlags,
        release: defaultReleaseBuildFlags,
    ],
    // CMake flags by OS and build type to override defaults for individual builds.
    buildFlags: [
        nop: [],
    ],
    // Configures what binary the coverage report uses and what paths to exclude.
    coverage: [
        binary: 'build/bin/vast-test',
        relativeExcludePaths: [
            "aux/",
            "vast/",
            "tools/",
            "libvast_test/",
            "libvast/test/",
        ],
    ],
    // Configures which binary to use from the install prefix and where the integration scripts are.
    integration: [
        binary: 'vast',
        path: 'share/vast/integration',
    ]
]

// Declarative pipeline for triggering all stages.
pipeline {
    options {
        buildDiscarder(logRotator(numToKeepStr: '20', artifactNumToKeepStr: '5'))
    }
    agent { label 'master' }
    environment {
        PrettyJobBaseName = env.JOB_BASE_NAME.replace('%2F', '/')
        PrettyJobName = "VAST/$PrettyJobBaseName #${env.BUILD_NUMBER}"
        ASAN_OPTIONS = 'detect_leaks=0'
    }
    stages {
        stage('Checkout') {
            steps {
                getSources(config)
            }
        }
        stage('Lint') {
            agent { label 'clang-format' }
            steps {
                runClangFormat(config)
            }
        }
        stage('Build') {
            steps {
                buildParallel(config, PrettyJobBaseName)
            }
        }
        stage('Notify') {
            steps {
                collectResults(config, PrettyJobName)
            }
        }
    }
    post {
        failure {
            emailext(
                subject: "$PrettyJobName: " + config['checks'].collect{ "⛔️ ${it}" }.join(', '),
                to: 'engineering@tenzir.com',
                recipientProviders: [culprits()],
                attachLog: true,
                compressLog: true,
                body: "Check console output at ${env.BUILD_URL} or see attached log.\n",
            )
            notifyAllChecks(config, 'failure', 'Failed due to earlier error')
        }
    }
}
