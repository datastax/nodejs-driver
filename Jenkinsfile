#!groovy


def initializeEnvironment() {
  def nodeVersions = ['8': '8.16.2', '10': '10.17.0', '12': '12.13.0']
  env.DRIVER_DISPLAY_NAME = 'Cassandra Node.js Driver'
  env.DRIVER_METRIC_TYPE = 'oss'
  if (env.GIT_URL.contains('riptano/nodejs-driver')) {
    env.DRIVER_DISPLAY_NAME = 'private ' + env.DRIVER_DISPLAY_NAME
    env.DRIVER_METRIC_TYPE = 'oss-private'
  } else if (env.GIT_URL.contains('nodejs-dse-driver')) {
    env.DRIVER_DISPLAY_NAME = 'DSE Node.js Driver'
    env.DRIVER_METRIC_TYPE = 'dse'
  }

  env.GIT_SHA = "${env.GIT_COMMIT.take(7)}"
  env.GITHUB_PROJECT_URL = "https://${GIT_URL.replaceFirst(/(git@|http:\/\/|https:\/\/)/, '').replace(':', '/').replace('.git', '')}"
  env.GITHUB_BRANCH_URL = "${GITHUB_PROJECT_URL}/tree/${env.BRANCH_NAME}"
  env.GITHUB_COMMIT_URL = "${GITHUB_PROJECT_URL}/commit/${env.GIT_COMMIT}"
  env.NODEJS_VERSION_FULL = nodeVersions[env.NODEJS_VERSION]

  sh label: 'Assign Node.js global environment', script: '''#!/bin/bash -lex
    nodenv versions
    echo "Using Node.js runtime ${NODEJS_VERSION} (${NODEJS_VERSION_FULL})"
    nodenv global ${NODEJS_VERSION_FULL}
  '''

  sh label: 'Download Apache Cassandra or DataStax Enterprise', script: '''#!/bin/bash -lex
    . ${CCM_ENVIRONMENT_SHELL} ${CASSANDRA_VERSION}
  '''

  sh label: 'Display Node.js and environment information', script: '''#!/bin/bash -le
    # Load CCM environment variables
    set -o allexport
    . ${HOME}/environment.txt
    set +o allexport

    node --version
    npm --version
    printenv | sort
  '''
}

def installDriverAndDependencies() {
  sh label: 'Install the driver', script: '''#!/bin/bash -lex
    npm install
  '''

  sh label: 'Install driver dependencies', script: '''#!/bin/bash -lex
    npm install mocha-jenkins-reporter@0
    npm install kerberos@1
    npm install -g eslint@4
  '''
}

def executeLinter() {
  sh label: 'Perform static analysis of source code', script: '''#!/bin/bash -lex
    npm run eslint
  '''
}

def executeTests() {
  sh label: 'Execute tests', script: '''#!/bin/bash -lex
    # Load CCM environment variables
    set -o allexport
    . ${HOME}/environment.txt
    set +o allexport

    npm run ci_jenkins
  '''
}

def executeExamples() {
  sh label: 'Create CCM cluster for examples', script: '''#!/bin/bash -lex
    # Load CCM environment variables
    set -o allexport
    . ${HOME}/environment.txt
    set +o allexport

    ccm create test_samples --dse -v ${CCM_VERSION} -n 1:0 -b -s
  '''

  sh label: 'Execute examples', script: '''#!/bin/bash -lex
    set -o allexport
    . ${HOME}/environment.txt
    set +o allexport

    (
      cd examples
      npm install
      node runner.js
    )
  '''

  sh label: 'Clean-up CCM cluster for examples', script: '''#!/bin/bash -lex
    ccm remove
  '''
}

def notifySlack(status = 'started') {
  // Set the global pipeline scoped environment (this is above each matrix)
  env.BUILD_STATED_SLACK_NOTIFIED = 'true'

  def buildType = 'Commit'
  if (params.CI_SCHEDULE != 'DO-NOT-CHANGE-THIS-SELECTION') {
    buildType = "${params.CI_SCHEDULE.toLowerCase().capitalize()}"
  }

  def color = 'good' // Green
  if (status.equalsIgnoreCase('aborted')) {
    color = '808080' // Grey
  } else if (status.equalsIgnoreCase('unstable')) {
    color = 'warning' // Orange
  } else if (status.equalsIgnoreCase('failed')) {
    color = 'danger' // Red
  }

  def message = """Build ${status} for ${env.DRIVER_DISPLAY_NAME} [${buildType}]
<${env.GITHUB_BRANCH_URL}|${env.BRANCH_NAME}> - <${env.RUN_DISPLAY_URL}|#${env.BUILD_NUMBER}> - <${env.GITHUB_COMMIT_URL}|${env.GIT_SHA}>"""
  if (!status.equalsIgnoreCase('Started')) {
    message += """
${status} after ${currentBuild.durationString - ' and counting'}"""
  }

//  slackSend color: "${color}",
//            channel: "#nodejs-driver-dev-bots",
//            message: "${message}"
}

def submitCIMetrics(buildType) {
  long durationMs = currentBuild.duration
  long durationSec = durationMs / 1000
  long nowSec = (currentBuild.startTimeInMillis + durationMs) / 1000
  def branchNameNoPeriods = env.BRANCH_NAME.replaceAll('\\.', '_')
  def durationMetric = "okr.ci.nodejs.${env.DRIVER_METRIC_TYPE}.${buildType}.${branchNameNoPeriods} ${durationSec} ${nowSec}"

  timeout(time: 1, unit: 'MINUTES') {
    withCredentials([string(credentialsId: 'lab-grafana-address', variable: 'LAB_GRAFANA_ADDRESS'),
                     string(credentialsId: 'lab-grafana-port', variable: 'LAB_GRAFANA_PORT')]) {
      withEnv(["DURATION_METRIC=${durationMetric}"]) {
        sh label: 'Send runtime metrics to labgrafana', script: '''#!/bin/bash -lex
          echo "${DURATION_METRIC}" | nc -q 5 ${LAB_GRAFANA_ADDRESS} ${LAB_GRAFANA_PORT}
        '''
      }
    }
  }
}

def describePerCommitStage() {
  script {
    currentBuild.displayName = "Per-Commit"
    currentBuild.description = '''Per-Commit build and testing'''
  }
}

def describeScheduledTestingStage() {
  script {
    def type = params.CI_SCHEDULE.toLowerCase().capitalize()
    currentBuild.displayName = "${type} schedule"
    currentBuild.description = "${type} scheduled build and testing of all supported Apache Cassandra and DataStax " +
            "Enterprise against multiple Node.js runtimes"
  }
}

def describeAdhocTestingStage() {
  script {
    def serverType = params.ADHOC_BUILD_AND_EXECUTE_TESTS_SERVER_VERSION.split('-')[0]
    def serverDisplayName = 'Apache Cassandra'
    def serverVersion = " v${serverType}"
    if (serverType == 'ALL') {
      serverDisplayName = "all ${serverDisplayName} and DataStax Enterprise server versions"
      serverVersion = ''
    } else {
      try {
        serverVersion = " v${env.ADHOC_BUILD_AND_EXECUTE_TESTS_SERVER_VERSION.split('-')[1]}"
      } catch (e) {
        ;; // no-op
      }
      if (serverType == 'ddac') {
        serverDisplayName = "DataStax Distribution of ${serverDisplayName}"
      } else if (serverType == 'dse') {
        serverDisplayName = 'DataStax Enterprise'
      }
    }

    def nodeJsVersionInformation = "Node.js v${params.ADHOC_BUILD_AND_EXECUTE_TESTS_NODEJS_VERSION}"
    if (params.ADHOC_BUILD_AND_EXECUTE_TESTS_NODEJS_VERSION == 'ALL') {
      nodeJsVersionInformation = 'all Node.js versions'
    }

    def examplesInformation = ''
    if (ADHOC_BUILD_AND_EXECUTE_TESTS_EXECUTE_EXAMPLES) {
      examplesInformation = ' with examples'
    }

    currentBuild.displayName = "${params.ADHOC_BUILD_AND_EXECUTE_TESTS_SERVER_VERSION} against ${nodeJsVersionInformation}"
    currentBuild.description = "Testing ${serverDisplayName} ${serverVersion} against ${nodeJsVersionInformation}${examplesInformation}"
  }
}

// branch pattern for cron
def branchPatternCron = ~"(master)"

pipeline {
  agent none

  // Global pipeline timeout
  options {
    disableConcurrentBuilds()
    timeout(time: 5, unit: 'HOURS')
    buildDiscarder(logRotator(artifactNumToKeepStr: '10', // Keep only the last 10 artifacts
                              numToKeepStr: '50'))        // Keep only the last 50 build records
  }

  parameters {
    choice(
      name: 'ADHOC_BUILD_TYPE',
      choices: ['BUILD', 'BUILD-AND-EXECUTE-TESTS'],
      description: '''<p>Perform a adhoc build operation</p>
                      <table style="width:100%">
                        <col width="25%">
                        <col width="75%">
                        <tr>
                          <th align="left">Choice</th>
                          <th align="left">Description</th>
                        </tr>
                        <tr>
                          <td><strong>BUILD</strong></td>
                          <td>Performs a <b>Per-Commit</b> build</td>
                        </tr>
                        <tr>
                          <td><strong>BUILD-AND-EXECUTE-TESTS</strong></td>
                          <td>Performs a build and executes the integration and unit tests</td>
                        </tr>
                      </table>''')
    choice(
      name: 'ADHOC_BUILD_AND_EXECUTE_TESTS_NODEJS_VERSION',
      choices: ['8', '10', '12.13.0', 'ALL'],
      description: 'Node.js version to use for adhoc <b>BUILD-AND-EXECUTE-TESTS</b> <strong>ONLY!</strong>')
    choice(
      name: 'ADHOC_BUILD_AND_EXECUTE_TESTS_SERVER_VERSION',
      choices: ['2.1',     // Legacy Apache Cassandra
                '3.11',    // Current Apache Cassandra
                '4.0',     // Development Apache Cassandra
                'dse-5.1', // Legacy DataStax Enterprise
                'dse-6.0', // Previous DataStax Enterprise
                'dse-6.7', // Current DataStax Enterprise
                'dse-6.8', // Development DataStax Enterprise
                'ALL'],
      description: '''Apache Cassandra and DataStax Enterprise server version to use for adhoc <b>BUILD-AND-EXECUTE-TESTS</b> <strong>ONLY!</strong>
                      <table style="width:100%">
                        <col width="15%">
                        <col width="85%">
                        <tr>
                          <th align="left">Choice</th>
                          <th align="left">Description</th>
                        </tr>
                        <tr>
                          <td><strong>2.1</strong></td>
                          <td>Apache Cassandra v2.1.x</td>
                        </tr>
                        <tr>
                          <td><strong>3.11</strong></td>
                          <td>Apache Cassandra v3.11.x</td>
                        </tr>
                        <tr>
                          <td><strong>4.0</strong></td>
                          <td>Apache Cassandra v4.x (<b>CURRENTLY UNDER DEVELOPMENT</b>)</td>
                        </tr>
                        <tr>
                          <td><strong>dse-5.1</strong></td>
                          <td>DataStax Enterprise v5.1.x</td>
                        </tr>
                        <tr>
                          <td><strong>dse-6.0</strong></td>
                          <td>DataStax Enterprise v6.0.x</td>
                        </tr>
                        <tr>
                          <td><strong>dse-6.7</strong></td>
                          <td>DataStax Enterprise v6.7.x</td>
                        </tr>
                        <tr>
                          <td><strong>dse-6.8</strong></td>
                          <td>DataStax Enterprise v6.8.x (<b>CURRENTLY UNDER DEVELOPMENT</b>)</td>
                        </tr>
                      </table>''')
    booleanParam(
      name: 'ADHOC_BUILD_AND_EXECUTE_TESTS_EXECUTE_EXAMPLES',
      defaultValue: false,
      description: 'Flag to determine if the examples should be executed for adhoc builds')
    booleanParam(
      name: 'JUNIT_REPORT_STACK',
      defaultValue: true,
      description: 'Flag to determine if stack trace should be enabled with test failures for scheduled or adhoc builds')
    booleanParam(
      name: 'TEST_TRACE',
      defaultValue: true,
      description: 'Flag to determine if test tracing should be enabled for scheduled or adhoc builds')
    choice(
      name: 'CI_SCHEDULE',
      choices: ['DO-NOT-CHANGE-THIS-SELECTION', 'WEEKNIGHTS'],
      description: 'CI testing schedule to execute periodically scheduled builds and tests of the driver (<strong>DO NOT CHANGE THIS SELECTION</strong>)')
  }

  triggers {
    parameterizedCron(branchPatternCron.matcher(env.BRANCH_NAME).matches() ? """
      # Every weeknight (Monday - Friday) around 7 PM
      H 19 * * 1-5 %CI_SCHEDULE=WEEKNIGHTS
    """ : "")
  }

  environment {
    OS_VERSION = 'ubuntu/bionic64/nodejs-driver'
    JUNIT_REPORT_STACK = "${params.JUNIT_REPORT_STACK ? '1' : '0'}"
    JUNIT_REPORT_PATH = '.'
    TEST_TRACE = "${params.TEST_TRACE ? 'on' : 'off'}"
    SIMULACRON_PATH = '/home/jenkins/simulacron.jar'
    CCM_PATH = '/home/jenkins/ccm'
    CCM_ENVIRONMENT_SHELL = '/usr/local/bin/ccm_environment.sh'
  }

  stages {
    stage('Per-Commit') {
      when {
        beforeAgent true
        allOf {
          expression { params.ADHOC_BUILD_TYPE == 'BUILD' }
          expression { params.CI_SCHEDULE == 'DO-NOT-CHANGE-THIS-SELECTION' }
          not { buildingTag() }
        }
      }

      matrix {
        axes {
          axis {
            name 'CASSANDRA_VERSION'
            values '2.1',     // Legacy Apache Cassandra
                   '3.11',    // Current Apache Cassandra
                   '4.0',     // Development Apache Cassandra
                   'dse-5.1', // Legacy DataStax Enterprise
                   'dse-6.0', // Previous DataStax Enterprise
                   'dse-6.7', // Current DataStax Enterprise
                   'dse-6.8'  // Development DataStax Enterprise
          }
          axis {
            name 'NODEJS_VERSION'
            values '8', '10', '12'
          }
        }

        excludes {
          exclude {
            axis {
              name 'NODEJS_VERSION'
              values '8'
            }
            axis {
              name 'CASSANDRA_VERSION'
              values '3.11', '4.0', 'dse-5.1', 'dse-6.8'
            }
          }
          exclude {
            axis {
              name 'NODEJS_VERSION'
              values '10'
            }
            axis {
              name 'CASSANDRA_VERSION'
              values '2.1', '4.0', 'dse-5.1', 'dse-6.0', 'dse-6.7'
            }
          }
          exclude {
            axis {
              name 'NODEJS_VERSION'
              values '12'
            }
            axis {
              name 'CASSANDRA_VERSION'
              values '2.1', '3.11', 'dse-6.0', 'dse-6.8'
            }
          }
        }

        agent {
          label "${OS_VERSION}"
        }

        stages {
          stage('Initialize-Environment') {
            steps {
              initializeEnvironment()
              script {
                if (env.BUILD_STATED_SLACK_NOTIFIED != 'true') {
                  notifySlack()
                }
              }
            }
          }
          stage('Describe-Build') {
            steps {
              describePerCommitStage()
            }
          }
          stage('Install-Driver-And-Dependencies') {
            steps {
              installDriverAndDependencies()
            }
          }
          stage('Execute-Linter') {
            steps {
              executeLinter()
            }
          }
          stage('Execute-Tests') {
            steps {
              catchError { // Handle error conditions in the event examples should be executed
                executeTests()
              }
            }
            post {
              always {
                junit testResults: '*.xml'
              }
            }
          }
          stage('Execute-Examples') {
            when {
              expression { env.CASSANDRA_VERSION == 'dse-6.7' }
            }
            steps {
              executeExamples()
            }
          }
        }
      }
      post {
        always {
          node('master') {
            submitCIMetrics('commit')
          }
        }
        aborted {
          notifySlack('aborted')
        }
        success {
          notifySlack('completed')
        }
        unstable {
          notifySlack('unstable')
        }
        failure {
          notifySlack('FAILED')
        }
      }
    }

    stage('Scheduled-Testing') {
      when {
        beforeAgent true
        branch pattern: '((\\d+(\\.[\\dx]+)+)|dse|master)', comparator: 'REGEXP'
        allOf {
          expression { params.ADHOC_BUILD_TYPE == 'BUILD' }
          expression { params.CI_SCHEDULE != 'DO-NOT-CHANGE-THIS-SELECTION' }
          not { buildingTag() }
        }
      }
      matrix {
        axes {
          axis {
            name 'CASSANDRA_VERSION'
            values '2.1',     // Legacy Apache Cassandra
                   '3.11',    // Current Apache Cassandra
                   '4.0',     // Development Apache Cassandra
                   'dse-5.1', // Legacy DataStax Enterprise
                   'dse-6.0', // Previous DataStax Enterprise
                   'dse-6.7', // Current DataStax Enterprise
                   'dse-6.8'  // Development DataStax Enterprise
          }
          axis {
            name 'NODEJS_VERSION'
            values '8', '10', '12'
          }
        }

        excludes {
          exclude {
            axis {
              name 'NODEJS_VERSION'
              values '10'
            }
            axis {
              name 'CASSANDRA_VERSION'
              values '2.1', '3.11', '4.0', 'dse-5.1', 'dse-6.0', 'dse-6.7'
            }
          }
        }

        agent {
          label "${OS_VERSION}"
        }

        stages {
          stage('Initialize-Environment') {
            steps {
              initializeEnvironment()
              script {
                if (env.BUILD_STATED_SLACK_NOTIFIED != 'true') {
                  notifySlack()
                }
              }
            }
          }
          stage('Describe-Build') {
            steps {
              describeScheduledTestingStage()
            }
          }
          stage('Install-Driver-And-Dependencies') {
            steps {
              installDriverAndDependencies()
            }
          }
          stage('Execute-Linter') {
            steps {
              executeLinter()
            }
          }
          stage('Execute-Tests') {
            steps {
              catchError { // Handle error conditions in the event examples should be executed
                executeTests()
              }
            }
            post {
              always {
                junit testResults: '*.xml'
              }
            }
          }
          stage('Execute-Examples') {
            when {
              expression { env.CASSANDRA_VERSION == 'dse-6.7' }
            }
            steps {
              executeExamples()
            }
          }
        }
      }
      post {
        aborted {
          notifySlack('aborted')
        }
        success {
          notifySlack('completed')
        }
        unstable {
          notifySlack('unstable')
        }
        failure {
          notifySlack('FAILED')
        }
      }
    }

    stage('Adhoc-Testing') {
      when {
        beforeAgent true
        allOf {
          expression { params.ADHOC_BUILD_TYPE == 'BUILD-AND-EXECUTE-TESTS' }
          not { buildingTag() }
        }
      }
      matrix {
        axes {
          axis {
            name 'CASSANDRA_VERSION'
            values '2.1',      // Legacy Apache Cassandra
                   '3.11',     // Current Apache Cassandra
                   '4.0',      // Development Apache Cassandra
                   'dse-5.1',  // Legacy DataStax Enterprise
                   'dse-6.0',  // Previous DataStax Enterprise
                   'dse-6.7',  // Current DataStax Enterprise
                   'dse-6.8' // Development DataStax Enterprise
          }
          axis {
            name 'NODEJS_VERSION'
            values '8', '10', '12'
          }
        }
        when {
          beforeAgent true
          allOf {
            expression { params.ADHOC_BUILD_AND_EXECUTE_TESTS_SERVER_VERSION ==~ /(ALL|${env.CASSANDRA_VERSION})/ }
            expression { params.ADHOC_BUILD_AND_EXECUTE_TESTS_NODEJS_VERSION ==~ /(ALL|${env.NODEJS_VERSION})/ }
          }
        }

        agent {
          label "${OS_VERSION}"
        }

        stages {
          stage('Describe-Build') {
            steps {
              describeAdhocTestingStage()
            }
          }
          stage('Initialize-Environment') {
            steps {
              initializeEnvironment()
            }
          }
          stage('Install-Driver-And-Dependencies') {
            steps {
              installDriverAndDependencies()
            }
          }
          stage('Execute-Linter') {
            steps {
              executeLinter()
            }
          }
          stage('Execute-Tests') {
            steps {
              catchError { // Handle error conditions in the event examples should be executed
                executeTests()
              }
            }
            post {
              always {
                junit testResults: '*.xml'
              }
            }
          }
          stage('Execute-Examples') {
            when {
              expression { params.ADHOC_BUILD_AND_EXECUTE_TESTS_EXECUTE_EXAMPLES }
            }
            steps {
              executeExamples()
            }
          }
        }
      }
    }
  }
}
