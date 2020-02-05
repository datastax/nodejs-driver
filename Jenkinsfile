#!groovy

def initializeEnvironment() {
  sh label: 'Assign Node.js global environment', script: '''#!/bin/bash -lex
    nodenv global ${NODEJS_VERSION}
  '''

  sh label: 'Download Apache Cassandra&reg; or DataStax Enterprise', script: '''#!/bin/bash -lex
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

def describePerCommitStage() {
  script {
    currentBuild.displayName = "Per-Commit"
    currentBuild.description = '''Per-Commit build and testing of against Node.js v8.16.2, v10.17.0, and 12.13.0
<ul>
  <li>8.16.2  - Apache Cassandara&reg; v2.1.x, DataStax Enterprise v6.0.x and v6.7.x</li>
  <li>10.17.0 - Apache Cassandara&reg; v2.1.x and development DataStax Enterprise v6.8.x</li>
  <li>12.13.0 - Development Apache Cassandara&reg; v4.0.x, DataStax Enterprise v5.1.x and v6.7.x (with examples)</li>
</ul>'''
  }
}

def describeScheduledTestingStage() {
  script {
    def type = params.CI_SCHEDULE.toLowerCase().capitalize()
    currentBuild.displayName = "${type} schedule"
    currentBuild.description = "${type} scheduled build and testing of all supported Apache Cassandara&reg; and DataStax Enterprise against Node.js v8.16.2, v10.17.0, and 12.13.0"
  }
}

def describeAdhocTestingStage() {
  script {
    def serverType = params.ADHOC_BUILD_AND_EXECUTE_TESTS_SERVER_VERSION.split('-')[0]
    def serverDisplayName = 'Apache Cassandara&reg;'
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

pipeline {
  agent none

  // Global pipeline timeout
  options {
    timeout(time: 10, unit: 'HOURS')
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
      choices: ['8.16.2', '10.17.0', '12.13.0', 'ALL'],
      description: 'Node.js version to use for adhoc <b>BUILD-AND-EXECUTE-TESTS</b> <strong>ONLY!</strong>')
    choice(
      name: 'ADHOC_BUILD_AND_EXECUTE_TESTS_SERVER_VERSION',
      choices: ['2.1',     // Legacy Apache Cassandara�
                '3.11',    // Current Apache Cassandara�
                '4.0',     // Development Apache Cassandara�
                'dse-5.1', // Legacy DataStax Enterprise
                'dse-6.0', // Previous DataStax Enterprise
                'dse-6.7', // Current DataStax Enterprise
                'dse-6.8', // Development DataStax Enterprise
                'ALL'],
      description: '''Apache Cassandara&reg; and DataStax Enterprise server version to use for adhoc <b>BUILD-AND-EXECUTE-TESTS</b> <strong>ONLY!</strong>
                      <table style="width:100%">
                        <col width="15%">
                        <col width="85%">
                        <tr>
                          <th align="left">Choice</th>
                          <th align="left">Description</th>
                        </tr>
                        <tr>
                          <td><strong>2.1</strong></td>
                          <td>Apache Cassandara&reg; v2.1.x</td>
                        </tr>
                        <tr>
                          <td><strong>3.11</strong></td>
                          <td>Apache Cassandara&reg; v3.11.x</td>
                        </tr>
                        <tr>
                          <td><strong>4.0</strong></td>
                          <td>Apache Cassandara&reg; v4.x (<b>CURRENTLY UNDER DEVELOPMENT</b>)</td>
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
    parameterizedCron("""
      # Every weeknight (Monday - Friday) around 3:00 AM
      H 3 * * 1-5 %CI_SCHEDULE=WEEKNIGHTS
    """)
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
            values '2.1',     // Legacy Apache Cassandara�
                   '3.11',    // Current Apache Cassandara�
                   '4.0',     // Development Apache Cassandara�
                   'dse-5.1', // Legacy DataStax Enterprise
                   'dse-6.0', // Previous DataStax Enterprise
                   'dse-6.7', // Current DataStax Enterprise
                   'dse-6.8'  // Development DataStax Enterprise
          }
          axis {
            name 'NODEJS_VERSION'
            values '8.16.2', '10.17.0', '12.13.0'
          }
        }
        excludes {
          exclude {
            axis {
              name 'NODEJS_VERSION'
              values '8.16.2'
            }
            axis {
              name 'CASSANDRA_VERSION'
              values '3.11', '4.0', 'dse-5.1', 'dse-6.8'
            }
          }
          exclude {
            axis {
              name 'NODEJS_VERSION'
              values '10.17.0'
            }
            axis {
              name 'CASSANDRA_VERSION'
              values '2.1', '4.0', 'dse-5.1', 'dse-6.0', 'dse-6.7'
            }
          }
          exclude {
            axis {
              name 'NODEJS_VERSION'
              values '12.13.0'
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
          stage('Describe-Build') {
            steps {
              describePerCommitStage()
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
              expression { env.CASSANDRA_VERSION == 'dse-6.7' }
            }
            steps {
              executeExamples()
            }
          }
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
            values '2.1',     // Legacy Apache Cassandara�
                   '3.11',    // Current Apache Cassandara�
                   '4.0',     // Development Apache Cassandara�
                   'dse-5.1', // Legacy DataStax Enterprise
                   'dse-6.0', // Previous DataStax Enterprise
                   'dse-6.7', // Current DataStax Enterprise
                   'dse-6.8'  // Development DataStax Enterprise
          }
          axis {
            name 'NODEJS_VERSION'
            values '8.16.2', '10.17.0', '12.13.0'
          }
        }

        agent {
          label "${OS_VERSION}"
        }

        stages {
          stage('Describe-Build') {
            steps {
              describeScheduledTestingStage()
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
              expression { env.CASSANDRA_VERSION == 'dse-6.7' }
            }
            steps {
              executeExamples()
            }
          }
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
            values '2.1',      // Legacy Apache Cassandara�
                   '3.11',     // Current Apache Cassandara�
                   '4.0',      // Development Apache Cassandara�
                   'dse-5.1',  // Legacy DataStax Enterprise
                   'dse-6.0',  // Previous DataStax Enterprise
                   'dse-6.7',  // Current DataStax Enterprise
                   'dse-6.8' // Development DataStax Enterprise
          }
          axis {
            name 'NODEJS_VERSION'
            values '8.16.2', '10.17.0', '12.13.0'
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
              expression { param.ADHOC_BUILD_AND_EXECUTE_TESTS_EXECUTE_EXAMPLES }
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