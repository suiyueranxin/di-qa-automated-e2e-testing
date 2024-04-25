pipeline {
  agent any
  stages {
    stage('Install dependencies') {
      steps {
        sh '''
          echo "Install dependencies"
          whoami
          if $shouldUpdateLibs
          then
          curl -sSL https://bootstrap.pypa.io/get-pip.py -o get-pip.py
          python3 get-pip.py
          whereis pip
          pip install requests 
          pip install unittest-xml-reporting 
          pip install hdbcli
          pip install junit2html
          pip install Cython
          pip install pyrfc
          fi
        '''
      }
    }

    stage('Start pipeline graph') {
      steps {
        sh '''
        echo "Start pipeline graph"
        resultpath=$WORKSPACE/testresult
        rm -rf $resultpath
        rm -rf *.txt
        rm -rf *.html
        rm -rf *.xml
        python3 runtestcases.py test_start tests test*.py ./testresult/startPipeline/
        '''
      }
    }

    stage('Validation for the result') {
      steps {
        script {
          catchError(buildResult: 'UNSTABLE', stageResult: 'FAILURE') {
            sh '''
            echo "Validation started"
            retries=$((retries+1))
            while [ $retries -gt 0 ]
            do
              retries=$((retries-1))
              sleep "$duration"m
              {
                python3 runtestcases.py test_validation tests test*.py testresult/${retries}
                status=$?
              } || echo "There are failed test cases"
              echo "$retries" > retries.txt
              if [ $status -eq 0 ]
              then
              break
              fi
            done
            if [ $status -ne 0 ]
            then
            exit 1
            fi
            '''
          }
        }
      }
    }

    stage('Generate HTML Report') {
      steps {
        sh '''
        echo "Generate HTML Report"
        if [ -f retries.txt ]
        then
          retrytimes=`cat retries.txt`
        fi
        RESULTPATH=$WORKSPACE/testresult/${retrytimes}
        if [ -d "$RESULTPATH" ]
        then
        resultFile=`ls $RESULTPATH/*.xml`
        echo "result file path is:"
        echo ${resultFile}
        cp $resultFile $WORKSPACE/testresult.xml
        python3 -m junit2htmlreport $WORKSPACE/testresult.xml $WORKSPACE/testresult.html
        fi
        '''
      }
    }

  }
  post {
    always {
      archiveArtifacts artifacts: 'testresult/startPipeline/*.xml,testresult.html'
      emailext (
        to: "${params.recipients}",
        subject: 'Testresult-customerscenario-$BUILD_DISPLAY_NAME',
        body: '${FILE,path="testresult.html"}',
        mimeType: 'text/html'
      )
    }
  }
  environment {
    PATH="/home/ccloud/.local/bin:/var/jenkins_home/.local/bin:${env.PATH}"
    SAPNWRFC_HOME="/usr/local/sap/nwrfcsdk"
    LD_LIBRARY_PATH="/usr/local/sap/nwrfcsdk/lib:${env.LD_LIBRARY_PATH}"
  }
  parameters {
    string(name: 'duration', defaultValue: '1', description: '')
    string(name: 'retries', defaultValue: '3', description: 'max retry times')
    booleanParam(name: 'shouldUpdateLibs', defaultValue: false, description: '')
    string(name: 'recipients', defaultValue: 'issac.han@sap.com;amanda.li@sap.com', description: '')
    choice(name: 'VSYSTEM_ENDPOINT', choices: ["https://vsystem.ingress.dh-9w8tyh8hqzm.di-dev2.shoot.canary.k8s-hana.ondemand.com","https://vsystem.ingress.dh-1n2gaz1i.dh-testing.shoot.canary.k8s-hana.ondemand.com"], description: 'Cluster url')
    string(name: 'VORA_TENANT', defaultValue: 'cit-tenant', description: 'tenant name')
    string(name: 'VORA_USERNAME', defaultValue: 'cit-test', description: 'user name')
    password(name: 'VORA_PASSWORD', description: 'password')
    string(name: 'TABLE_SUFFIX', defaultValue: 'UNIQUESUFFIX', description: 'suffix of target table, please make it unique')
    string(name: "ABAP_CONN_USER",defaultValue: 'DH_TEST', description: 'User name of ABAP connection')
    password(name: "ABAP_CONN_PASSWD",defaultValue: 'Qwer1234!', description: 'Password of ABAP connection')
    string(name: "ABAP_CONN_ASHOST",defaultValue: 'ldciuk5.wdf.sap.corp', description: 'ABAP connection server')
    string(name: "ABAP_CONN_SYSNR",defaultValue: 'UK5', description: 'ABAP connection type')
    string(name: "ABAP_CONN_CLIENT",defaultValue: '800', description: 'ABAP client')
  }
}
