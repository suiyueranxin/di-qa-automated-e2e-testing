# Jaas

The files in this folder will be used to run test cases in jenkins in kubernetes cluster. Basically when the job is started, a pod which contains jenkins slave container(https://hub.docker.com/r/jenkins/inbound-agent/) will be created automatically by kubernetes plugin, and register itself to master node via JNLP. The test cases will be running in a container defined in the jenkinsfile. Since a shared volume will be created and mounted into all of the containers, in jenkins file containers could be easily switched and share the data in the workspace. After the execution of the test cases, the pod will be deleted and the resources will be released.

The detailed steps for the whole process:

1. A pod is created and assigned to a kubernetes node by kubernetes plugin.
2. Jenkins slave container and project container(defined in project.yaml) will be started seperately in the pod. 
3. Clone the project code to the agent workspace
4. Execute the steps defined in jenkinsfile
5. Delete the pod after the job is finished.

