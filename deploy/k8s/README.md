
### Installing the Chart

`helm install --name my-release --namespace drc ./`


### Install a test mysql

`helm install --name example-mysql --namespace drc -f mysql-values stable/mysql`

### Debug with minikube

`minikube service list`