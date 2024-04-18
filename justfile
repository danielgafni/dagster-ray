setup-minikube:
    minikube start --kubernetes-version=1.25.3
    eval $(minikube docker-env)
    minikube image load
    minikube stop
