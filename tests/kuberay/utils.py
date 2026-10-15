import os
import socket


def get_random_free_port():
    sock = socket.socket()
    sock.bind(("", 0))
    return sock.getsockname()[1]


# a namespace per test session lets several sessions share one cluster
NAMESPACE = os.getenv("PYTEST_KUBERAY_NAMESPACE", "ray")

# keeps each test Ray pod small, so CI nodes don't run out of CPU; KubeRay derives Ray's num-cpus from the CPU limit
RAY_POD_RESOURCES = {
    "requests": {"cpu": "250m", "memory": "512Mi"},
    "limits": {"cpu": "2", "memory": "3Gi"},
}
