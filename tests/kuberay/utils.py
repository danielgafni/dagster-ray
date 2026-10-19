import os
import socket


def get_random_free_port():
    sock = socket.socket()
    sock.bind(("", 0))
    return sock.getsockname()[1]


# a namespace per test session lets several sessions share one cluster
NAMESPACE = os.getenv("PYTEST_KUBERAY_NAMESPACE", "ray")

# keeps each test Ray pod small, so CI nodes don't run out of CPU; the tests mostly wait on I/O.
# The limit is a whole CPU because `ray start` is single-threaded and starts twice as slow on half a CPU.
RAY_POD_RESOURCES = {
    "requests": {"cpu": "100m", "memory": "512Mi"},
    "limits": {"cpu": "1", "memory": "2Gi"},
}
# the object store default is 30% of the memory limit
RAY_START_PARAMS = {"num-cpus": "1", "object-store-memory": str(200 * 1024**2)}
# KubeRay's default head readiness probe, polled every second instead of every 5 after a 10 second delay
HEAD_READINESS_PROBE = {
    "exec": {
        "command": [
            "bash",
            "-c",
            "wget -T 2 -q -O- http://localhost:52365/api/local_raylet_healthz | grep success"
            " && wget -T 2 -q -O- http://localhost:8265/api/gcs_healthz | grep success",
        ]
    },
    "periodSeconds": 1,
    "timeoutSeconds": 5,
    "failureThreshold": 300,
}
