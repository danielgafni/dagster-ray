import shlex

from dagger import field, function, object_type
from dagger.telemetry import get_tracer
from dagger_clients.core import File, core
from dagger_clients.k3s import K3S, k3_s

from dagster_ray_dev.registry import REGISTRY_HOST, registry

HELM_IMAGE = "alpine/helm:3.19.0"

# containerd in k3s pulls `registry:5000/...` images from the registry service over plain HTTP
REGISTRIES_YAML = f"""mirrors:
  "{REGISTRY_HOST}":
    endpoint:
      - "http://{REGISTRY_HOST}"
"""

# k3s reads this on start; the reservations keep CPU and memory for k3s itself when the test pods are busy,
# otherwise the kubelet and the API server stall and the node goes NotReady. The tests reach Ray through
# port-forwarding, so they need neither a load balancer pod per RayCluster nor volume provisioning.
CONFIG_YAML = """kubelet-arg:
  - "kube-reserved=cpu=1,memory=1Gi"
  - "system-reserved=cpu=500m,memory=512Mi"
disable:
  - servicelb
  - local-storage
"""


def _version(v: str) -> tuple[int, ...]:
    return tuple(int(part) for part in v.split("."))


@object_type
class KuberayCluster:
    """A k3s cluster with the KubeRay operator. It pulls `registry:5000/...` images from the registry service."""

    version: str = field()
    kubernetes_version: str = field()
    instance: str = field(default="")

    @function
    def name(self) -> str:
        # the k3s module keys its state volumes by name, so concurrent clusters on one engine need distinct names
        name = f"kuberay{self.version}-k8s{self.kubernetes_version}"
        return f"{name}-{self.instance}" if self.instance else name

    def _helm_install_args(self) -> list[str]:
        args = [
            "helm",
            "upgrade",
            "--install",
            "--create-namespace",
            "--namespace",
            "kuberay-operator",
            "kuberay-operator",
            "kuberay/kuberay-operator",
            "--version",
            self.version,
            "--wait",
            "--timeout",
            "10m",
        ]
        if _version(self.version) > (1, 3, 0):
            args += [
                "--set",
                "featureGates[0].name=RayClusterStatusConditions",
                "--set",
                "featureGates[0].enabled=true",
            ]
        # RayJobDeletionPolicy is alpha and off by default in 1.5.x, and beta and on by default from 1.6.0
        if (1, 5, 0) <= _version(self.version) < (1, 6, 0):
            args += [
                "--set",
                "featureGates[1].name=RayJobDeletionPolicy",
                "--set",
                "featureGates[1].enabled=true",
            ]
        return args

    async def _start(self) -> K3S:
        """Start k3s and install the KubeRay operator. The cluster runs until the session ends."""
        cluster = k3_s(self.name(), image=f"rancher/k3s:v{self.kubernetes_version}-k3s1")
        cluster = cluster.with_container(
            cluster.container()
            .with_service_binding("registry", registry())
            # /etc/rancher/k3s is a cache volume, so the files have to be written by an exec
            .with_exec(
                [
                    "sh",
                    "-c",
                    f"cat > /etc/rancher/k3s/registries.yaml <<'EOF'\n{REGISTRIES_YAML}EOF\n"
                    f"cat > /etc/rancher/k3s/config.yaml <<'EOF'\n{CONFIG_YAML}EOF",
                ]
            )
        )
        with get_tracer().start_as_current_span(f"start k3s {self.name()}"):
            await cluster.server().start()

        with get_tracer().start_as_current_span(f"install KubeRay {self.version}"):
            await (
                core()
                .container()
                .from_(HELM_IMAGE)
                .with_exec(["apk", "add", "--no-cache", "kubectl"])
                .with_file("/root/.kube/config", cluster.config())
                # the API server accepts connections before it can serve requests
                .with_exec(
                    [
                        "sh",
                        "-c",
                        "until kubectl get --raw=/readyz && kubectl wait --for=condition=Ready nodes --all; "
                        "do sleep 2; done",
                    ]
                )
                .with_exec(["helm", "repo", "add", "kuberay", "https://ray-project.github.io/kuberay-helm/"])
                # the chart comes from GitHub releases, which occasionally time out; the install is idempotent
                .with_exec(
                    [
                        "sh",
                        "-c",
                        f"for i in 1 2 3; do {shlex.join(self._helm_install_args())} && exit 0; sleep 10; done; exit 1",
                    ]
                )
                .sync()
            )
        return cluster

    @function
    async def kubeconfig(self, context: str = "default") -> File:
        """Start the cluster and return its kubeconfig, with the context named `context`."""
        cluster = await self._start()
        return (
            core()
            .container()
            .from_("alpine:3.22")
            .with_file("/kubeconfig", cluster.config())
            # k3s names the cluster, user and context `default`
            .with_exec(["sed", "-i", f"s/: default$/: {context}/", "/kubeconfig"])
            .file("/kubeconfig")
        )

    @function
    async def pods(self) -> str:
        """Start the cluster and list its pods."""
        cluster = await self._start()
        return await cluster.kubectl("get pods --all-namespaces").stdout()
