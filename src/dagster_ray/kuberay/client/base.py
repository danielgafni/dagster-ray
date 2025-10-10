from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

if TYPE_CHECKING:
    from kubernetes import client
    from kubernetes.client.models.v1_endpoints import V1Endpoints


def load_kubeconfig(context: str | None = None, config_file: str | None = None) -> Any:
    from kubernetes import config

    try:
        return config.load_kube_config(context=context, config_file=config_file)
    except config.config_exception.ConfigException:
        try:
            return config.load_incluster_config()
        except config.config_exception.ConfigException:
            pass


T_Status = TypeVar("T_Status")


class BaseKubeRayClient(Generic[T_Status]):
    def __init__(
        self,
        group: str,
        version: str,
        kind: str,
        plural: str,
        api_client: client.ApiClient | None = None,
    ):
        from kubernetes import client

        self.group = group
        self.version = version
        self.kind = kind
        self.plural = plural
        self.api_client = api_client
        self._api = client.CustomObjectsApi(api_client=api_client)
        self._core_v1_api = client.CoreV1Api(api_client=api_client)

    def wait_for_service_endpoints(
        self, service_name: str, namespace: str, poll_interval: float = 1, timeout: float = 600
    ):
        from kubernetes.client import ApiException

        start_time = time.time()

        while True:
            try:
                # Get the endpoints for the service
                endpoints: V1Endpoints = self._core_v1_api.read_namespaced_endpoints(service_name, namespace)  # type: ignore

                # Check if there are addresses in any of the subsets
                if endpoints.subsets:
                    for subset in endpoints.subsets:
                        if subset.addresses:
                            return
            except ApiException:
                pass

            elapsed_time = time.time() - start_time
            if elapsed_time > timeout:
                raise TimeoutError(
                    f"Timed out ({timeout:.1f}s) waiting for endpoints for service {service_name} in namespace {namespace}"
                )

            time.sleep(poll_interval)

    def get_status(self, name: str, namespace: str, timeout: float = 60, poll_interval: float = 1.0) -> T_Status:
        start_time = time.time()

        while timeout > time.time() - start_time:
            resource = cast(
                dict[str, Any],
                self._api.get_namespaced_custom_object_status(
                    group=self.group,
                    version=self.version,
                    plural=self.plural,
                    name=name,
                    namespace=namespace,
                ),
            )

            if resource.get("status"):
                return resource["status"]
            else:
                time.sleep(poll_interval)
        else:
            raise TimeoutError(
                f"Timed out ({timeout:.1f}s) waiting for status of {self.kind} {name} in namespace {namespace}"
            )

    def list(self, namespace: str, label_selector: str = "", async_req: bool = False) -> dict[str, Any]:
        from kubernetes.client import ApiException

        try:
            resource: Any = self._api.list_namespaced_custom_object(
                group=self.group,
                version=self.version,
                plural=self.plural,
                namespace=namespace,
                label_selector=label_selector,
                async_req=async_req,
            )
            if "items" in resource:
                return resource
            else:
                return {}
        except ApiException as e:
            if e.status == 404:
                return {}

            raise

    def get(self, name: str, namespace: str) -> dict[str, Any]:
        from kubernetes.client import ApiException

        try:
            resource: Any = self._api.get_namespaced_custom_object(
                group=self.group,
                version=self.version,
                plural=self.plural,
                name=name,
                namespace=namespace,
            )
            return resource
        except ApiException as e:
            if e.status == 404:
                return {}
            raise

    def create(self, body: dict[str, Any], namespace: str) -> Any:
        return self._api.create_namespaced_custom_object(
            group=self.group,
            version=body.get("apiVersion", f"{self.group}/{self.version}").split("/")[1],
            plural=self.plural,
            body=body,
            namespace=namespace,
        )

    def delete(self, name: str, namespace: str):
        return self._api.delete_namespaced_custom_object(
            group=self.group,
            version=self.version,
            plural=self.plural,
            name=name,
            namespace=namespace,
        )

    def update(self, name: str, namespace: str, body: Any):
        return self._api.patch_namespaced_custom_object(
            group=self.group,
            version=self.version,
            plural=self.plural,
            name=name,
            body=body,
            namespace=namespace,
        )

    def update_json_patch(self, name: str, namespace: str, body: Any) -> Any:
        data, status_code, headers = self._api.api_client.call_api(  # pyright: ignore[reportGeneralTypeIssues]
            f"/apis/{self.group}/{self.version}/namespaces/{namespace}/{self.plural}/{name}",
            "PATCH",
            header_params={"Content-Type": "application/json-patch+json"},
            body=body,
            response_type="object",
            auth_settings=["BearerToken"],
            _preload_content=True,
            async_req=False,
        )
        return data

    def wait_until_exists(self, name: str, namespace: str, timeout: float = 60.0, poll_interval: float = 1.0) -> None:
        from kubernetes.client import ApiException

        start_time = time.time()

        while not timeout < time.time() - start_time:
            try:
                self.get(name=name, namespace=namespace)
                return
            except ApiException as e:
                if e.status == 404:
                    time.sleep(poll_interval)
                else:
                    raise

        raise TimeoutError(
            f"Timed out ({timeout:.1f}s) waiting for existence of {self.kind} {name} in namespace {namespace}"
        )
