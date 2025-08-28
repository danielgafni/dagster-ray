from __future__ import annotations

import contextlib
from collections.abc import Generator
from typing import TYPE_CHECKING

from dagster import ConfigurableResource, InitResourceContext
from dagster._annotations import beta
from pydantic import PrivateAttr

from dagster_ray.kuberay.client import RayJobClient
from dagster_ray.kuberay.client.base import load_kubeconfig

if TYPE_CHECKING:
    import kubernetes


@beta
class KubeRayJobClientResource(ConfigurableResource[RayJobClient]):
    kube_context: str | None = None
    kubeconfig_file: str | None = None

    _rayjob_client: RayJobClient = PrivateAttr()
    _k8s_api: kubernetes.client.CustomObjectsApi = PrivateAttr()
    _k8s_core_api: kubernetes.client.CoreV1Api = PrivateAttr()

    @property
    def client(self) -> RayJobClient:
        if self._rayjob_client is None:
            raise ValueError(f"{self.__class__.__name__} not initialized")
        return self._raycluster_client

    @property
    def k8s(self) -> kubernetes.client.CustomObjectsApi:
        if self._k8s_api is None:
            raise ValueError(f"{self.__class__.__name__} not initialized")
        return self._k8s_api

    @property
    def k8s_core(self) -> kubernetes.client.CoreV1Api:
        if self._k8s_core_api is None:
            raise ValueError(f"{self.__class__.__name__} not initialized")
        return self._k8s_core_api

    @contextlib.contextmanager
    def yield_for_execution(self, context: InitResourceContext) -> Generator[RayJobClient, None, None]:
        import kubernetes

        load_kubeconfig(context=self.kube_context, config_file=self.kubeconfig_file)

        self._rayjob_client = RayJobClient(context=self.kube_context, config_file=self.kubeconfig_file)
        self._k8s_api = kubernetes.client.CustomObjectsApi()
        self._k8s_core_api = kubernetes.client.CoreV1Api()

        yield self.client
