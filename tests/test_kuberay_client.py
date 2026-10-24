import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pytest

from dagster_ray.kuberay.client import RayClusterClient, RayJobClient


@pytest.fixture
def patch_api(request: pytest.FixtureRequest) -> Iterator[tuple[Any, int, list[dict[str, Any]]]]:
    from kubernetes import client

    requests: list[dict[str, Any]] = []
    status = request.param
    response = {"metadata": {"name": "my-resource"}} if status == 200 else {"message": "patch rejected"}

    class Handler(BaseHTTPRequestHandler):
        def do_PATCH(self):  # noqa: N802
            requests.append(
                {
                    "path": self.path,
                    "content_type": self.headers["Content-Type"],
                    "authorization": self.headers.get("Authorization"),
                    "body": json.loads(self.rfile.read(int(self.headers["Content-Length"]))),
                }
            )
            data = json.dumps(response).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, format, *args):
            pass

    with HTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        configuration = client.Configuration()
        configuration.host = f"http://127.0.0.1:{server.server_port}"
        # Client 36 renamed the configuration key to match the auth scheme.
        for key in ("authorization", "BearerToken"):
            configuration.api_key[key] = "test-token"
            configuration.api_key_prefix[key] = "Bearer"
        try:
            with client.ApiClient(configuration) as api_client:
                yield api_client, status, requests
        finally:
            server.shutdown()
            thread.join(timeout=5)


@pytest.mark.parametrize("patch_api", [200, 403, 409, 422], indirect=True)
@pytest.mark.parametrize("client_class", [RayClusterClient, RayJobClient])
def test_update_json_patch(patch_api, client_class):
    from kubernetes.client import ApiException

    api_client, status, requests = patch_api
    ray_client = client_class(api_client=api_client)
    body = (
        [{"op": "add", "path": "/metadata/annotations/dagster~1lock", "value": "heartbeat"}]
        if client_class is RayClusterClient
        else [{"op": "add", "path": "/spec/jobId", "value": "job-id"}]
    )

    if status == 200:
        assert ray_client.update_json_patch("my-resource", "ray", body) == {"metadata": {"name": "my-resource"}}
    else:
        with pytest.raises(ApiException) as exc_info:
            ray_client.update_json_patch("my-resource", "ray", body)
        assert exc_info.value.status == status
        assert exc_info.value.body is not None
        assert json.loads(exc_info.value.body) == {"message": "patch rejected"}

    assert requests == [
        {
            "path": f"/apis/ray.io/v1/namespaces/ray/{ray_client.plural}/my-resource",
            "content_type": "application/json-patch+json",
            "authorization": "Bearer test-token",
            "body": body,
        }
    ]
