from dagger_clients.core import Service, core

REGISTRY_IMAGE = "registry:3"
# containers bound to the registry service under the `registry` alias reach it here
REGISTRY_HOST = "registry:5000"


def registry() -> Service:
    """An empty container registry."""
    return core().container().from_(REGISTRY_IMAGE).with_exposed_port(5000).as_service()
