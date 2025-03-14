from dagster_ray.kuberay.utils import normalize_k8s_label_values


def test_normalize_k8s_label_values():
    assert normalize_k8s_label_values(
        {
            "foo": "bar",
            "my/label": "my/value",
            "user": "daniel@my.org",
            "user-dirty": "daniel!`~@my.org",
            "alphanumeric": "abc123",
        }
    ) == {
        "foo": "bar",
        "my/label": "myvalue",
        "user": "daniel-my-org",
        "user-dirty": "daniel-my-org",
        "alphanumeric": "abc123",
    }
