import re


def normalize_k8s_label_values(labels: dict[str, str]) -> dict[str, str]:
    # prevent errors like:
    # Invalid value: \"daniel@anam.ai\": a valid label must be an empty string or consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyValue',  or 'my_value',  or '12345', regex used for validation is '(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?')

    cleanup_regex = re.compile(r"[^a-zA-Z0-9-_.]+")

    for key, value in labels.items():
        # daniel~!@my.domain -> daniel-my-domain
        labels[key] = cleanup_regex.sub("", value.replace("@", "-").replace(".", "-"))

    return labels
