import re


def normalize_k8s_label_values(labels: dict[str, str]) -> dict[str, str]:
    # prevent errors like:
    # Invalid value: \"daniel@anam.ai\": a valid label must be an empty string or consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyValue',  or 'my_value',  or '12345', regex used for validation is '(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?')
    # and comply with the 63 character limit

    cleanup_regex = re.compile(r"[^a-zA-Z0-9-_.]+")

    banned_starting_characters = ["-", "_", "."]

    for key, value in labels.items():
        # -daniel~!@my.domain -> daniel-my-domain
        with_maybe_bad_start = cleanup_regex.sub("", value.replace("@", "-").replace(".", "-"))
        while with_maybe_bad_start and with_maybe_bad_start[0] in banned_starting_characters:
            with_maybe_bad_start = with_maybe_bad_start[1:]
        labels[key] = with_maybe_bad_start[:63]

    return labels
