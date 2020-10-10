import json


def get_sources(sources_fp):
    with open(sources_fp) as f:
        sources = json.load(f)

    # add `label` attribute to lowercase name and replace space with underscore
    for source in sources:
        source.update({"label": "_".join(source["name"].lower().split())})

    return sources


SOURCES_MINI = get_sources("sources_mini.json")
SOURCES_ALL = get_sources("sources_all.json")
