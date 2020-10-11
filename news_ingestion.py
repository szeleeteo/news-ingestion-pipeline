import json
from datetime import datetime, timezone
from pathlib import Path, PosixPath

import feedparser
from dagster import (
    AssetMaterialization,
    Dict,
    EventMetadataEntry,
    Field,
    List,
    Output,
    OutputDefinition,
    PresetDefinition,
    file_relative_path,
    pipeline,
    repository,
    solid,
    usable_as_dagster_type,
)

import data

# NEWS_SOURCES should be easily replaceable by retrieval from database or API
NEWS_SOURCES = data.SOURCES_MINI
# NEWS_SOURCES = data.SOURCES_ALL


@usable_as_dagster_type
class DagsterPath(PosixPath):
    pass


@solid(
    config_schema={"sources": Field(list)},
    output_defs=[
        OutputDefinition(name=source["label"], dagster_type=dict, is_required=False)
        for source in NEWS_SOURCES
    ],
)
def get_all_sources(context):
    for source in context.solid_config["sources"]:
        yield Output(value=source, output_name=source["label"])


@solid(
    output_defs=[
        OutputDefinition(
            name="source_with_entries", dagster_type=dict, is_required=False
        )
    ],
)
def ingest_data(context, source: Dict):
    feed = feedparser.parse(source["feed"])
    source["entries"] = feed.get("entries")
    return source


@solid
def append_meta(context, source: Dict):
    source["updated_at"] = datetime.now(tz=timezone.utc).isoformat()
    return source


@solid(
    output_defs=[
        OutputDefinition(
            dagster_type=DagsterPath,
            name="content_save_path",
        )
    ]
)
def save_raw(context, source: Dict):
    articles = source.get("entries")
    if articles:
        source_dir = (
            Path(file_relative_path(__file__, "data/news/"))
            / f"{source['label']}"
            / "raw"
        )
        updated_at = datetime.fromisoformat(source["updated_at"])
        content_save_dir = (
            source_dir
            / f"{updated_at.year}/{updated_at.month:02d}/{updated_at.day:02d}/"
        )
        content_save_dir.mkdir(parents=True, exist_ok=True)
        content_save_path = content_save_dir / f"{updated_at.strftime('%H%M%S')}.json"

        context.log.info(
            f'Save content from source {source["label"]} at {content_save_path}'
        )

        with open(content_save_path, "w") as f:
            json.dump(articles, f, indent=2)

        yield AssetMaterialization(
            asset_key="ingested_data",
            description="Persisted result to storage",
            metadata_entries=[
                EventMetadataEntry.text(
                    label="Source",
                    text=source["name"],
                ),
                EventMetadataEntry.text(
                    label="Number of articles",
                    text=str(len(source["entries"])),
                ),
                EventMetadataEntry.path(
                    label="Output file path",
                    path=str(content_save_path),
                ),
                EventMetadataEntry.text(
                    label="Timestamp",
                    text=source["updated_at"],
                ),
            ],
        )
        yield Output(DagsterPath(content_save_path), "content_save_path")


@solid
def upload_to_datalake(context, saved_files: List[DagsterPath]):
    uploaded_paths = ["path1", "path2"]
    for saved_file in saved_files:
        context.log.info(f"Upload {saved_file} to datalake")
        # TODO: Upload the file to a S3 bucket and yield Materialization and s3 path

    return uploaded_paths


@solid
def clean_data(context, data: List[str]):
    # For example remove HTML codes
    clean_data = [{}]
    return clean_data


@solid
def transform_data(context, data: List[Dict]):
    # unify schemas for all data
    transformed_data = [{}]
    return transformed_data


@solid
def post_to_datawarehouse(context, data: List[Dict]):
    # upload to structured data to Postgres DB or BiqQuery etc
    pass


@solid
def post_to_microservice(context, data: List[Dict]):
    # upload to structured data to Postgres DB or BiqQuery etc
    pass


@pipeline(
    preset_defs=[
        PresetDefinition(
            name="default",
            run_config={
                "execution": {"multiprocess": {"config": {"max_concurrent": 4}}},
                "storage": {"filesystem": {}},
                "loggers": {"console": {"config": {"log_level": "INFO"}}},
                "solids": {"get_all_sources": {"config": {"sources": NEWS_SOURCES}}},
            },
        )
    ],
)
def news_ingestion_pipeline():
    sources = get_all_sources()

    if sources:
        save_raw_solids = []
        labels = [news_src["label"] for news_src in NEWS_SOURCES]

        for source_solid, label in zip(sources, labels):
            ingest_solid = ingest_data.alias(f"ingest_data_{label}")
            update_solid = append_meta.alias(f"append_meta_{label}")
            save_raw_solid = save_raw.alias(f"save_raw_{label}")
            save_raw_solids.append(
                save_raw_solid(update_solid(ingest_solid(source_solid)))
            )

        transformed_data_solid = transform_data(
            clean_data(upload_to_datalake(save_raw_solids))
        )
        post_to_datawarehouse(transformed_data_solid)
        post_to_microservice(transformed_data_solid)


@repository
def news_ingestion_repository():
    return [news_ingestion_pipeline]
