from os.path import join

from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.adb.pipeline import Pipeline


class TestPipeline:
    def test_pipeline(self, configuration, fixtures_dir, input_dir, config_dir):
        with temp_dir(
            "TestAdb",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )

                pipeline = Pipeline(configuration, retriever, tempdir)

                # Test with one country and one dataflow
                test_countries = [{"id": "AFG", "name": "Afghanistan"}]
                test_dataflows = [{"id": "EGELC", "name": "Energy and Electricity"}]

                output = next(
                    pipeline.get_indicators_per_country(
                        countries=test_countries, dataflows=test_dataflows
                    )
                )

                # Verify data row structure
                row = output["data"][0]
                assert "economy_code" in row
                assert "economy_name" in row
                assert "dataflow" in row
                assert "indicator" in row
                assert "year" in row
                assert "value" in row
                assert "unit_of_measure" in row

                dataset = pipeline.generate_dataset(
                    economy_code=output["economy_code"],
                    rows=output["data"],
                )
                economy_name = output["economy_name"]
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                dataset["notes"] = dataset["notes"].replace("(country)", economy_name)

                assert dataset == {
                    "caveats": "[Terms of use](https://kidb.adb.org/terms)\n",
                    "data_update_frequency": 365,
                    "dataset_date": "[2024-01-01T00:00:00 TO 2025-12-31T23:59:59]",
                    "dataset_source": "Asian Development Bank",
                    "groups": [{"name": "afg"}],
                    "license_id": "cc-by",
                    "maintainer": "b682f6f7-cd7e-4bd4-8aa7-f74138dc6313",
                    "methodology": "[FAQ](https://kidb.adb.org/faq)\n",
                    "name": "afghanistan-key-indicators",
                    "notes": "Key Indicators for Afghanistan presents the latest data on economic, "
                    "financial, social, and environmental development issues in the country.",
                    "owner_org": "c64c8840-933b-4378-91a4-d5063da28879",
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "tags": [
                        {
                            "name": "economics",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "environment",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "development",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "socioeconomics",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Afghanistan - Key Indicators",
                }

                # Check resources
                resources = dataset.get_resources()
                assert len(resources) == 1
                assert resources[0] == {
                    "name": "afg-key-indicators.csv",
                    "description": "Key indicators for Afghanistan",
                    "format": "csv",
                }
