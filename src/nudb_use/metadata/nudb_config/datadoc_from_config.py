from pathlib import Path

from dapla_metadata.datadoc_model.all_optional import model
from dapla_metadata.datasets.core import Datadoc
from dapla_metadata.datasets.dapla_dataset_path_info import DaplaDatasetPathInfo
from nudb_config import settings

from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger


def initial_datadoc_from_nudb_config(path: str | Path) -> Datadoc:
    """Create an initial Datadoc object for a path, based on the NUDB config."""
    # This can be improved later... For now this is ok

    path = Path(path)

    info_path = DaplaDatasetPathInfo(path)
    dataset = info_path.dataset_short_name

    if not dataset or not isinstance(dataset, str):
        raise ValueError("Unable to parse dataset name from path!")

    if dataset not in settings.datasets.keys():
        logger.warning(
            f"Unable to find dataset '{dataset}' in the NUDB config!"
        )  # doesn't stop us from looking at the variables

    meta = Datadoc(path)

    write = list(set(meta.variables_lookup.keys()) & set(settings.variables.keys()))

    with LoggerStack("Adding info to variable fields..."):

        for variable in write:
            logger.info(f"Adding info to {variable} variable...")
            var_settings = settings.variables[variable]

            # not optimal
            meta.variables_lookup[variable].name = model.LanguageStringType(
                root=[
                    model.LanguageStringTypeItem(
                        languageCode="nb", languageText=variable
                    ),
                    model.LanguageStringTypeItem(languageCode="nn", languageText=""),
                    model.LanguageStringTypeItem(languageCode="en", languageText=""),
                ]
            )

            if var_settings.description_short:
                meta.variables_lookup[variable].comment = model.LanguageStringType(
                    root=[
                        model.LanguageStringTypeItem(
                            languageCode="nb",
                            languageText=var_settings.description_short,
                        ),
                        model.LanguageStringTypeItem(
                            languageCode="nn", languageText=""
                        ),
                        model.LanguageStringTypeItem(
                            languageCode="en", languageText=""
                        ),
                    ]
                )

            if var_settings.klass_codelist:
                meta.variables_lookup[variable].classification_uri = model.AnyUrl(
                    f"urn:ssb:classification:klass:{var_settings.klass_codelist}"
                )

    return meta
