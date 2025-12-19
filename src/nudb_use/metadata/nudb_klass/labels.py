import klass
from nudb_config import settings

from nudb_use.nudb_logger import logger

from .variants import klass_variant_search_term_mapping


# currently missing version handling...
def get_klass_label_mapping(variable: str) -> dict[str, str] | None:
    """Subfunction to get the mapping from klass codes to code labels.

    Args:
        variable: Variable name.

    Returns:
        dict[str, str] | None: The mapping dict from klass, or None if no mapping is found.
    """
    try:
        metadata = settings.variables[variable]
    except KeyError:
        logger.warning(f"Unable to find variable '{variable}' in config!")
        return None

    codelist = metadata.klass_codelist
    variant = metadata.klass_variant_search_term

    if not codelist:
        logger.warning(f"No codelist found for variable '{variable}'!")
        return None

    if variant:
        return klass_variant_search_term_mapping(
            metadata, key="code", value="name", select_level=1
        )
    else:
        codes = klass.KlassClassification(codelist).get_codes()
        return codes.to_dict(key="code", value="name")
