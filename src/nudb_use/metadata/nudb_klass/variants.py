import klass
from nudb_config.pydantic.variables import Variable


def klass_variant_search_term_mapping(var_meta: Variable) -> dict[str, str]:
    """Subfunction to get the mapping from klass for the variable.

    Args:
        var_meta: The variable metadata from the settings relating to the derived variable.

    Returns:
        dict[str, str]: The mapping dict from klass.

    Raises:
        TypeError: If the klass_codelist in the config for the variable is not an int, or klass_variant_search_term is not a str.
    """
    klass_codelist_maybe_none: int | None = var_meta.klass_codelist
    if not isinstance(klass_codelist_maybe_none, int):
        raise TypeError(
            "klass_codelist on variable sent to klass_variant_search_term_mapping must be an int in the config."
        )
    else:
        klass_codelist: int = klass_codelist_maybe_none

    search_term_maybe_none: str | None = var_meta.klass_variant_search_term
    if not isinstance(search_term_maybe_none, str):
        raise TypeError(
            "search_term on variable sent to klass_variant_search_term_mapping must be a str in the config."
        )
    else:
        search_term: str = search_term_maybe_none

    variant = (
        klass.KlassClassification(klass_codelist)
        .get_version()  # Future development: Could we support "refdate" in the klass package on this to get the version by date?
        .get_variant(search_term=search_term)
    )
    # Should we log the amount of codes that do not map to a grouping in the variant?
    return variant.to_dict()
