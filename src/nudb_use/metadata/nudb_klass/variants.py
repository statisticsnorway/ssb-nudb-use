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

    version = (
        klass.KlassClassification(klass_codelist)
        .get_version()  # Future development: Could we support "refdate" in the klass package on this to get the version by date?
    )

    found_variants = {
            k: v
            for k, v in version.variants_simple().items()
            if v.lower().startswith(search_term.lower())  # klass-package does an "in" here, while we need a startswith because of "Varighet" is defined twice as a variant in nus2000
        }
    if len(found_variants) != 1:
        err_msg = f"When searching for a variant that matches your search, we did not find a single match. If you got multiple matches, be more specific in your search term: {list(found_variants.values())}"
        raise ValueError(err_msg)
    
    variant = version.get_variant(next(iter(found_variants.keys())))
    # Should we log the amount of codes that do not map to a grouping in the variant?
    return variant.to_dict()
