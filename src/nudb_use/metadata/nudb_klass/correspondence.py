import klass
from nudb_config.pydantic.variables import Variable

from .klass_utils import find_earliest_latest_klass_version_date


def klass_correspondence_to_mapping(var_meta: Variable) -> dict[str, str | None]:
    """Subfunction to get the mapping from klass for a correspondence to another classifcation.

    Args:
        var_meta: The variable metadata from the settings relating to the derived variable.

    Returns:
        dict[str, str | None]: The mapping dict from klass.

    Raises:
        TypeError: If the klass_codelist in the config for the variable is not an int, or klass_variant_search_term is not a str.
    """
    klass_codelist_maybe_none: int | None = var_meta.klass_codelist
    if not isinstance(klass_codelist_maybe_none, int):
        raise TypeError(
            "klass_codelist on variable sent to klass_correspondence_to_mapping must be an int in the config."
        )
    else:
        klass_codelist: int = klass_codelist_maybe_none

    correspondence_maybe_none: int | None = var_meta.klass_correspondence_to
    if not isinstance(correspondence_maybe_none, int):
        raise TypeError(
            "klass_correspondence_to on variable sent to klass_correspondence_to_mapping must be a int in the config."
        )
    else:
        correspondence_to: int = correspondence_maybe_none

    _first_date, last_date = find_earliest_latest_klass_version_date(klass_codelist)

    correspondence = klass.KlassCorrespondence(
        source_classification_id=correspondence_to,
        target_classification_id=klass_codelist,
        from_date=last_date,  # Future development, do we want to pass time down to this function to not always get the latest versions?
        # to_date=?,
    )

    return correspondence.to_dict()
