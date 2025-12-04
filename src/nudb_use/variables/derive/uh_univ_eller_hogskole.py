import pandas as pd

from .derive_decorator import wrap_derive


@wrap_derive
def uh_univ_eller_hogskole(
    df: pd.DataFrame,
) -> pd.Series:
    """Derive `uh_univ_eller_hogskole` from `utd_utdanningstype` and `utd_datakilde`.

    # noqa: DAR101
    # noqa: DAR201
    """
    # SAS code:
    # if SN07 in ('85.421','85.422') then univ = '1';
    # if kilde = '41'                then univ = '2'; * FS-høgskoler *;
    # if kilde = '48'                then univ = '2'; * Lånekassedata *;
    # if utd = '710'                 then univ = '2'; * fagskoler 5.04.2011 *;

    # I don't think we need to care about SN07, we can use `utd` instead, since there is a one-to-one
    # between `utd` and sn07, if utd is in (401, 402). Looking at `f_utd_kurs` univ seems to be 1
    # if `utd` == 400 (all cases seem to be missing sn07).

    # we do however need kilde to disinguish between Lånekassedata, and data from DBH
    # this does however not seem like enough information, as there is a bunch of 311, 312
    # 620 with missing sn07, which are mapped to 2

    # There seems to be a lot of weird values as well, e.g., rows with kilde = 11
    # and missing sn07 seem to be mapped to 2, but also to missing for a bunch of
    # overlappling utd values

    # use utd to get the initial mappings
    initial_mapping = {
        "400": "1",
        "401": "1",
        "402": "1",
        "211": "2",
        "311": "2",
        # "312": "2", # All records with utd=="312" in f_utd_kurs have missing univ
        "313": "2",  # kanskje feil???
        "710": "2",
        "620": "2",
    }

    univ = df["utd_utdanningstype"].astype("string[pyarrow]").map(initial_mapping)
    kilde = df["utd_datakilde"]

    univ[kilde.isin(("41", "48"))] = "2"  # 41 = FS-Høgskoler, 48 = Lånekassedata

    return univ.astype("string[pyarrow]")
