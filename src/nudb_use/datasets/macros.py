from nudb_config import settings

VIDEREUTDANNING_UHGRUPPE = tuple(settings.constants.videreutd_uhgrupper)
VENSTRESENSUR = settings.constants.venstresensur
_MACRO = "CREATE OR REPLACE MACRO"
_UHNUS = ["6", "7", "8"]


_DUCKDB_MACROS = f"""
{_MACRO} PREP_NUS2000(nus2000) AS
    COALESCE(nus2000, '999999');


{_MACRO} PREP_UTD_KLASSETRINN(utd_klassetrinn) AS
    LPAD(COALESCE(CAST(utd_klassetrinn AS VARCHAR), '00'), 2, '0');


{_MACRO} PREP_UHGRUPPE(uh_gruppering_nus) AS
    LPAD(COALESCE(CAST(uh_gruppering_nus AS VARCHAR), '00'), 2, '0');


{_MACRO} PREP_UTD_SKOLEAAR_START(utd_skoleaar_start) AS
    COALESCE(CAST(utd_skoleaar_start AS VARCHAR), '{VENSTRESENSUR}');


{_MACRO} YEAR_PLUS_ONE(utd_skoleaar_start) AS
    CAST(utd_skoleaar_start AS INTEGER) + 1;


{_MACRO} STOP_DATE_FROM_SCHOOL_YEAR(utd_skoleaar_start) AS
    make_date(YEAR_PLUS_ONE(utd_skoleaar_start), 1, 1);


{_MACRO} PREP_UTD_AKTIVITET_SLUTT(
    utd_aktivitet_slutt,
    uh_eksamen_dato,
    utd_skoleaar_start
) AS
    COALESCE(utd_aktivitet_slutt, uh_eksamen_dato, STOP_DATE_FROM_SCHOOL_YEAR(utd_skoleaar_start));


{_MACRO} PREP_UH_EKSAMEN_STUDPOENG(uh_eksamen_studpoeng) AS
    COALESCE(uh_eksamen_studpoeng, 0);


{_MACRO} IS_EKSAMENER_120_STUDP(uh_eksamen_dato, uh_eksamen_studpoeng, uh_gruppering_nus) AS
    uh_eksamen_dato IS NOT NULL AND
    uh_eksamen_studpoeng IS NOT NULL AND
    uh_eksamen_studpoeng > 0 AND
    uh_gruppering_nus NOT IN {VIDEREUTDANNING_UHGRUPPE};


{_MACRO} TRINN_PLASSERING(
   nus2000,
   nivaa2000,
   uh_eksamen_dato,
   uh_eksamen_studpoeng,
   utd_aktivitet_slutt,
   is_eksamener_120_studp,
   utd_klassetrinn
) AS
    CASE
        WHEN nivaa2000 IN {_UHNUS} AND NOT is_eksamener_120_studp                                                         THEN '4'
        WHEN nivaa2000 IN {_UHNUS} AND     is_eksamener_120_studp                                                         THEN '3'
        WHEN nivaa2000 == '3'      AND utd_klassetrinn IN ['10', '11']  AND utd_aktivitet_slutt >= make_date(1975, 10, 1) THEN '1'
        WHEN nivaa2000 == '3'                                           AND utd_aktivitet_slutt >= make_date(1995, 10, 1) THEN '1'
        WHEN nus2000 == '999999'                                                                                          THEN '0'
                                                                                                                          ELSE '2'
    END;


{_MACRO} DATE2STR(x) AS
    strftime(x, '%Y%m');


{_MACRO} INVERT_DATE(x) AS
    lpad(CAST(999999 - CAST(DATE2STR(x) AS INTEGER) AS VARCHAR), 6, '0');


{_MACRO} UTD_HOEYESTE_AAR(utd_hoeyeste_dato) AS
    CASE
        WHEN MONTH(utd_hoeyeste_dato) <= 10 THEN YEAR(utd_hoeyeste_dato)
                                            ELSE YEAR(utd_hoeyeste_dato) + 1
    END;


{_MACRO} UTD_HOEYESTE_RANGERING(
    _nus2000,
    _uh_eksamen_dato,
    _uh_eksamen_studpoeng,
    _uh_gruppering_nus,
    _utd_aktivitet_slutt,
    _utd_klassetrinn,
    _utd_skoleaar_start
) AS (
    /* ======================================================================================================================= */
    /* === Step 0: Handle Missing Values                                                                                   === */
    /* ======================================================================================================================= */

    WITH T0 AS (
        SELECT
            PREP_NUS2000(_nus2000) AS nus2000,
            _uh_eksamen_dato AS uh_eksamen_dato,
            PREP_UH_EKSAMEN_STUDPOENG(_uh_eksamen_studpoeng) AS uh_eksamen_studpoeng,
            PREP_UHGRUPPE(_uh_gruppering_nus) AS uh_gruppering_nus,
            PREP_UTD_AKTIVITET_SLUTT(_utd_aktivitet_slutt, _uh_eksamen_dato, _utd_skoleaar_start) AS utd_aktivitet_slutt,
            PREP_UTD_KLASSETRINN(_utd_klassetrinn) AS utd_klassetrinn,
            PREP_UTD_SKOLEAAR_START(_utd_skoleaar_start) AS utd_skoleaar_start
    ),

    /* ======================================================================================================================= */
    /* === Step 1: Identify 120 Studp Exam Rows and NUS Nivaa                                                              === */
    /* ======================================================================================================================= */

    T1 AS (
        SELECT
            *,
            IS_EKSAMENER_120_STUDP(
                uh_eksamen_dato,
                uh_eksamen_studpoeng,
                uh_gruppering_nus
            ) AS is_eksamener_120_studp,
            SUBSTR(nus2000, 1, 1) AS nivaa2000
        FROM
            T0
    ),


    /* ======================================================================================================================= */
    /* === Step 2: Classify Type of Record                                                                                 === */
    /* ======================================================================================================================= */
    /*     4 = Degree (Grad) From UH (Univseritet og Høgskoler)                                                                */
    /*     3 = Combined Exam Records from UH                                                                                   */
    /*     2 = Other Completed Educations                                                                                      */
    /*     1 = Lower Level Completions From VGS (VG1 and VG2). Prioritized Lower than Grunnskole                               */
    /*     0 = Missing nus2000 code, or nus2000 == '999999'                                                                    */
    /* ======================================================================================================================= */

    T2 AS (
        SELECT
            *,
            TRINN_PLASSERING(
                nus2000,
                nivaa2000,
                uh_eksamen_dato,
                uh_eksamen_studpoeng,
                utd_aktivitet_slutt,
                is_eksamener_120_studp,
                utd_klassetrinn
            ) AS trinn_plassering
        FROM
            T1
    ),

    /* ======================================================================================================================= */
    /* === Step 3: Date Tie Breakers, NUS nivaa, Allmenne Fag and PPU/Forberedende Prøver                                  === */
    /* ======================================================================================================================= */
    /*     For record types 2 and 4 we don't consider the date (yet).                                                          */
    /*     For record type 3 we prioritize old (i.e., the first valid record) records.                                         */
    /*     For record type 0 and 1 we prioritize the latest/newest records.                                                    */
    /*     We overflow nivaa2000 such that 9 gets mapped to 0, as 9 should be prioritized last.                                */
    /*     We identify and down prioritize allmenne fag                                                                        */
    /* ======================================================================================================================= */

    T3 AS (
        SELECT
            *,
            CASE
                WHEN SUBSTR(trinn_plassering, 1, 1) == '3'        THEN INVERT_DATE(utd_aktivitet_slutt)
                WHEN SUBSTR(trinn_plassering, 1, 1) IN ('2', '4') THEN '000000'
                                                                  ELSE DATE2STR(utd_aktivitet_slutt)
            END AS first_date_tiebreak,
            DATE2STR(utd_aktivitet_slutt) AS last_date_tiebreak,
            RIGHT(CAST(CAST(nivaa2000 AS INTEGER) + 1 AS VARCHAR), 1) AS nivaa2000_overflowed,
            CASE WHEN SUBSTR(nus2000, 2, 1) == '0' THEN '0'
                                                   ELSE '1'
            END AS allmenne_fag,
            CASE WHEN uh_gruppering_nus == '01' THEN '0'
                 WHEN uh_gruppering_nus == '23' THEN '1'
                                                ELSE '9'
            END AS ppu_forberedende_proever
        FROM
            T2
    )


    /* ======================================================================================================================= */
    /* === Step 4: Create Ranking Number                                                                                   === */
    /* ======================================================================================================================= */

    SELECT CONCAT(
        trinn_plassering,           /* [   00] [1] Record Type.                                                                */
        first_date_tiebreak,        /* [01-06] [6] First Date Tiebreak with Inverted Date for Exam Records.                    */
        nivaa2000_overflowed,       /* [   07] [1] Nivaa nus2000 overflowed such that 9 is mapped to 0.                        */
        utd_klassetrinn,            /* [09-10] [2] Klassetrinn (Higher = better).                                              */
        allmenne_fag,               /* [   11] [1] Allmenne Fag (Allmenne fag = 0, other = 1).                                 */
        ppu_forberedende_proever,   /* [   12] [1] Forberedene Prøver is Worst (0) PPU is better (1) Other is best (9).        */
        last_date_tiebreak,         /* [13-18] [6] Last Date Tiebreak. Newer is Better.                                        */
        nus2000                     /* [19-24] [6] NUS2000 Tiebreak. Higher NUS2000 is Better.                                 */
    ) FROM T3

);
"""
