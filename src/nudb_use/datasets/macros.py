from nudb_config import settings

VIDEREUTDANNING_UHGRUPPE = tuple(settings.constants.videreutd_uhgrupper)
VENSTRESENSUR = settings.constants.venstresensur
_MACRO = "CREATE OR REPLACE MACRO"
_UHNUS = ["6", "7", "8"]


_DUCKDB_MACROS = f"""
{_MACRO} PREP_NUS2000(nus2000) AS
    COALESCE(nus2000, '999999');


{_MACRO} PREP_UTD_KLASSETRINN(utd_klassetrinn) AS
    COALESCE(CAST(utd_klassetrinn AS VARCHAR), '00');


{_MACRO} PREP_UHGRUPPE(uh_gruppering_nus) AS
    COALESCE(CAST(uh_gruppering_nus AS VARCHAR), '00');


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
   uh_gruppering_nus,
   utd_aktivitet_slutt,
   is_eksamener_120_studp,
   utd_klassetrinn
) AS
    CASE
        WHEN nivaa2000 IN {_UHNUS} AND NOT is_eksamener_120_studp                                                     THEN '4'
        WHEN nivaa2000 IN {_UHNUS} AND     is_eksamener_120_studp                                                     THEN '3'
        WHEN nivaa2000 == '3'      AND utd_klassetrinn IN ['10', '11']  AND utd_aktivitet_slutt >= make_date(1975, 10, 1) THEN '1'
        WHEN nivaa2000 == '3'                                       AND utd_aktivitet_slutt >= make_date(1995, 10, 1) THEN '1'
        WHEN nus2000 == '999999'                                                                                      THEN '0'
                                                                                                                      ELSE '2'
    END;


{_MACRO} DATE2STR(x) AS
    strftime(x, '%Y%m');


{_MACRO} INVERT_DATE(x) AS
    lpad(CAST(999999 - CAST(DATE2STR(x) AS INTEGER) AS VARCHAR), 6, '0');


{_MACRO} UTD_HOEYESTE_RANGERING_PREPPED_LAST(
    nus2000,
    nivaa2000,
    uh_eksamen_dato,
    uh_eksamen_studpoeng,
    uh_gruppering_nus,
    utd_aktivitet_slutt,
    utd_klassetrinn,
    is_eksamener_120_studp,
    utd_skoleaar_start,
    rangering_1,
    rangering_2,
    rangering_3,
    rangering_4,
    rangering_5,
    rangering_6
) AS
    CONCAT(
        rangering_1,
        rangering_2,
        rangering_3,
        rangering_4,
        rangering_5,
        rangering_6,
        DATE2STR(utd_aktivitet_slutt),
        nus2000
    );


{_MACRO} UTD_HOEYESTE_RANGERING_PREPPED_STEP_6(
    nus2000,
    nivaa2000,
    uh_eksamen_dato,
    uh_eksamen_studpoeng,
    uh_gruppering_nus,
    utd_aktivitet_slutt,
    utd_klassetrinn,
    is_eksamener_120_studp,
    utd_skoleaar_start,
    rangering_1,
    rangering_2,
    rangering_3,
    rangering_4,
    rangering_5
) AS
    UTD_HOEYESTE_RANGERING_PREPPED_LAST(
        nus2000,
        nivaa2000,
        uh_eksamen_dato,
        uh_eksamen_studpoeng,
        uh_gruppering_nus,
        utd_aktivitet_slutt,
        utd_klassetrinn,
        is_eksamener_120_studp,
        utd_skoleaar_start,
        rangering_1,
        rangering_2,
        rangering_3,
        rangering_4,
        rangering_5,
        CASE
            WHEN uh_gruppering_nus == '01' THEN '0'
            WHEN uh_gruppering_nus == '23' THEN '1'
                                           ELSE '9'
        END
    );


{_MACRO} UTD_HOEYESTE_RANGERING_PREPPED_STEP_5(
    nus2000,
    nivaa2000,
    uh_eksamen_dato,
    uh_eksamen_studpoeng,
    uh_gruppering_nus,
    utd_aktivitet_slutt,
    utd_klassetrinn,
    is_eksamener_120_studp,
    utd_skoleaar_start,
    rangering_1,
    rangering_2,
    rangering_3,
    rangering_4
) AS
    UTD_HOEYESTE_RANGERING_PREPPED_STEP_6(
        nus2000,
        nivaa2000,
        uh_eksamen_dato,
        uh_eksamen_studpoeng,
        uh_gruppering_nus,
        utd_aktivitet_slutt,
        utd_klassetrinn,
        is_eksamener_120_studp,
        utd_skoleaar_start,
        rangering_1,
        rangering_2,
        rangering_3,
        rangering_4,
        CASE
            WHEN SUBSTR(nus2000, 2, 1) == '0' THEN '0'
                                              ELSE '1'
        END
    );


{_MACRO} UTD_HOEYESTE_RANGERING_PREPPED_STEP_4(
    nus2000,
    nivaa2000,
    uh_eksamen_dato,
    uh_eksamen_studpoeng,
    uh_gruppering_nus,
    utd_aktivitet_slutt,
    utd_klassetrinn,
    is_eksamener_120_studp,
    utd_skoleaar_start,
    rangering_1,
    rangering_2,
    rangering_3
) AS
    UTD_HOEYESTE_RANGERING_PREPPED_STEP_5(
        nus2000,
        nivaa2000,
        uh_eksamen_dato,
        uh_eksamen_studpoeng,
        uh_gruppering_nus,
        utd_aktivitet_slutt,
        utd_klassetrinn,
        is_eksamener_120_studp,
        utd_skoleaar_start,
        rangering_1,
        rangering_2,
        rangering_3,
        lpad(utd_klassetrinn, 2, '0')
    );


{_MACRO} UTD_HOEYESTE_RANGERING_PREPPED_STEP_3(
    nus2000,
    nivaa2000,
    uh_eksamen_dato,
    uh_eksamen_studpoeng,
    uh_gruppering_nus,
    utd_aktivitet_slutt,
    utd_klassetrinn,
    is_eksamener_120_studp,
    utd_skoleaar_start,
    rangering_1,
    rangering_2
) AS
    UTD_HOEYESTE_RANGERING_PREPPED_STEP_4(
        nus2000,
        nivaa2000,
        uh_eksamen_dato,
        uh_eksamen_studpoeng,
        uh_gruppering_nus,
        utd_aktivitet_slutt,
        utd_klassetrinn,
        is_eksamener_120_studp,
        utd_skoleaar_start,
        rangering_1,
        rangering_2,
        CAST(CAST(nivaa2000 AS INTEGER) + 1 AS VARCHAR)
    );


{_MACRO} UTD_HOEYESTE_RANGERING_PREPPED_STEP_2(
    nus2000,
    nivaa2000,
    uh_eksamen_dato,
    uh_eksamen_studpoeng,
    uh_gruppering_nus,
    utd_aktivitet_slutt,
    utd_klassetrinn,
    is_eksamener_120_studp,
    utd_skoleaar_start,
    rangering_1
) AS
    UTD_HOEYESTE_RANGERING_PREPPED_STEP_3(
        nus2000,
        nivaa2000,
        uh_eksamen_dato,
        uh_eksamen_studpoeng,
        uh_gruppering_nus,
        utd_aktivitet_slutt,
        utd_klassetrinn,
        is_eksamener_120_studp,
        utd_skoleaar_start,
        rangering_1,
        CASE
            WHEN SUBSTR(rangering_1, 1, 1) == '3'        THEN INVERT_DATE(utd_aktivitet_slutt)
            WHEN SUBSTR(rangering_1, 1, 1) IN ('2', '4') THEN '000000'
                                                         ELSE DATE2STR(utd_aktivitet_slutt)
        END
    );


{_MACRO} UTD_HOEYESTE_RANGERING_PREPPED_STEP_1(
    nus2000,
    nivaa2000,
    uh_eksamen_dato,
    uh_eksamen_studpoeng,
    uh_gruppering_nus,
    utd_aktivitet_slutt,
    utd_klassetrinn,
    is_eksamener_120_studp,
    utd_skoleaar_start
) AS
    UTD_HOEYESTE_RANGERING_PREPPED_STEP_2(
        nus2000,
        nivaa2000,
        uh_eksamen_dato,
        uh_eksamen_studpoeng,
        uh_gruppering_nus,
        utd_aktivitet_slutt,
        utd_klassetrinn,
        is_eksamener_120_studp,
        utd_skoleaar_start,
        TRINN_PLASSERING(nus2000, nivaa2000, uh_eksamen_dato, uh_eksamen_studpoeng, ugruppe,
                         utd_aktivitet_slutt, is_eksamener_120_studp, utd_klassetrinn)
    );


{_MACRO} UTD_HOEYESTE_RANGERING_PREPPED(
    nus2000,
    uh_eksamen_dato,
    uh_eksamen_studpoeng,
    uh_gruppering_nus,
    utd_aktivitet_slutt,
    utd_klassetrinn,
    utd_skoleaar_start
) AS
    UTD_HOEYESTE_RANGERING_PREPPED_STEP_1(
        nus2000,
        SUBSTR(nus2000, 1, 1),
        uh_eksamen_dato,
        uh_eksamen_studpoeng,
        uh_gruppering_nus,
        utd_aktivitet_slutt,
        utd_klassetrinn,
        IS_EKSAMENER_120_STUDP(uh_eksamen_dato, uh_eksamen_studpoeng, uh_gruppering_nus),
        utd_skoleaar_start
    );


{_MACRO} UTD_HOEYESTE_RANGERING(
    nus2000,
    uh_eksamen_dato,
    uh_eksamen_studpoeng,
    uh_gruppering_nus,
    utd_aktivitet_slutt,
    utd_klassetrinn,
    utd_skoleaar_start
) AS
    UTD_HOEYESTE_RANGERING_PREPPED(
        PREP_NUS2000(nus2000),
        uh_eksamen_dato,
        PREP_UH_EKSAMEN_STUDPOENG(uh_eksamen_studpoeng),
        PREP_UHGRUPPE(uh_gruppering_nus),
        PREP_UTD_AKTIVITET_SLUTT(utd_aktivitet_slutt, uh_eksamen_dato, utd_skoleaar_start),
        PREP_UTD_KLASSETRINN(utd_klassetrinn),
        PREP_UTD_SKOLEAAR_START(utd_skoleaar_start)
    );
"""
