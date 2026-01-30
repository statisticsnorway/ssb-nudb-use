import gzip

import pandas as pd
import pytest
from brreg.enhetsregisteret import Cursor
from brreg.enhetsregisteret import Enhet
from brreg.enhetsregisteret import Organisasjonsform
from brreg.enhetsregisteret import Page
from brreg.enhetsregisteret import Underenhet
from brreg.enhetsregisteret import UnderenhetQuery
from pydantic import BaseModel

from nudb_use.metadata.external_apis import brreg_api


def test_download_csv_content_enheter_parses_gzip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    csv_text = "orgnr,navn\n123456789,Test AS\n"
    gzipped = gzip.compress(csv_text.encode("utf-8"))

    class FakeResponse:
        content = gzipped

        @staticmethod
        def raise_for_status() -> None:
            return None

    def fake_get(url: str) -> FakeResponse:
        assert "lastned/csv" in url
        return FakeResponse()

    monkeypatch.setattr(brreg_api.requests, "get", fake_get)  # type: ignore[attr-defined]

    df = brreg_api.download_csv_content_enheter()

    expected = pd.DataFrame([{"orgnr": "123456789", "navn": "Test AS"}])
    pd.testing.assert_frame_equal(df.reset_index(drop=True), expected)


def test_filter_utd_csv_enheter(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(brreg_api, "UTD_NACEKODER", ["85.200"])

    source_df = pd.DataFrame(
        [
            {
                "orgnr": "1",
                "naeringskode1.kode": "85.200",
                "naeringskode2.kode": "01.100",
                "naeringskode3.kode": None,
            },
            {
                "orgnr": "2",
                "naeringskode1.kode": "01.100",
                "naeringskode2.kode": "02.200",
                "naeringskode3.kode": "03.300",
            },
        ]
    )

    monkeypatch.setattr(brreg_api, "download_csv_content_enheter", lambda: source_df)

    filtered = brreg_api.filter_utd_csv_enheter()

    assert filtered["orgnr"].tolist() == ["1"]


def test_orgnr_is_underenhet(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeClient:
        def __enter__(self) -> "FakeClient":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        @staticmethod
        def get_underenhet(orgnr: str) -> object | None:
            return {"hit": True} if orgnr == "123456789" else None

    monkeypatch.setattr(brreg_api, "Client", FakeClient)

    assert brreg_api.orgnr_is_underenhet("123 456 789") is True
    assert brreg_api.orgnr_is_underenhet("000") is False


def test_get_enhet_prefers_enhet_over_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str]] = []

    organisasjonsform = Organisasjonsform(
        kode="AS",
        beskrivelse="Aksjeselskap",
    )

    class FakeClient:
        def __enter__(self) -> "FakeClient":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        @staticmethod
        def get_enhet(orgnr: str) -> Enhet:
            calls.append(("enhet", orgnr))
            return Enhet(
                organisasjonsnummer=orgnr,
                navn="Test AS",
                organisasjonsform=organisasjonsform,
            )

        @staticmethod
        def get_underenhet(orgnr: str) -> Underenhet:
            calls.append(("underenhet", orgnr))
            return Underenhet(
                organisasjonsnummer=orgnr,
                navn="Fallback",
                organisasjonsform=organisasjonsform,
            )

    monkeypatch.setattr(brreg_api, "Client", FakeClient)

    result = brreg_api.get_enhet(" 99 123 0000 ")

    assert result is not None
    assert result["organisasjonsnummer"] == "991230000"
    assert result["navn"] == "Test AS"
    assert ("underenhet", "991230000") not in calls


def test_get_enhet_falls_back_to_underenhet(monkeypatch: pytest.MonkeyPatch) -> None:
    organisasjonsform = Organisasjonsform(
        kode="AS",
        beskrivelse="Aksjeselskap",
    )

    class FakeClient:
        def __enter__(self) -> "FakeClient":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        @staticmethod
        def get_enhet(orgnr: str) -> None:
            return None

        @staticmethod
        def get_underenhet(orgnr: str) -> Underenhet:
            return Underenhet(
                organisasjonsnummer=orgnr,
                navn="Backup AS",
                organisasjonsform=organisasjonsform,
            )

    monkeypatch.setattr(brreg_api, "Client", FakeClient)

    result = brreg_api.get_enhet("00 777 0000")
    assert result is not None
    assert result["organisasjonsnummer"] == "007770000"
    assert result["navn"] == "Backup AS"


def test_get_enhet_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeClient:
        def __enter__(self) -> "FakeClient":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        @staticmethod
        def get_enhet(orgnr: str) -> None:
            return None

        @staticmethod
        def get_underenhet(orgnr: str) -> None:
            return None

    monkeypatch.setattr(brreg_api, "Client", FakeClient)

    assert brreg_api.get_enhet("11 222") is None


def test_search_nace_requires_dot() -> None:
    with pytest.raises(ValueError):
        brreg_api.search_nace(["8510"])


def test_search_nace_builds_dataframe(monkeypatch: pytest.MonkeyPatch) -> None:
    organisasjonsform = Organisasjonsform(
        kode="AS",
        beskrivelse="Aksjeselskap",
    )

    class FakeClient:
        def __enter__(self) -> "FakeClient":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        @staticmethod
        def search_underenhet(
            query: UnderenhetQuery,
        ) -> Cursor[Underenhet, UnderenhetQuery]:
            assert query.naeringskode == ["85.510"]
            items = [
                Underenhet.model_construct(
                    organisasjonsnummer="123456789",
                    navn="One AS",
                    organisasjonsform=organisasjonsform,
                    frivillig_mva_registrert_beskrivelser=["one", "two"],
                ),
                Underenhet.model_construct(
                    organisasjonsnummer="987654321",
                    navn="Two AS",
                    organisasjonsform=organisasjonsform,
                ),
            ]
            page = Page[Underenhet].model_construct(
                items=items,
                page_size=2,
                page_number=0,
                total_elements=2,
                total_pages=1,
            )
            search = Cursor(
                operation=lambda _: (_ for _ in ()).throw(
                    AssertionError("Unexpected pagination")
                ),
                query=query,
                page=page,
            )
            return search

    monkeypatch.setattr(brreg_api, "Client", FakeClient)

    df = brreg_api.search_nace(["85.510"])

    assert set(df["organisasjonsnummer"]) == {"123456789", "987654321"}
    assert df.loc[df["organisasjonsnummer"] == "123456789", "navn"].iat[0] == "One AS"
    assert (
        df.loc[
            df["organisasjonsnummer"] == "123456789",
            "frivillig_mva_registrert_beskrivelser",
        ].iat[0]
        == "one - two"
    )


def test_flatten_handles_pydantic_model() -> None:
    class Model(BaseModel):
        a: int
        b: str

    result = brreg_api.flatten(Model(a=1, b="x"))

    assert result == {"a": 1, "b": "x"}


def test_flatten_handles_nested_list_objects() -> None:
    data = [{"a": 1}, {"a": 2}]

    result = brreg_api.flatten(data, prefix="items")

    assert result == {"items_0_a": 1, "items_1_a": 2}
