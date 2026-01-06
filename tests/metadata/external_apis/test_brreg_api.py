import gzip
from types import SimpleNamespace

import pandas as pd
import pytest
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

    class FakeClient:
        def __enter__(self) -> "FakeClient":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        @staticmethod
        def get_enhet(orgnr: str) -> dict[str, object]:
            calls.append(("enhet", orgnr))
            return {"orgnr": orgnr, "count": 2}

    monkeypatch.setattr(brreg_api, "Client", FakeClient)

    result = brreg_api.get_enhet(" 99 123 ")

    assert result == {"orgnr": "99123", "count": "2"}
    assert ("underenhet", "99123") not in calls


def test_get_enhet_falls_back_to_underenhet(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeClient:
        def __enter__(self) -> "FakeClient":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        @staticmethod
        def get_enhet(orgnr: str) -> None:
            return None

        @staticmethod
        def get_underenhet(orgnr: str) -> dict[str, object]:
            return {"orgnr": orgnr, "name": "Backup AS"}

    monkeypatch.setattr(brreg_api, "Client", FakeClient)

    assert brreg_api.get_enhet("00 777") == {"orgnr": "00777", "name": "Backup AS"}


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


def test_get_enhet_raises_on_unknown_type(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeClient:
        def __enter__(self) -> "FakeClient":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        @staticmethod
        def get_enhet(orgnr: str) -> list[str]:
            return ["unexpected"]

        @staticmethod
        def get_underenhet(orgnr: str) -> None:
            return None

    monkeypatch.setattr(brreg_api, "Client", FakeClient)


def test_search_nace_requires_dot() -> None:
    with pytest.raises(ValueError):
        brreg_api.search_nace(["8510"])


def test_search_nace_builds_dataframe(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeSearch:
        def __init__(self) -> None:
            self.pages = {
                0: [
                    ("total_pages", 1),
                    (
                        "items",
                        [
                            {"orgnr": "1", "name": {"value": "One AS"}},
                            {"orgnr": "2", "tags": [1, 2]},
                        ],
                    ),
                ]
            }

        def get_page(self, page: int) -> list[tuple[str, object]]:
            return self.pages[page]

    class FakeClient:
        def __enter__(self) -> "FakeClient":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        @staticmethod
        def search_enhet(query: SimpleNamespace) -> FakeSearch:
            assert query.naeringskode == ["85.510"]
            return FakeSearch()

    class DummyQuery(SimpleNamespace):
        def __init__(self) -> None:
            super().__init__()
            self.naeringskode: list[str] = []

    monkeypatch.setattr(brreg_api, "Client", FakeClient)
    monkeypatch.setattr(brreg_api, "UnderenhetQuery", DummyQuery)

    df = brreg_api.search_nace(["85.510"])

    assert set(df["orgnr"]) == {"1", "2"}
    assert df.loc[df["orgnr"] == "1", "name_value"].iat[0] == "One AS"
    assert df.loc[df["orgnr"] == "2", "tags"].iat[0] == "1 - 2"


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
