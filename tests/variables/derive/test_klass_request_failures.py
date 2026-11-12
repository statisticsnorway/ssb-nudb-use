"""KLASS failures raise by default and support explicit permissive derivation."""

from typing import TypedDict
from unittest.mock import Mock

import pandas as pd
import pytest
import requests

from nudb_use.datasets.nudb_data import NudbData
from nudb_use.datasets.nudb_database import nudb_database
from nudb_use.nudb_logger import logger
from nudb_use.variables import derive
from nudb_use.variables.derive import derive_decorator
from nudb_use.variables.derive.derive_decorator import DeriveError
from nudb_use.variables.derive.derive_decorator import get_derive_function


class DeriveOptions(TypedDict, total=False):
    raise_errors: bool


@pytest.fixture(params=[400, 404, 429, 500, 503, "connection", "timeout", "json"])
def failed_klass_request(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> type[requests.exceptions.RequestException]:
    response = requests.Response()
    response.url = "https://data.ssb.no/api/klass/v1/classifications/36"
    response.status_code = request.param if isinstance(request.param, int) else 200
    response._content = b"Service unavailable"
    send = Mock(return_value=response)
    error_type: type[requests.exceptions.RequestException]
    if request.param == "connection":
        error_type = requests.exceptions.ConnectionError
        send.side_effect = error_type("KLASS connection failed")
    elif request.param == "timeout":
        error_type = requests.exceptions.Timeout
        send.side_effect = error_type("KLASS request timed out")
    elif request.param == "json":
        error_type = requests.exceptions.JSONDecodeError
    else:
        error_type = requests.exceptions.HTTPError
    monkeypatch.setattr(requests.Session, "send", send)
    return error_type


@pytest.mark.parametrize("raise_errors", [None, False, True])
@pytest.mark.parametrize("variable", ["uh_gruppering_nus", "utd_klassetrinn_lav_nus"])
def test_klass_failure_aborts_direct_and_recursive_derivations(
    failed_klass_request: type[requests.exceptions.RequestException],
    variable: str,
    raise_errors: bool | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = pd.DataFrame({"nus2000": ["611101"]})
    function = get_derive_function(variable)
    assert function is not None
    options: DeriveOptions = (
        {} if raise_errors is None else {"raise_errors": raise_errors}
    )
    if raise_errors is False:
        warning = Mock()
        monkeypatch.setattr(logger, "warning", warning)
        result = derive.derive_variables(source, [variable], **options)
        pd.testing.assert_frame_equal(result, source)
        assert any(
            failed_klass_request.__name__ in call.args[0]
            for call in warning.call_args_list
        )
        return

    # Also exercise the individual wrapper's default, independent of derive_variables.
    with pytest.raises(DeriveError) as direct:
        function(source, **options)
    with pytest.raises(DeriveError) as caught:
        derive.derive_variables(source, [variable], **options)
    assert str(direct.value) == str(caught.value)
    error = caught.value
    assert variable in str(error)
    while isinstance(error.__cause__, DeriveError):
        assert str(error.__cause__) in str(error)
        error = error.__cause__
    assert isinstance(error.__cause__, failed_klass_request)
    assert str(error.__cause__) in str(caught.value)
    if variable == "utd_klassetrinn_lav_nus":
        assert "utd_klassetrinn_lav_hoey_nus" in str(caught.value)


def test_klass_failure_aborts_dataset_construction(
    failed_klass_request: type[requests.exceptions.RequestException],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(nudb_database, "_datasets", {})
    with pytest.raises(failed_klass_request):
        NudbData("nuskat")
    assert "nuskat" not in nudb_database._datasets


def test_klass_failure_is_not_treated_as_raw_sql_in_join(
    failed_klass_request: type[requests.exceptions.RequestException],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(nudb_database, "_datasets", {})
    source = NudbData("avslutta", attach_using_init=False)
    with pytest.raises(failed_klass_request):
        source.join("nuskat")


@pytest.mark.parametrize("raise_errors", [None, False, True])
def test_whole_dataset_join_preserves_request_failure(
    monkeypatch: pytest.MonkeyPatch,
    raise_errors: bool | None,
) -> None:
    source = pd.DataFrame({"utd_skoleaar_start": ["2020"]})
    monkeypatch.setattr(derive_decorator, "get_source_data", Mock(return_value=source))
    error = requests.exceptions.ConnectionError("KLASS unavailable")
    monkeypatch.setattr(derive_decorator, "join_variable_data", Mock(side_effect=error))

    def utd_skoleaar_slutt(df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(utd_skoleaar_slutt=df["utd_skoleaar_start"])

    wrapped = derive_decorator.wrap_derive_join_all_data(utd_skoleaar_slutt)
    options: DeriveOptions = (
        {} if raise_errors is None else {"raise_errors": raise_errors}
    )
    if raise_errors is False:
        pd.testing.assert_frame_equal(wrapped(source, **options), source)
    else:
        with pytest.raises(DeriveError, match="KLASS unavailable") as caught:
            wrapped(source, **options)
        assert "utd_skoleaar_slutt" in str(caught.value)
        assert caught.value.__cause__ is error


@pytest.mark.parametrize("raise_errors", [False, True])
def test_whole_dataset_wrapper_forwards_raise_errors(
    monkeypatch: pytest.MonkeyPatch,
    raise_errors: bool,
) -> None:
    source = pd.DataFrame({"utd_skoleaar_start": ["2020"]})
    monkeypatch.setattr(derive_decorator, "get_source_data", Mock(return_value=source))
    monkeypatch.setattr(
        derive_decorator, "join_variable_data", Mock(return_value=source)
    )
    error = ValueError("Original derivation detail")

    def utd_skoleaar_slutt(df: pd.DataFrame) -> pd.DataFrame:
        raise error

    wrapped = derive_decorator.wrap_derive_join_all_data(utd_skoleaar_slutt)
    if raise_errors:
        with pytest.raises(DeriveError, match="Original derivation detail") as caught:
            wrapped(source, raise_errors=raise_errors)
        assert caught.value.__cause__ is error
    else:
        pd.testing.assert_frame_equal(
            wrapped(source, raise_errors=raise_errors), source
        )
