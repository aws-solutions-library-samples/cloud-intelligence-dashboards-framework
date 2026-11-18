""" Version parsing from the dashboard "About" sheet (multi-digit components must not be truncated) """
from unittest.mock import MagicMock

import pytest

from cid.helpers.quicksight.dashboard import Dashboard
from cid.helpers.quicksight.definition import Definition
from cid.helpers.quicksight.version import CidVersion


def _about(text: str) -> dict:
    return {"Sheets": [{"Name": "About", "TextBoxes": [{"Content": text}]}]}


@pytest.mark.parametrize("text, expected", [
    ("CORA v0.0.11", "v0.0.11"),
    ("Version v12.34.56 released", "v12.34.56"),
    ("v1.2.3", "v1.2.3"),
    ("V5.10", "v5.10.0"),          # two-part fallback
])
def test_resolve_version_keeps_all_digits(text, expected):
    assert Definition(_about(text))._raw_version == expected


def test_resolve_version_none_without_version():
    assert Definition(_about("no version here"))._raw_version is None


def test_latest_available_version_loads_definition_lazily():
    """A freshly described Dashboard has no cached CID definition; the property must look it up."""
    qs = MagicMock()
    qs.supported_dashboards = {"cora": {"dashboardId": "cora", "version": "v0.0.11"}}
    dashboard = Dashboard({"DashboardId": "cora", "Name": "CORA", "Arn": "arn", "Version": {}}, qs=qs)
    assert dashboard.latest_available_cid_version == CidVersion("v0.0.11")
