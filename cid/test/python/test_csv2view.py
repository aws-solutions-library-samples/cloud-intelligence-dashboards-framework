""" Unit tests for csv2view (cid/helpers/csv2view.py)

Tests the helper directly - no AWS credentials or Cid() bootstrap required.
"""
import pytest

from cid.helpers.csv2view import csv2view, escape_sql, escape_text
from cid.exceptions import CidCritical


def test_escape_text_doubles_single_quotes():
    # SQL standard: quotes inside values are escaped by doubling
    assert escape_text("d'e") == "d''e"


def test_escape_sql_replaces_special_characters():
    assert escape_sql('my col-name!') == 'my_col_name_'


def test_basic_csv2view(tmp_path):
    input_file = tmp_path / 'test.csv'
    input_file.write_text("a,b\nc,d'e")
    output_file = tmp_path / 'res.sql'

    csv2view(str(input_file), 'res', str(output_file))

    sql = output_file.read_text()
    assert 'CREATE OR REPLACE VIEW res AS' in sql
    assert "ROW('c', 'd''e')" in sql
    assert '(a, b)' in sql


def test_csv2view_lowercases_names(tmp_path):
    input_file = tmp_path / 'test.csv'
    input_file.write_text('Col A,COLB\n1,2')
    output_file = tmp_path / 'res.sql'

    csv2view(str(input_file), 'MyView', str(output_file))

    sql = output_file.read_text()
    assert 'CREATE OR REPLACE VIEW myview AS' in sql
    assert '(col_a, colb)' in sql


def test_csv2view_missing_file_raises():
    with pytest.raises(CidCritical, match='File not found'):
        csv2view('/nonexistent/file.csv', 'res', '/tmp/out.sql')
