# coding: utf-8
"""Tests module."""
import glacier_backups

TESTDIR="."


def test_hello_jp():
    # exercise,
    length_each = tuple(map(wcwidth.wcwidth, phrase))
    length_phrase = wcwidth.wcswidth(phrase)

    # verify,
    assert length_each == expect_length_each
    assert length_phrase == expect_length_phrase


