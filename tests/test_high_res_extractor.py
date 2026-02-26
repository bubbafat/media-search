"""Tests for high-res MJPEG parser: SOI/EOI buffers and PTS pairing."""

import pytest

from src.video.high_res_extractor import (
    SOI,
    EOI,
    _parse_mjpeg_buffers,
    parse_mjpeg_stream_for_test,
)


def test_parse_mjpeg_buffers_empty_stream():
    """Empty stream yields no buffers."""
    assert _parse_mjpeg_buffers(b"") == []
    assert _parse_mjpeg_buffers(b"no markers here") == []


def test_parse_mjpeg_buffers_single_complete_jpeg():
    """Single SOI...EOI yields one buffer."""
    payload = b"JFIF\x00\x01\x01"
    stream = SOI + payload + EOI
    buffers = _parse_mjpeg_buffers(stream)
    assert len(buffers) == 1
    assert buffers[0] == stream
    assert buffers[0].startswith(SOI) and buffers[0].endswith(EOI)


def test_parse_mjpeg_buffers_multiple_complete_jpegs():
    """Multiple SOI...EOI pairs yield one buffer each."""
    j1 = SOI + b"a" + EOI
    j2 = SOI + b"bb" + EOI
    j3 = SOI + b"ccc" + EOI
    stream = j1 + j2 + j3
    buffers = _parse_mjpeg_buffers(stream)
    assert len(buffers) == 3
    assert buffers[0] == j1
    assert buffers[1] == j2
    assert buffers[2] == j3


def test_parse_mjpeg_buffers_incomplete_at_eof_discarded():
    """Trailing SOI without EOI is discarded."""
    complete = SOI + b"x" + EOI
    incomplete = SOI + b"trailing"
    stream = complete + incomplete
    buffers = _parse_mjpeg_buffers(stream)
    assert len(buffers) == 1
    assert buffers[0] == complete


def test_parse_mjpeg_buffers_only_incomplete_returns_empty():
    """Stream with only SOI and no EOI yields no buffers."""
    stream = SOI + b"no end"
    assert _parse_mjpeg_buffers(stream) == []


def test_parse_mjpeg_stream_for_test_pairs_by_index():
    """Test helper pairs buffers with PTS list by index; shorter length wins."""
    j1 = SOI + b"1" + EOI
    j2 = SOI + b"2" + EOI
    stream = j1 + j2
    pts_list = [1.0, 2.0]
    pairs = parse_mjpeg_stream_for_test(stream, pts_list)
    assert len(pairs) == 2
    assert pairs[0] == (j1, 1.0)
    assert pairs[1] == (j2, 2.0)


def test_parse_mjpeg_stream_for_test_more_pts_than_buffers():
    """When PTS list is longer than buffers, only min(len) pairs returned."""
    j1 = SOI + b"1" + EOI
    stream = j1
    pts_list = [1.0, 2.0, 3.0]
    pairs = parse_mjpeg_stream_for_test(stream, pts_list)
    assert len(pairs) == 1
    assert pairs[0] == (j1, 1.0)


def test_parse_mjpeg_stream_for_test_more_buffers_than_pts():
    """When buffers exceed PTS list, only min(len) pairs returned."""
    j1 = SOI + b"1" + EOI
    j2 = SOI + b"2" + EOI
    stream = j1 + j2
    pts_list = [1.0]
    pairs = parse_mjpeg_stream_for_test(stream, pts_list)
    assert len(pairs) == 1
    assert pairs[0] == (j1, 1.0)
