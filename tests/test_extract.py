"""
Unit Tests for Data Extraction Module (Level 1)
"""

from unittest.mock import MagicMock, patch

import pytest

from src.extract.extract_taxi_data import download_file


def test_download_file_idempotency(tmp_path):
    """Test that existing files are not re-downloaded."""
    test_file = tmp_path / "sample.parquet"
    test_file.write_text("existing data content")

    # Should return early without making requests.get call
    with patch("requests.get") as mock_get:
        result = download_file("http://dummy.url/file.parquet", test_file)
        assert result == test_file
        mock_get.assert_not_called()


def test_download_file_streaming(tmp_path):
    """Test that remote content is streamed and written to disk."""
    test_file = tmp_path / "downloaded.parquet"

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.headers = {"content-length": "12"}
    mock_response.iter_content.return_value = [b"chunk1", b"chunk2"]
    mock_response.__enter__.return_value = mock_response

    with patch("requests.get", return_value=mock_response):
        result = download_file("http://dummy.url/file.parquet", test_file)
        assert result.exists()
        assert test_file.read_bytes() == b"chunk1chunk2"


def test_download_file_404_raises(tmp_path):
    """Test that HTTP 404 raises a FileNotFoundError."""
    test_file = tmp_path / "not_found.parquet"

    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.__enter__.return_value = mock_response

    with patch("requests.get", return_value=mock_response):
        with pytest.raises(FileNotFoundError):
            download_file("http://dummy.url/nonexistent.parquet", test_file)
