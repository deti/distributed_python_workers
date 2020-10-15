import unittest
import urllib.request
from unittest import mock
import urllib.error
from http.client import RemoteDisconnected, HTTPException

import worker


class GetUrlTestCase(unittest.TestCase):
    def test_url_opening(self):
        database = mock.MagicMock()
        response = mock.MagicMock(code=200)

        with mock.patch("worker.urllib.request.urlopen") as urlopen:
            urlopen.return_value = response
            worker.get_url(1, "url.url", database)

        urlopen.assert_called_once_with("url.url")
        database.mark_url_done.assert_called_once_with(1, 200)
        self.assertTrue(True)

    def test_url_error_handled(self):
        database = mock.MagicMock()
        with mock.patch("worker.urllib.request.urlopen") as urlopen:
            urlopen.side_effect = urllib.error.URLError("Exception reason")
            worker.get_url(2, "anything", database)
        database.mark_url_error.assert_called_once_with(2)

    def test_remote_disconnected_handled(self):
        database = mock.MagicMock()
        with mock.patch("worker.urllib.request.urlopen") as urlopen:
            urlopen.side_effect = RemoteDisconnected()
            worker.get_url(3, "anything", database)
        database.mark_url_error.assert_called_once_with(3)

    def test_http_exception_handled(self):
        database = mock.MagicMock()
        with mock.patch("worker.urllib.request.urlopen") as urlopen:
            urlopen.side_effect = HTTPException()
            worker.get_url(4, "anything", database)
        database.mark_url_error.assert_called_once_with(4)

    def test_timeout_error_handled(self):
        database = mock.MagicMock()
        with mock.patch("worker.urllib.request.urlopen") as urlopen:
            urlopen.side_effect = TimeoutError()
            worker.get_url(4, "anything", database)
        database.mark_url_error.assert_called_once_with(4)