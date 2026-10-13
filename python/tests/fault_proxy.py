"""Disposable TLS-terminating HTTP proxy that injects real network-level CREATE faults.

Bound to one Docker bridge gateway address (never 0.0.0.0) for a single test. The client trusts
the proxy's throw-away certificate; the proxy forwards the client's own bearer token to the real
API server, so admission sees the true caller identity.

Modes (one-shot, armed per test, matched on a non-dry-run Pod POST):
  drop_response  forward to the API server, withhold the response until `proceed` is set, then
                 reset the client's TCP connection: the server committed, the client never knows.
  hold_request   withhold the request itself until `proceed` is set, then forward it: a delayed
                 CREATE that the API server admits only after the client is gone.
"""

from __future__ import annotations

import http.client
import json
import re
import socket
import ssl
import struct
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit

from fleetright.workload_controller import POD_CREATE_ATTEMPT_ANNOTATION

_POD_COLLECTION = re.compile(r"^/api/v1/namespaces/[^/]+/pods$")
_HOP_BY_HOP = {"connection", "keep-alive", "transfer-encoding", "proxy-authenticate", "upgrade"}


class FaultProxy:
    def __init__(
        self,
        bind: tuple[str, int],
        upstream: str,
        upstream_ca: str,
        certfile: str,
        keyfile: str,
    ) -> None:
        parts = urlsplit(upstream)
        self.upstream = (parts.hostname or "", parts.port or 443)
        self.upstream_context = ssl.create_default_context(cafile=upstream_ca)
        self.mode = "pass"
        self.proceed = threading.Event()
        self.fault_seen = threading.Event()
        self.events: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_args: object) -> None:
                return

            def _handle(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else b""
                url = urlsplit(self.path)
                armed = (
                    self.command == "POST"
                    and _POD_COLLECTION.match(url.path) is not None
                    and "dryRun" not in url.query
                )
                mode = proxy._claim_fault() if armed else "pass"
                event: dict[str, Any] = {"mode": mode, "method": self.command, "path": url.path}
                if mode != "pass":
                    pod = json.loads(body)
                    event["pod_name"] = pod["metadata"]["name"]
                    event["create_id"] = pod["metadata"]["annotations"].get(
                        POD_CREATE_ATTEMPT_ANNOTATION
                    )
                    proxy._record(event)
                    proxy.fault_seen.set()
                    if mode == "hold_request":
                        proxy.proceed.wait(timeout=300)
                connection = http.client.HTTPSConnection(
                    *proxy.upstream, context=proxy.upstream_context, timeout=30
                )
                headers = {
                    key: value
                    for key, value in self.headers.items()
                    if key.lower() not in _HOP_BY_HOP | {"host", "content-length"}
                }
                connection.request(self.command, self.path, body=body or None, headers=headers)
                response = connection.getresponse()
                payload = response.read()
                if mode != "pass":
                    event["upstream_status"] = response.status
                    if response.status < 300:
                        event["pod_uid"] = json.loads(payload)["metadata"]["uid"]
                    proxy._record({**event, "stage": "upstream-completed"})
                if mode == "drop_response":
                    proxy.proceed.wait(timeout=300)
                    # RST instead of a response: the client sees a reset, never a status.
                    self.connection.setsockopt(
                        socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
                    )
                    self.close_connection = True
                    self.connection.close()
                    return
                try:
                    self.send_response(response.status)
                    for key, value in response.getheaders():
                        if key.lower() not in _HOP_BY_HOP | {"content-length"}:
                            self.send_header(key, value)
                    self.send_header("Content-Length", str(len(payload)))
                    self.send_header("Connection", "close")
                    self.end_headers()
                    self.wfile.write(payload)
                except OSError:
                    pass  # the client (controller A) is already dead
                self.close_connection = True

            do_GET = do_POST = do_PATCH = do_PUT = do_DELETE = _handle

        self.server = ThreadingHTTPServer(bind, Handler)
        self.server.daemon_threads = True
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(certfile, keyfile)
        self.server.socket = context.wrap_socket(self.server.socket, server_side=True)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def _claim_fault(self) -> str:
        with self._lock:
            mode, self.mode = self.mode, "pass"
            return mode

    def _record(self, event: dict[str, Any]) -> None:
        with self._lock:
            self.events.append(dict(event))

    def arm(self, mode: str) -> None:
        assert mode in {"drop_response", "hold_request"}
        self.proceed.clear()
        self.fault_seen.clear()
        self.mode = mode

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.proceed.set()
        self.server.shutdown()
        self.server.server_close()
