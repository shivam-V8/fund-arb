"""Vercel serverless function: GET /api/spreads -> the funding-arb snapshot JSON.

Vercel has no long-running process, so the dashboard's data endpoint runs here
as a stateless function. It reuses all the fetch/compute logic from the root
`fund_arb.py` module (stdlib-only, so no requirements.txt is needed).

The in-memory cache used by the standalone `serve()` doesn't survive between
serverless invocations, so instead we let Vercel's edge CDN cache the response
via Cache-Control: s-maxage — one upstream refresh is shared by all viewers for
the TTL, which is what protects the Hyperliquid/Ondo APIs from per-viewer load.
"""
import json
import os
import sys
from http.server import BaseHTTPRequestHandler

# The function bundle is rooted at api/; add the repo root so `import fund_arb`
# resolves (fund_arb.py is shipped alongside via vercel.json includeFiles).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import fund_arb  # noqa: E402

CACHE_SECONDS = 60


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            snap = fund_arb.snapshot(with_avg24=True)
            body = json.dumps(snap).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header(
                "Cache-Control",
                f"public, s-maxage={CACHE_SECONDS}, "
                f"stale-while-revalidate={CACHE_SECONDS * 5}",
            )
        except Exception as exc:  # surface upstream failures to the UI
            body = json.dumps({"error": str(exc)}).encode("utf-8")
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
