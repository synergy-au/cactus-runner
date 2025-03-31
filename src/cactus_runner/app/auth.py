from aiohttp import web
from envoy.server.api.depends.lfdi_auth import LFDIAuthDepends

from cactus_runner.app.shared import APPKEY_AGGREGATOR


def request_is_authorized(request: web.Request) -> bool:
    """Returns true if the certificate in the request header matches the registered aggregator's certificate"""
    certificate = request.headers["ssl-certificate"]
    aggregator_lfdi = request.app[APPKEY_AGGREGATOR].lfdi
    if aggregator_lfdi is None:
        # We don't have an aggregator lfdi so no verification is possible
        return False

    return lfdi_from_certificate_matches(certificate=certificate, lfdi=aggregator_lfdi)


def lfdi_from_certificate_matches(certificate: str, lfdi: str) -> bool:
    lfdi_from_certificate = LFDIAuthDepends.generate_lfdi_from_pem(certificate)
    return lfdi == lfdi_from_certificate
