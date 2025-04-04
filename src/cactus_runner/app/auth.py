import logging

from aiohttp import web
from envoy.server.api.depends.lfdi_auth import LFDIAuthDepends

from cactus_runner.app.shared import APPKEY_AGGREGATOR

logger = logging.getLogger(__name__)


def request_is_authorized(request: web.Request) -> bool:
    """Returns true if the certificate in the request header matches the registered aggregator's certificate"""
    # Certificate forwarded https://kubernetes.github.io/ingress-nginx
    certificate = request.headers["ssl-client-cert"]
    aggregator_lfdi = request.app[APPKEY_AGGREGATOR].lfdi
    if aggregator_lfdi is None:
        # We don't have an aggregator lfdi so no verification is possible
        return False

    logger.debug(f"Registered aggregator lfdi={aggregator_lfdi}")
    logger.debug(f"Certificate from request={certificate}")
    return lfdi_from_certificate_matches(certificate=certificate, lfdi=aggregator_lfdi)


def lfdi_from_certificate_matches(certificate: str, lfdi: str) -> bool:
    lfdi_from_certificate = LFDIAuthDepends.generate_lfdi_from_pem(certificate)
    logger.debug(f"lfdi from certificate={lfdi_from_certificate}")

    return lfdi == lfdi_from_certificate
