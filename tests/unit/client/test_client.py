import http
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from cactus_runner.client import RunnerClient
from cactus_runner.models import ClientInteraction, ClientInteractionType, RunnerStatus


@pytest.mark.asyncio
async def test_last_interaction():
    # Arrange
    expected_last_interaction = ClientInteraction(
        interaction_type=ClientInteractionType.PROXIED_REQUEST, timestamp=datetime.now(timezone.utc)
    )
    body = RunnerStatus(status_summary="", last_client_interaction=expected_last_interaction).to_json()
    mock_session = MagicMock()
    mock_session.get.return_value.__aenter__.return_value.status = 200
    mock_session.get.return_value.__aenter__.return_value.text.return_value = body

    # Act
    last_interaction = await RunnerClient.last_interaction(session=mock_session)

    # Assert
    assert isinstance(last_interaction, ClientInteraction)
    assert last_interaction == expected_last_interaction
