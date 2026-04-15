from project.events import score_event_frame, arbitrate_events, EventScoreColumns


def test_score_and_arbitrate_importable():
    assert callable(score_event_frame)
    assert callable(arbitrate_events)


def test_event_score_columns_importable():
    assert isinstance(EventScoreColumns, list)
    assert "event_tradeability_score" in EventScoreColumns
