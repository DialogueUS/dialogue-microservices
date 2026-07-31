import harvest_orchestrator


def test_orchestrator_importable() -> None:
    assert harvest_orchestrator.__doc__ is not None
