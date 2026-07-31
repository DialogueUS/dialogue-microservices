import harvest_harvester


def test_harvester_importable() -> None:
    assert harvest_harvester.__doc__ is not None
