from drop_hunter.domain_checker import classify_candidate


def test_candidate_high_confidence():
    assert classify_candidate("NOT_FOUND", "NXDOMAIN") == (True, "HIGH")


def test_candidate_medium_confidence():
    assert classify_candidate("NOT_FOUND", "NO_ANSWER") == (True, "MEDIUM")


def test_registered_is_not_candidate():
    assert classify_candidate("REGISTERED", "RESOLVES") == (False, "")
