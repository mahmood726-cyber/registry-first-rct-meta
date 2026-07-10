import pandas as pd

from registry_first_ma.meta import meta_analyze_binary


def test_meta_analyze_binary_rr() -> None:
    df = pd.DataFrame(
        [
            {
                "trial_id": "NCT00000001",
                "outcome_name": "mortality",
                "timepoint": "52 weeks",
                "arm": "treatment",
                "events": 10,
                "total": 100,
                "measure_type": "binary_main_outcome",
                "matched_main_outcome": True,
            },
            {
                "trial_id": "NCT00000001",
                "outcome_name": "mortality",
                "timepoint": "52 weeks",
                "arm": "control",
                "events": 20,
                "total": 100,
                "measure_type": "binary_main_outcome",
                "matched_main_outcome": True,
            },
            {
                "trial_id": "NCT00000002",
                "outcome_name": "mortality",
                "timepoint": "52 weeks",
                "arm": "treatment",
                "events": 30,
                "total": 200,
                "measure_type": "binary_main_outcome",
                "matched_main_outcome": True,
            },
            {
                "trial_id": "NCT00000002",
                "outcome_name": "mortality",
                "timepoint": "52 weeks",
                "arm": "control",
                "events": 40,
                "total": 200,
                "measure_type": "binary_main_outcome",
                "matched_main_outcome": True,
            },
        ]
    )

    result = meta_analyze_binary(df, measure="RR")

    assert result["k"] == 2
    assert result["random"]["rr"] < 1.0
    assert "rr_ci_low" in result["random"]



def _binary_df(rows):
    return pd.DataFrame(
        [
            {
                "trial_id": tid,
                "outcome_name": "mortality",
                "timepoint": "52 weeks",
                "arm": arm,
                "events": ev,
                "total": tot,
                "measure_type": "binary_main_outcome",
                "matched_main_outcome": True,
            }
            for (tid, arm, ev, tot) in rows
        ]
    )


def test_study_log_effect_or_non_zero_matches_escalc() -> None:
    # Regression guard: with no zero cell the correction must not fire and the
    # log-OR / variance must match metafor escalc (measure="OR", ai=10 bi=90 ci=20 di=80).
    from registry_first_ma.meta import _study_log_effect

    yi, vi = _study_log_effect(10, 100, 20, 100, measure="OR")
    assert abs(yi - (-0.8109302162163288)) < 1e-9
    assert abs(vi - 0.1736111111111111) < 1e-9


def test_meta_analyze_binary_or_all_events_arm_does_not_crash() -> None:
    # F2 regression: a 100%-event arm previously raised ZeroDivisionError for OR
    # because the continuity correction never reached the non-event cell.
    df = _binary_df(
        [
            ("NCT00000001", "treatment", 100, 100),
            ("NCT00000001", "control", 50, 100),
            ("NCT00000002", "treatment", 30, 200),
            ("NCT00000002", "control", 40, 200),
        ]
    )
    result = meta_analyze_binary(df, measure="OR")
    assert result["k"] == 2
    import math

    assert math.isfinite(result["random"]["or"])
    assert math.isfinite(result["random"]["or_ci_low"])
    assert math.isfinite(result["random"]["or_ci_high"])


def test_meta_analyze_binary_or_zero_events_arm_does_not_crash() -> None:
    # A 0%-event arm must also pool cleanly under OR.
    df = _binary_df(
        [
            ("NCT00000001", "treatment", 0, 100),
            ("NCT00000001", "control", 10, 100),
            ("NCT00000002", "treatment", 5, 120),
            ("NCT00000002", "control", 12, 120),
        ]
    )
    result = meta_analyze_binary(df, measure="OR")
    assert result["k"] == 2
    import math

    assert math.isfinite(result["random"]["or"])


def test_meta_analyze_binary_single_study_finite_ci() -> None:
    # k=1 must pool without exception and yield a finite CI.
    df = _binary_df(
        [
            ("NCT00000001", "treatment", 10, 100),
            ("NCT00000001", "control", 20, 100),
        ]
    )
    result = meta_analyze_binary(df, measure="RR")
    assert result["k"] == 1
    import math

    assert math.isfinite(result["random"]["rr"])
    assert math.isfinite(result["random"]["rr_ci_low"])
    assert math.isfinite(result["random"]["rr_ci_high"])
    assert result["random"]["rr_ci_low"] <= result["random"]["rr"] <= result["random"]["rr_ci_high"]
