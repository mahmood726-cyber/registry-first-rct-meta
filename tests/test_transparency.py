from datetime import date

from registry_first_ma.models import TrialUniverseRecord
from registry_first_ma.transparency import apply_operational_cutoff


def test_operational_cutoff_passes() -> None:
    decision = apply_operational_cutoff(
        post_2015_participant_share=0.8,
        binary_main_outcome=True,
        registered_trial_share=0.95,
        participant_weighted_coverage=0.75,
        results_posting_rate=0.7,
    )
    assert decision["passed"] is True
    assert decision["decision"].startswith("RUN")


def test_trial_model_smoke() -> None:
    trial = TrialUniverseRecord(
        review_id="r1",
        trial_id="NCT00000001",
        overall_status="COMPLETED",
        study_type="Interventional",
        allocation="Randomized",
        start_date=date(2018, 1, 1),
        primary_completion_date=date(2020, 1, 1),
        enrollment=120,
        sponsor_type="industry",
        has_results=True,
        is_registered=True,
        raw={},
    )
    assert trial.trial_id == "NCT00000001"
