"""Binary-outcome meta-analysis and MNAR sensitivity routines."""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd


def _continuity_correct(
    e_t: float,
    n_t: float,
    e_c: float,
    n_c: float,
    cc: float = 0.5,
) -> tuple[float, float, float, float]:
    if any(v < 0 for v in (e_t, n_t, e_c, n_c)):
        raise ValueError("Negative cell values are invalid")
    if e_t == 0 or e_c == 0 or e_t == n_t or e_c == n_c:
        return e_t + cc, n_t + cc, e_c + cc, n_c + cc
    return e_t, n_t, e_c, n_c


def _study_log_effect(e_t: float, n_t: float, e_c: float, n_c: float, measure: str = "RR") -> tuple[float, float]:
    e_t, n_t, e_c, n_c = _continuity_correct(e_t, n_t, e_c, n_c)
    non_t = n_t - e_t
    non_c = n_c - e_c

    if measure.upper() == "RR":
        risk_t = e_t / n_t
        risk_c = e_c / n_c
        yi = math.log(risk_t / risk_c)
        vi = 1.0 / e_t - 1.0 / n_t + 1.0 / e_c - 1.0 / n_c
    elif measure.upper() == "OR":
        yi = math.log((e_t / non_t) / (e_c / non_c))
        vi = 1.0 / e_t + 1.0 / non_t + 1.0 / e_c + 1.0 / non_c
    else:
        raise ValueError("measure must be RR or OR")

    return yi, vi


def _pool_fixed(yi: np.ndarray, vi: np.ndarray) -> dict[str, float]:
    w = 1.0 / vi
    mu = float(np.sum(w * yi) / np.sum(w))
    se = float(math.sqrt(1.0 / np.sum(w)))
    lo = mu - 1.96 * se
    hi = mu + 1.96 * se
    return {"mu": mu, "se": se, "ci_low": lo, "ci_high": hi}


def _pool_random_dl(yi: np.ndarray, vi: np.ndarray) -> dict[str, float]:
    k = len(yi)
    fe = _pool_fixed(yi, vi)
    w = 1.0 / vi
    q = float(np.sum(w * (yi - fe["mu"]) ** 2))
    c = float(np.sum(w) - (np.sum(w**2) / np.sum(w)))
    tau2 = max(0.0, (q - (k - 1)) / c) if k > 1 and c > 0 else 0.0

    w_re = 1.0 / (vi + tau2)
    mu = float(np.sum(w_re * yi) / np.sum(w_re))
    se = float(math.sqrt(1.0 / np.sum(w_re)))
    lo = mu - 1.96 * se
    hi = mu + 1.96 * se
    i2 = max(0.0, (q - (k - 1)) / q) if q > 0 and k > 1 else 0.0

    return {
        "mu": mu,
        "se": se,
        "ci_low": lo,
        "ci_high": hi,
        "tau2": tau2,
        "Q": q,
        "I2": i2,
    }


def _exp_summary(summary: dict[str, float], measure: str) -> dict[str, float]:
    out = dict(summary)
    out[f"{measure.lower()}"] = float(math.exp(summary["mu"]))
    out[f"{measure.lower()}_ci_low"] = float(math.exp(summary["ci_low"]))
    out[f"{measure.lower()}_ci_high"] = float(math.exp(summary["ci_high"]))
    return out


def _select_two_arms(group: pd.DataFrame) -> pd.DataFrame:
    # Deterministic fallback:
    # 1) prefer larger arms, 2) prefer intervention-like labels over control/placebo.
    ordered = group.sort_values(["total", "arm"], ascending=[False, True]).head(2).copy()
    if len(ordered) < 2:
        return ordered

    def _is_control_label(label: str) -> bool:
        low = label.lower()
        markers = [
            "placebo",
            "control",
            "usual care",
            "standard care",
            "comparator",
        ]
        return any(m in low for m in markers)

    arm0 = str(ordered.iloc[0].get("arm", ""))
    arm1 = str(ordered.iloc[1].get("arm", ""))
    if _is_control_label(arm0) and not _is_control_label(arm1):
        ordered = ordered.iloc[[1, 0]]
    return ordered


def study_effect_table(outcome_rows: pd.DataFrame, measure: str = "RR") -> pd.DataFrame:
    if outcome_rows.empty:
        return pd.DataFrame(columns=["trial_id", "yi", "vi", "events_t", "n_t", "events_c", "n_c", "participants"])

    work = outcome_rows.copy()
    work = work[work["events"].notna() & work["total"].notna()]
    work = work[work["measure_type"].str.contains("binary", case=False, na=False)]

    rows: list[dict[str, Any]] = []
    for trial_id, g in work.groupby("trial_id"):
        picked = _select_two_arms(g)
        if len(picked) < 2:
            continue

        r1 = picked.iloc[0]
        r2 = picked.iloc[1]
        e_t, n_t = float(r1["events"]), float(r1["total"])
        e_c, n_c = float(r2["events"]), float(r2["total"])

        if n_t <= 0 or n_c <= 0:
            continue

        yi, vi = _study_log_effect(e_t, n_t, e_c, n_c, measure=measure)
        rows.append(
            {
                "trial_id": trial_id,
                "yi": yi,
                "vi": vi,
                "events_t": e_t,
                "n_t": n_t,
                "events_c": e_c,
                "n_c": n_c,
                "participants": n_t + n_c,
            }
        )

    return pd.DataFrame(rows)


def meta_analyze_binary(outcome_rows: pd.DataFrame, measure: str = "RR") -> dict[str, Any]:
    effects = study_effect_table(outcome_rows, measure=measure)
    if not effects.empty:
        effects = effects[np.isfinite(effects["yi"]) & np.isfinite(effects["vi"]) & (effects["vi"] > 0)]
    if effects.empty:
        return {
            "measure": measure.upper(),
            "k": 0,
            "fixed": None,
            "random": None,
            "study_effects": [],
        }

    yi = effects["yi"].to_numpy(dtype=float)
    vi = effects["vi"].to_numpy(dtype=float)
    fixed = _exp_summary(_pool_fixed(yi, vi), measure.upper())
    random = _exp_summary(_pool_random_dl(yi, vi), measure.upper())

    return {
        "measure": measure.upper(),
        "k": int(len(effects)),
        "fixed": fixed,
        "random": random,
        "study_effects": effects.to_dict(orient="records"),
    }


def _conclusion_from_ci(ci_low: float, ci_high: float) -> str:
    if ci_high < 1.0:
        return "benefit"
    if ci_low > 1.0:
        return "harm"
    return "uncertain"


def mnar_sensitivity(
    observed_meta: dict[str, Any],
    study_effects: pd.DataFrame,
    *,
    n_missing_trials: int,
    delta_values: tuple[float, float, float] = (0.0, 0.10, 0.20),
    measure: str = "RR",
) -> dict[str, Any]:
    if study_effects.empty or observed_meta.get("random") is None:
        return {"scenarios": {}, "fragility_shift": {"changed_under_S1": False, "changed_under_S2": False}}

    yi = study_effects["yi"].to_numpy(dtype=float)
    vi = study_effects["vi"].to_numpy(dtype=float)
    mask = np.isfinite(yi) & np.isfinite(vi) & (vi > 0)
    yi = yi[mask]
    vi = vi[mask]
    if len(yi) == 0:
        return {"scenarios": {}, "fragility_shift": {"changed_under_S1": False, "changed_under_S2": False}}

    observed_mu = float(observed_meta["random"]["mu"])
    mean_vi = float(np.mean(vi))
    if not np.isfinite(mean_vi) or mean_vi <= 0:
        mean_vi = 1e-6

    scenarios: dict[str, Any] = {}
    labels = ["S0", "S1", "S2"]
    for label, delta in zip(labels, delta_values, strict=True):
        if n_missing_trials > 0:
            missing_y = np.repeat(observed_mu + delta, n_missing_trials)
            missing_v = np.repeat(mean_vi, n_missing_trials)
            yi_adj = np.concatenate([yi, missing_y])
            vi_adj = np.concatenate([vi, missing_v])
        else:
            yi_adj = yi
            vi_adj = vi

        pooled = _exp_summary(_pool_random_dl(yi_adj, vi_adj), measure.upper())
        scenarios[label] = {
            "delta_log_effect": delta,
            "n_missing_trials": n_missing_trials,
            "random": pooled,
        }

    worst_delta = 0.40
    if n_missing_trials > 0:
        yi_w = np.concatenate([yi, np.repeat(observed_mu + worst_delta, n_missing_trials)])
        vi_w = np.concatenate([vi, np.repeat(mean_vi, n_missing_trials)])
    else:
        yi_w, vi_w = yi, vi
    worst = _exp_summary(_pool_random_dl(yi_w, vi_w), measure.upper())

    obs_conclusion = _conclusion_from_ci(
        observed_meta["random"][f"{measure.lower()}_ci_low"],
        observed_meta["random"][f"{measure.lower()}_ci_high"],
    )
    s1_conclusion = _conclusion_from_ci(
        scenarios["S1"]["random"][f"{measure.lower()}_ci_low"],
        scenarios["S1"]["random"][f"{measure.lower()}_ci_high"],
    )
    s2_conclusion = _conclusion_from_ci(
        scenarios["S2"]["random"][f"{measure.lower()}_ci_low"],
        scenarios["S2"]["random"][f"{measure.lower()}_ci_high"],
    )

    return {
        "scenarios": scenarios,
        "worst_plausible_bound": {
            "delta_log_effect": worst_delta,
            "random": worst,
        },
        "fragility_shift": {
            "observed_conclusion": obs_conclusion,
            "S1_conclusion": s1_conclusion,
            "S2_conclusion": s2_conclusion,
            "changed_under_S1": obs_conclusion != s1_conclusion,
            "changed_under_S2": obs_conclusion != s2_conclusion,
        },
    }


def meta_analyze_effect_measure_rows(
    outcome_rows: pd.DataFrame,
    *,
    effect_metric: str = "HR",
) -> dict[str, Any]:
    """Pool trial-level ratio effect measures (e.g., HR) when estimate+CI are available."""
    if outcome_rows.empty:
        return {
            "measure": effect_metric.upper(),
            "k": 0,
            "fixed": None,
            "random": None,
            "study_effects": [],
        }

    work = outcome_rows.copy()
    if "effect_metric" not in work.columns or "effect_estimate" not in work.columns:
        return {
            "measure": effect_metric.upper(),
            "k": 0,
            "fixed": None,
            "random": None,
            "study_effects": [],
        }
    work = work[
        (work["effect_metric"].fillna("").astype(str).str.upper() == effect_metric.upper())
        & (work["effect_estimate"].notna())
    ]
    if work.empty:
        return {
            "measure": effect_metric.upper(),
            "k": 0,
            "fixed": None,
            "random": None,
            "study_effects": [],
        }

    rows: list[dict[str, Any]] = []
    for trial_id, g in work.groupby("trial_id"):
        row = g.iloc[0]
        est = float(row["effect_estimate"])
        if est <= 0:
            continue
        ci_low = row.get("effect_ci_low")
        ci_high = row.get("effect_ci_high")
        if ci_low is None or ci_high is None or float(ci_low) <= 0 or float(ci_high) <= 0:
            # Conservative skip when no usable CI to estimate within-trial variance.
            continue
        yi = math.log(est)
        se = (math.log(float(ci_high)) - math.log(float(ci_low))) / 3.92
        vi = se * se
        if not np.isfinite(yi) or not np.isfinite(vi) or vi <= 0:
            continue
        rows.append(
            {
                "trial_id": trial_id,
                "yi": yi,
                "vi": vi,
                "effect_estimate": est,
                "effect_ci_low": float(ci_low),
                "effect_ci_high": float(ci_high),
            }
        )

    effects = pd.DataFrame(rows)
    if effects.empty:
        return {
            "measure": effect_metric.upper(),
            "k": 0,
            "fixed": None,
            "random": None,
            "study_effects": [],
        }

    yi = effects["yi"].to_numpy(dtype=float)
    vi = effects["vi"].to_numpy(dtype=float)
    fixed = _exp_summary(_pool_fixed(yi, vi), effect_metric.upper())
    random = _exp_summary(_pool_random_dl(yi, vi), effect_metric.upper())
    return {
        "measure": effect_metric.upper(),
        "k": int(len(effects)),
        "fixed": fixed,
        "random": random,
        "study_effects": effects.to_dict(orient="records"),
    }
