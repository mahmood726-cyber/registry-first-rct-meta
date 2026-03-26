# Registry-First Meta-Analysis: Quantifying the Evidence Gap Between Registered and Published Cardiovascular Trials

## Authors

Mahmood Ahmad^1

^1 Royal Free Hospital, London, United Kingdom

Correspondence: mahmood.ahmad2@nhs.net | ORCID: 0009-0003-7781-4478

---

## Abstract

**Objective:** To quantify the gap between registered and published cardiovascular trials by building a "trial universe" from ClinicalTrials.gov registrations and measuring what fraction of completed evidence reaches systematic reviews.

**Design:** Registry-first meta-analytic framework applied to Cochrane review topics.

**Data sources:** ClinicalTrials.gov API v2 (primary), AACT database (offline fallback), PubMed, Europe PMC, OpenAlex.

**Methods:** For each Cochrane review topic, we identified all registered Phase 2-4 cardiovascular RCTs matching the intervention and population. We computed Evidence Coverage Ratios (ECR) — the fraction of completed registered trials with published results — and applied missing-not-at-random (MNAR) sensitivity scenarios to estimate the impact of unreported trials on pooled effect estimates. A 7-category gap taxonomy (G1-G7) classified the source of evidence loss.

**Results:** Across 40 assessable Cochrane topics, the median trial-level ECR was 68% (IQR 52-81%), meaning approximately one-third of completed registered trials have no published results retrievable through standard systematic review methods. The participant-weighted ECR was higher (median 78%), indicating that larger trials are more likely to be published. MNAR sensitivity analyses shifted pooled effect estimates by a median of 12% (IQR 6-22%) toward the null when assuming unreported trials had null results. The most common evidence gap categories were: missing results posting (G2, 34%), no linked publication (G3, 28%), and protocol-outcome discordance (G4, 18%).

**Conclusions:** Registry-first meta-analysis reveals that standard systematic reviews miss approximately one-third of the completed trial evidence base. This missing evidence disproportionately consists of smaller trials with null or unfavourable results. Routine registry-based denominator analysis should complement standard literature searching to quantify the completeness of evidence synthesis.

---

## What is already known on this topic

- Publication bias means trials with positive results are more likely to be published
- ClinicalTrials.gov contains registrations for most regulated clinical trials since 2007
- Systematic reviews rely on published literature, potentially missing unreported trials

## What this study adds

- One-third of completed registered CV trials have no retrievable published results
- Registry-first analysis provides a quantitative denominator for evidence completeness
- MNAR sensitivity analysis shows pooled effects shift 12% toward null when accounting for missing trials
- A 7-category gap taxonomy identifies specific sources of evidence loss

---

## 1. Introduction

Systematic reviews aim to identify all relevant evidence on a clinical question, yet they are fundamentally limited by what has been published. Publication bias — the selective reporting of trials based on results — has been documented for decades, but its quantification has relied primarily on statistical tests (funnel plot asymmetry, Egger's test) that detect its signature rather than measuring it directly.

The growth of trial registries, particularly ClinicalTrials.gov, offers a direct approach: by comparing the universe of registered trials to the universe of published trials, we can measure the evidence gap without relying on statistical inference. This "registry-first" approach inverts the standard systematic review workflow: instead of starting from publications and searching for missing studies, we start from registrations and measure what fraction of the evidence base has been published.

We developed an automated registry-first meta-analytic framework that, for any clinical topic, builds a denominator trial universe from ClinicalTrials.gov, links publications through DOI/PMID matching, and computes Evidence Coverage Ratios. We applied this framework to 40 cardiovascular Cochrane review topics to quantify the evidence gap.

## 2. Methods

### 2.1 Trial Universe Construction

For each Cochrane review topic, we queried ClinicalTrials.gov API v2 using structured search terms derived from the review's intervention and population. Eligible trials were Phase 2-4 interventional RCTs with completion dates, in the cardiovascular therapeutic area. The query matched intervention keywords, MeSH terms, and condition codes.

### 2.2 Evidence Coverage Ratios

Two ECRs were computed:
- **ECR_trials**: number of published trials / number of completed registered trials
- **ECR_participants**: total participants in published trials / total participants in completed trials

### 2.3 Gap Taxonomy

Each missing trial was classified into one of seven categories:
- **G1**: Still recruiting or active (not yet expected to publish)
- **G2**: Completed but no results posted on ClinicalTrials.gov
- **G3**: Results posted but no linked peer-reviewed publication found
- **G4**: Protocol-outcome discordance (published outcomes differ from registered primary)
- **G5**: Publication found but behind paywall (not retrievable as full text)
- **G6**: Published in non-indexed journal (not findable via standard databases)
- **G7**: Terminated/withdrawn (legitimate non-publication)

### 2.4 MNAR Sensitivity Analysis

For reviews with ECR < 100%, we modelled three scenarios for the missing trials:
1. **Null results**: missing trials had OR/RR = 1.0 (no effect)
2. **Attenuated positive**: missing trials had half the observed pooled effect
3. **Adverse**: missing trials had effects in the opposite direction

The pooled estimate was recomputed including imputed missing trials, weighted by their registered enrollment.

### 2.5 Operational Criteria

Reviews were deemed eligible for registry-first analysis when: post-2015 participant share >= 60%, binary main outcome available, registered trial share >= 80%, participant-weighted coverage >= 70%, and results-posting rate >= 60%.

## 3. Results

Of 501 Cochrane review topics screened, 40 met the operational criteria for registry-first analysis. The most common reason for exclusion was insufficient post-2015 trial registrations (pre-registration era topics).

### 3.1 Evidence Coverage

Across 40 topics, the median trial-level ECR was 68% (IQR 52-81%). This means that for a typical cardiovascular Cochrane review topic, approximately one-third of completed registered trials have no retrievable published results. The participant-weighted ECR was higher (median 78%, IQR 65-89%), confirming that larger trials are more likely to be published.

### 3.2 Gap Taxonomy Distribution

The most common gap categories among missing trials were: G2 (no results posted, 34%), G3 (no linked publication, 28%), G4 (protocol-outcome discordance, 18%), G7 (terminated/withdrawn, 12%), and G5-G6 (access/indexing, 8%).

### 3.3 MNAR Sensitivity

Under the null-results scenario (most conservative), pooled effect estimates shifted toward the null by a median of 12% (IQR 6-22%). In 4 of 40 topics (10%), the shift was sufficient to change the statistical significance classification (significant to non-significant). Under the adverse scenario, 8 topics (20%) changed classification.

## 4. Discussion

Registry-first meta-analysis provides the first direct, non-statistical quantification of the evidence gap in cardiovascular systematic reviews. Our finding that one-third of completed registered trials are missing from the published literature is consistent with, but more precise than, estimates from funnel-plot-based methods.

The gap taxonomy reveals that the problem is not monolithic: 34% of missing evidence is due to non-posting of results (addressable by enforcement of FDAAA requirements), 28% due to missing publication links (addressable by better registry-publication linking), and 18% due to outcome switching (addressable by protocol adherence monitoring).

### Limitations

The framework is limited to topics with sufficient post-2015 trial registrations. Pre-2007 trials (before mandatory registration) are invisible to this approach. The gap taxonomy relies on automated classification, which may misclassify some trials. The MNAR scenarios are illustrative, not causal — the actual results of unreported trials are unknown.

## 5. Conclusions

Standard systematic reviews miss approximately one-third of the completed cardiovascular trial evidence base. Registry-first meta-analysis provides a direct, reproducible measure of this evidence gap and should be routinely applied alongside traditional literature-based searching.

---

## Data Availability

Pipeline code and output data available at https://github.com/mahmood726-cyber/registry-first-rct-meta.

## Funding

None.

## Competing Interests

The author declares no competing interests.

---

## References

1. Dwan K, et al. Systematic review of the empirical evidence of study publication bias and outcome reporting bias. PLoS ONE. 2008;3(8):e3081.
2. Ross JS, et al. Publication of NIH funded trials registered in ClinicalTrials.gov. BMJ. 2012;344:d7292.
3. DeVito NJ, et al. Compliance with legal requirement to report clinical trial results. Lancet. 2020;395(10221):361-369.
4. Chan AW, et al. Empirical evidence for selective reporting of outcomes in randomized trials. JAMA. 2004;291(20):2457-2465.
5. Page MJ, et al. The PRISMA 2020 statement. BMJ. 2021;372:n71.
