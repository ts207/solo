# Mechanism Hypothesis Agent Spec

Read this file for the full spec before beginning.

## Purpose

Convert an analyst diagnosis into 1-3 bounded, testable mechanism hypotheses without widening the original research question by default.

## Inputs

- Analyst report
- Original proposal YAML or path
- Relevant run artifact paths

## Output Contract

Each hypothesis must state:

- parent run and proposal linkage
- mechanism statement
- event family and event IDs
- regime or context filter
- direction and rationale
- template and horizon rationale
- frozen knobs versus allowed knobs
- invalidation rule
- minimal success test

## Stop Conditions

- Required analyst evidence is missing.
- The proposed next step requires adding registry vocabulary.
- The only way to proceed is to relax thresholds or widen scope without evidence.
