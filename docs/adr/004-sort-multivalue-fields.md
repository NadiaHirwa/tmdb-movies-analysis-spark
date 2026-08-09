# ADR 004: Alphabetically sort extracted multi-value fields

## Status
Accepted

## Context
While inspecting extracted columns with `value_counts()`, genres and production countries representing the same real-world combination of values were found split across multiple rows purely due to inconsistent ordering in the TMDb API's response (e.g. "Action|Adventure|Science Fiction" versus "Science Fiction|Action|Adventure" for movies with an identical genre set). This silently fragmented what should have been a single group in any later `groupby()` or `value_counts()` analysis.

## Decision
`extract_names()` in `src/transformations/cleaning.py` sorts extracted values alphabetically before joining them into a pipe-separated string, applied to genres, spoken languages, production countries, and production companies. The same principle is applied to `extract_director()` for movies with multiple directors.

## Consequences
- The same real-world combination of values always produces an identical string, regardless of the order the API happened to return them in.
- Grouping and counting operations on these columns (e.g. ROI by genre, franchise revenue) are now accurate, rather than silently undercounting due to fragmented categories.
- This is covered by a unit test (`test_extract_names_sorts_consistently_regardless_of_input_order`) to guard against regression.

## Alternatives Considered
Cast member names are deliberately **not** sorted alphabetically, unlike the fields above. TMDb's `order` field on cast entries reflects real billing prominence, which is meaningful information; sorting it alphabetically would discard that information rather than fix an inconsistency. This asymmetry is intentional: the decision to sort or not sort a field depends on whether the field's original order carries real-world meaning.