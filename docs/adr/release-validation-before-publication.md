# Release validation precedes publication

Date: 2026-10-21

Status: Accepted

## Context

The fork restarts its package version at `1.0.0.0`. Existing downstream Cabal
constraints such as `hw-kafka-client >=5.3 && <6` exclude that version after a
name-only replacement. Publishing a tag before attempting the downstream build
would discover this incompatibility after an immutable release exists.

## Decision

EP-6 validates the complete candidate, including the source distribution and a
scratch downstream build, before publishing its tag. The scratch dependency edit
changes both the package name and bounds in every affected component. Candidate
proof uses the candidate commit; remote proof then fetches the published tag and
confirms that it resolves to that same commit.

The copied requests remain open while release acceptance is outstanding. EP-6
records completion only after the remote tag proof succeeds. Local-path builds
are useful prepublication evidence but cannot substitute for successful remote
retrieval. Published tags are never moved.

## Consequences

Release preparation, publication and remote verification are separate milestones.
Any candidate source change invalidates the affected validation evidence. Request
completion and final plan bookkeeping are a subsequent documentation commit and
do not silently alter the tagged artifact. Repository visibility or credentials
must support the documented downstream retrieval path before publication.

EP-1 will adopt this existing record into the profiled ADR bundle and allocate
its stable handle using OKF.
