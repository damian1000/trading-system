-- One-off repair for state predating the V2 fill ledger: positions rows that no ledger fill
-- derives. Emptying the table lets the consumer's next start (ledger empty, so it seeks to the
-- beginning of the retained stream) rebuild every position THROUGH the ledger, after which the
-- ledger fully derives the table and the positions and limits views agree on their stream
-- position. Precondition, verified on the live estate before deploy: the retained stream covers
-- the whole fill history (the stream-derived limits view equals this table). On a fresh install
-- both tables are empty and this is a no-op.
DELETE FROM positions;
