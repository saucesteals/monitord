-- +goose Up
-- +goose StatementBegin

-- V5 is a clean runtime boundary. Operator-owned route and proxy resources from
-- the original schema remain useful, but name-keyed monitor state/history must
-- not coexist with deployment-ID persistence.
DROP TABLE events;
DROP TABLE runs;
DROP TABLE monitors;

-- +goose StatementEnd

-- +goose Down
-- This clean-break migration is intentionally irreversible. Restoring V4
-- runtime tables would create two competing persistence identities.

