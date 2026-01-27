-- +goose Up
-- +goose StatementBegin

-- Fix backups -> storages relationship (change from RESTRICT to CASCADE)
ALTER TABLE backups
    DROP CONSTRAINT fk_backups_storage_id;

ALTER TABLE backups
    ADD CONSTRAINT fk_backups_storage_id
    FOREIGN KEY (storage_id)
    REFERENCES storages (id)
    ON DELETE CASCADE;

-- Fix databases -> workspaces relationship (add CASCADE)
ALTER TABLE databases
    DROP CONSTRAINT fk_databases_workspace_id;

ALTER TABLE databases
    ADD CONSTRAINT fk_databases_workspace_id
    FOREIGN KEY (workspace_id)
    REFERENCES workspaces (id)
    ON DELETE CASCADE;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

-- Restore original backups -> storages relationship (RESTRICT)
ALTER TABLE backups
    DROP CONSTRAINT fk_backups_storage_id;

ALTER TABLE backups
    ADD CONSTRAINT fk_backups_storage_id
    FOREIGN KEY (storage_id)
    REFERENCES storages (id)
    ON DELETE RESTRICT;

-- Restore original databases -> workspaces relationship (no CASCADE)
ALTER TABLE databases
    DROP CONSTRAINT fk_databases_workspace_id;

ALTER TABLE databases
    ADD CONSTRAINT fk_databases_workspace_id
    FOREIGN KEY (workspace_id)
    REFERENCES workspaces (id);

-- +goose StatementEnd
