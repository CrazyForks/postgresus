import { ExclamationCircleOutlined, SyncOutlined } from '@ant-design/icons';
import { CheckCircleOutlined } from '@ant-design/icons';
import { Button, Modal, Spin, Tooltip } from 'antd';
import dayjs from 'dayjs';
import { useEffect, useRef, useState } from 'react';

import type { Backup } from '../../../entity/backups';
import { type Database, DatabaseType, type PostgresqlDatabase } from '../../../entity/databases';
import { type Restore, RestoreStatus, restoreApi } from '../../../entity/restores';
import { getUserTimeFormat } from '../../../shared/time';
import { EditDatabaseSpecificDataComponent } from '../../databases/ui/edit/EditDatabaseSpecificDataComponent';

interface Props {
  database: Database;
  backup: Backup;
}

export const RestoresComponent = ({ database, backup }: Props) => {
  const [editingDatabase, setEditingDatabase] = useState<Database>({
    ...database,
    postgresql: database.postgresql
      ? ({
          ...database.postgresql,
          host: undefined,
          port: undefined,
          password: undefined,
        } as unknown as PostgresqlDatabase)
      : undefined,
  });

  const [restores, setRestores] = useState<Restore[]>([]);
  const [isLoading, setIsLoading] = useState(false);

  const [showingRestoreError, setShowingRestoreError] = useState<Restore | undefined>();

  const [isShowRestore, setIsShowRestore] = useState(false);

  const isReloadInProgress = useRef(false);

  const loadRestores = async () => {
    if (isReloadInProgress.current) {
      return;
    }

    isReloadInProgress.current = true;

    try {
      const restores = await restoreApi.getRestores(backup.id);
      setRestores(restores);
    } catch (e) {
      alert((e as Error).message);
    }

    isReloadInProgress.current = false;
  };

  const restore = async (editingDatabase: Database) => {
    try {
      await restoreApi.restoreBackup({
        backupId: backup.id,
        postgresql: editingDatabase.postgresql as PostgresqlDatabase,
      });
      await loadRestores();

      setIsShowRestore(false);
    } catch (e) {
      alert((e as Error).message);
    }
  };

  useEffect(() => {
    setIsLoading(true);
    loadRestores().finally(() => setIsLoading(false));

    const interval = setInterval(() => {
      loadRestores();
    }, 1_000);

    return () => clearInterval(interval);
  }, [backup.id]);

  const isRestoreInProgress = restores.some(
    (restore) => restore.status === RestoreStatus.IN_PROGRESS,
  );

  if (isShowRestore) {
    if (database.type === DatabaseType.POSTGRES) {
      return (
        <>
          <div className="my-5 text-sm">
            Enter info of the database we will restore backup to. During the restore,{' '}
            <u>all the current data will be cleared</u>
            <br />
            <br />
            Make sure the database is not used right now (most likely you do not want to restore the
            data to the same DB where the backup was made)
          </div>

          <EditDatabaseSpecificDataComponent
            database={editingDatabase}
            onCancel={() => setIsShowRestore(false)}
            isShowBackButton={false}
            onBack={() => setIsShowRestore(false)}
            saveButtonText="Restore to this DB"
            isSaveToApi={false}
            onSaved={(database) => {
              setEditingDatabase({ ...database });
              restore(database);
            }}
            isShowDbVersionHint={false}
          />
        </>
      );
    }
  }

  return (
    <div className="mt-5">
      {isLoading ? (
        <div className="flex w-full justify-center">
          <Spin />
        </div>
      ) : (
        <>
          <Button
            className="w-full"
            type="primary"
            disabled={isRestoreInProgress}
            loading={isRestoreInProgress}
            onClick={() => setIsShowRestore(true)}
          >
            Restore from backup
          </Button>

          {restores.length === 0 && (
            <div className="my-5 text-center text-gray-400">No restores yet</div>
          )}

          <div className="mt-5">
            {restores.map((restore) => {
              let restoreDurationMs = 0;
              if (restore.status === RestoreStatus.IN_PROGRESS) {
                restoreDurationMs = Date.now() - new Date(restore.createdAt).getTime();
              } else {
                restoreDurationMs = restore.restoreDurationMs;
              }

              const minutes = Math.floor(restoreDurationMs / 60000);
              const seconds = Math.floor((restoreDurationMs % 60000) / 1000);
              const milliseconds = restoreDurationMs % 1000;
              const duration = `${minutes}m ${seconds}s ${milliseconds}ms`;

              const backupDurationMs = backup.backupDurationMs;
              const expectedRestoreDurationMs = backupDurationMs * 5;
              const expectedRestoreDuration = `${Math.floor(expectedRestoreDurationMs / 60000)}m ${Math.floor((expectedRestoreDurationMs % 60000) / 1000)}s`;

              return (
                <div key={restore.id} className="mb-1 rounded border border-gray-200 p-3 text-sm">
                  <div className="mb-1 flex">
                    <div className="w-[75px] min-w-[75px]">Status</div>

                    {restore.status === RestoreStatus.FAILED && (
                      <Tooltip title="Click to see error details">
                        <div
                          className="flex cursor-pointer items-center text-red-600 underline"
                          onClick={() => setShowingRestoreError(restore)}
                        >
                          <ExclamationCircleOutlined
                            className="mr-2"
                            style={{ fontSize: 16, color: '#ff0000' }}
                          />

                          <div>Failed</div>
                        </div>
                      </Tooltip>
                    )}

                    {restore.status === RestoreStatus.COMPLETED && (
                      <div className="flex items-center">
                        <CheckCircleOutlined
                          className="mr-2"
                          style={{ fontSize: 16, color: '#008000' }}
                        />

                        <div>Successful</div>
                      </div>
                    )}

                    {restore.status === RestoreStatus.IN_PROGRESS && (
                      <div className="flex items-center font-bold text-blue-600">
                        <SyncOutlined spin />
                        <span className="ml-2">In progress</span>
                      </div>
                    )}
                  </div>

                  <div className="mb-1 flex">
                    <div className="w-[75px] min-w-[75px]">Started at</div>
                    <div>
                      {dayjs.utc(restore.createdAt).local().format(getUserTimeFormat().format)} (
                      {dayjs.utc(restore.createdAt).local().fromNow()})
                    </div>
                  </div>

                  <div className="flex">
                    <div className="w-[75px] min-w-[75px]">Duration</div>
                    <div>
                      <div>{duration}</div>
                      <div className="mt-2 text-xs text-gray-500">
                        Expected restoration time usually 3x-5x longer than the backup duration
                        (sometimes less, sometimes more depending on data type)
                        <br />
                        <br />
                        So it is expected to take up to {expectedRestoreDuration} (usually
                        significantly faster)
                      </div>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </>
      )}

      {showingRestoreError && (
        <Modal
          title="Restore error details"
          open={!!showingRestoreError}
          onCancel={() => setShowingRestoreError(undefined)}
          footer={null}
        >
          <div className="text-sm">{showingRestoreError.failMessage}</div>
        </Modal>
      )}
    </div>
  );
};
