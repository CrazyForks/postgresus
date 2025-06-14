import { InfoCircleOutlined } from '@ant-design/icons';
import { Button, Input, InputNumber, Select, Switch, Tooltip } from 'antd';
import { useEffect, useState } from 'react';

import {
  type Database,
  DatabaseType,
  type PostgresqlDatabase,
  PostgresqlVersion,
  databaseApi,
} from '../../../../entity/databases';
import { ToastHelper } from '../../../../shared/toast';

interface Props {
  database: Database;

  isShowCancelButton?: boolean;
  onCancel: () => void;

  isShowBackButton: boolean;
  onBack: () => void;

  saveButtonText?: string;
  isSaveToApi: boolean;
  onSaved: (database: Database) => void;

  isShowDbVersionHint?: boolean;
  isShowDbName?: boolean;
}

export const EditDatabaseSpecificDataComponent = ({
  database,

  isShowCancelButton,
  onCancel,

  isShowBackButton,
  onBack,

  saveButtonText,
  isSaveToApi,
  onSaved,

  isShowDbVersionHint = true,
  isShowDbName = true,
}: Props) => {
  const [editingDatabase, setEditingDatabase] = useState<Database>();
  const [isSaving, setIsSaving] = useState(false);

  const [isConnectionTested, setIsConnectionTested] = useState(false);
  const [isTestingConnection, setIsTestingConnection] = useState(false);

  const testConnection = async () => {
    if (!editingDatabase) return;
    setIsTestingConnection(true);

    try {
      await databaseApi.testDatabaseConnectionDirect(editingDatabase);
      setIsConnectionTested(true);
      ToastHelper.showToast({
        title: 'Connection test passed',
        description: 'You can continue with the next step',
      });
    } catch (e) {
      alert((e as Error).message);
    }

    setIsTestingConnection(false);
  };

  const saveDatabase = async () => {
    if (!editingDatabase) return;

    if (isSaveToApi) {
      setIsSaving(true);

      try {
        await databaseApi.updateDatabase(editingDatabase);
      } catch (e) {
        alert((e as Error).message);
      }

      setIsSaving(false);
    }

    onSaved(editingDatabase);
  };

  useEffect(() => {
    setIsSaving(false);
    setIsConnectionTested(false);
    setIsTestingConnection(false);

    setEditingDatabase({ ...database });
  }, [database]);

  if (!editingDatabase) return null;

  let isAllFieldsFilled = true;
  if (!editingDatabase.postgresql?.version) isAllFieldsFilled = false;
  if (!editingDatabase.postgresql?.host) isAllFieldsFilled = false;
  if (!editingDatabase.postgresql?.port) isAllFieldsFilled = false;
  if (!editingDatabase.postgresql?.username) isAllFieldsFilled = false;
  if (!editingDatabase.postgresql?.password) isAllFieldsFilled = false;
  if (!editingDatabase.postgresql?.database) isAllFieldsFilled = false;

  return (
    <div>
      <div className="mb-5 flex w-full items-center">
        <div className="min-w-[150px]">Database type</div>
        <Select
          value={database.type}
          onChange={(v) => {
            setEditingDatabase({
              ...editingDatabase,
              type: v,
              postgresql: {} as unknown as PostgresqlDatabase,
            } as Database);

            setIsConnectionTested(false);
          }}
          disabled={!!editingDatabase.id}
          size="small"
          className="max-w-[200px] grow"
          options={[{ label: 'PostgreSQL', value: DatabaseType.POSTGRES }]}
          placeholder="Select database type"
        />
      </div>

      {editingDatabase.type === DatabaseType.POSTGRES && (
        <>
          <div className="mb-1 flex w-full items-center">
            <div className="min-w-[150px]">PG version</div>

            <Select
              value={editingDatabase.postgresql?.version}
              onChange={(v) => {
                if (!editingDatabase.postgresql) return;

                setEditingDatabase({
                  ...editingDatabase,
                  postgresql: {
                    ...editingDatabase.postgresql,
                    version: v as PostgresqlVersion,
                  },
                });
                setIsConnectionTested(false);
              }}
              size="small"
              className="max-w-[200px] grow"
              placeholder="Select PG version"
              options={[
                { label: '13', value: PostgresqlVersion.PostgresqlVersion13 },
                { label: '14', value: PostgresqlVersion.PostgresqlVersion14 },
                { label: '15', value: PostgresqlVersion.PostgresqlVersion15 },
                { label: '16', value: PostgresqlVersion.PostgresqlVersion16 },
                { label: '17', value: PostgresqlVersion.PostgresqlVersion17 },
              ]}
            />

            {isShowDbVersionHint && (
              <Tooltip
                className="cursor-pointer"
                title="Please select the version of PostgreSQL you are backing up now. You will be able to restore backup to the same version or higher"
              >
                <InfoCircleOutlined className="ml-2" style={{ color: 'gray' }} />
              </Tooltip>
            )}
          </div>

          <div className="mb-1 flex w-full items-center">
            <div className="min-w-[150px]">Host</div>
            <Input
              value={editingDatabase.postgresql?.host}
              onChange={(e) => {
                if (!editingDatabase.postgresql) return;

                setEditingDatabase({
                  ...editingDatabase,
                  postgresql: {
                    ...editingDatabase.postgresql,
                    host: e.target.value.trim().replace('https://', '').replace('http://', ''),
                  },
                });
              }}
              size="small"
              className="max-w-[200px] grow"
              placeholder="Enter PG host"
            />
          </div>

          <div className="mb-1 flex w-full items-center">
            <div className="min-w-[150px]">Port</div>
            <InputNumber
              type="number"
              value={editingDatabase.postgresql?.port}
              onChange={(e) => {
                if (!editingDatabase.postgresql || e === null) return;

                setEditingDatabase({
                  ...editingDatabase,
                  postgresql: { ...editingDatabase.postgresql, port: e },
                });
                setIsConnectionTested(false);
              }}
              size="small"
              className="max-w-[200px] grow"
              placeholder="Enter PG port"
            />
          </div>

          <div className="mb-1 flex w-full items-center">
            <div className="min-w-[150px]">Username</div>
            <Input
              value={editingDatabase.postgresql?.username}
              onChange={(e) => {
                if (!editingDatabase.postgresql) return;

                setEditingDatabase({
                  ...editingDatabase,
                  postgresql: { ...editingDatabase.postgresql, username: e.target.value.trim() },
                });
              }}
              size="small"
              className="max-w-[200px] grow"
              placeholder="Enter PG username"
            />
          </div>

          <div className="mb-1 flex w-full items-center">
            <div className="min-w-[150px]">Password</div>
            <Input.Password
              value={editingDatabase.postgresql?.password}
              onChange={(e) => {
                if (!editingDatabase.postgresql) return;

                setEditingDatabase({
                  ...editingDatabase,
                  postgresql: { ...editingDatabase.postgresql, password: e.target.value.trim() },
                });
                setIsConnectionTested(false);
              }}
              size="small"
              className="max-w-[200px] grow"
              placeholder="Enter PG password"
            />
          </div>

          {isShowDbName && (
            <div className="mb-1 flex w-full items-center">
              <div className="min-w-[150px]">DB name</div>
              <Input
                value={editingDatabase.postgresql?.database}
                onChange={(e) => {
                  if (!editingDatabase.postgresql) return;

                  setEditingDatabase({
                    ...editingDatabase,
                    postgresql: { ...editingDatabase.postgresql, database: e.target.value.trim() },
                  });
                  setIsConnectionTested(false);
                }}
                size="small"
                className="max-w-[200px] grow"
                placeholder="Enter PG database name (optional)"
              />
            </div>
          )}

          <div className="mb-3 flex w-full items-center">
            <div className="min-w-[150px]">Use HTTPS</div>
            <Switch
              checked={editingDatabase.postgresql?.isHttps}
              onChange={(checked) => {
                if (!editingDatabase.postgresql) return;

                setEditingDatabase({
                  ...editingDatabase,
                  postgresql: { ...editingDatabase.postgresql, isHttps: checked },
                });
                setIsConnectionTested(false);
              }}
              size="small"
            />
          </div>

          <div className="mb-1 flex w-full items-center">
            <div className="min-w-[150px]">CPU count</div>
            <InputNumber
              value={editingDatabase.postgresql?.cpuCount}
              onChange={(e) => {
                if (!editingDatabase.postgresql || e === null) return;

                setEditingDatabase({
                  ...editingDatabase,
                  postgresql: { ...editingDatabase.postgresql, cpuCount: e },
                });
                setIsConnectionTested(false);
              }}
              size="small"
              className="max-w-[200px] grow"
              placeholder="Enter PG CPU count"
              min={1}
              step={1}
            />
            <Tooltip
              className="cursor-pointer"
              title="The amount of CPU can be utilized for backuping or restoring"
            >
              <InfoCircleOutlined className="ml-2" style={{ color: 'gray' }} />
            </Tooltip>
          </div>
        </>
      )}

      <div className="mt-5 flex">
        {isShowCancelButton && (
          <Button className="mr-1" danger ghost onClick={() => onCancel()}>
            Cancel
          </Button>
        )}

        {isShowBackButton && (
          <Button className="mr-auto" type="primary" ghost onClick={() => onBack()}>
            Back
          </Button>
        )}

        {!isConnectionTested && (
          <Button
            type="primary"
            onClick={() => testConnection()}
            loading={isTestingConnection}
            disabled={!isAllFieldsFilled}
            className="mr-5"
          >
            Test connection
          </Button>
        )}

        {isConnectionTested && (
          <Button
            type="primary"
            onClick={() => saveDatabase()}
            loading={isSaving}
            disabled={!isAllFieldsFilled}
            className="mr-5"
          >
            {saveButtonText || 'Save'}
          </Button>
        )}
      </div>
    </div>
  );
};
