import { type Database, DatabaseType, PostgresqlVersion } from '../../../../entity/databases';

interface Props {
  database: Database;
}

const databaseTypeLabels = {
  [DatabaseType.POSTGRES]: 'PostgreSQL',
};

const postgresqlVersionLabels = {
  [PostgresqlVersion.PostgresqlVersion13]: '13',
  [PostgresqlVersion.PostgresqlVersion14]: '14',
  [PostgresqlVersion.PostgresqlVersion15]: '15',
  [PostgresqlVersion.PostgresqlVersion16]: '16',
  [PostgresqlVersion.PostgresqlVersion17]: '17',
  [PostgresqlVersion.PostgresqlVersion18]: '18',
};

export const ShowDatabaseSpecificDataComponent = ({ database }: Props) => {
  return (
    <div>
      <div className="mb-5 flex w-full items-center">
        <div className="min-w-[150px]">Database type</div>
        <div>{database.type ? databaseTypeLabels[database.type] : ''}</div>
      </div>

      {database.type === DatabaseType.POSTGRES && (
        <>
          <div className="mb-1 flex w-full items-center">
            <div className="min-w-[150px]">PG version</div>
            <div>
              {database.postgresql?.version
                ? postgresqlVersionLabels[database.postgresql.version]
                : ''}
            </div>
          </div>

          <div className="mb-1 flex w-full items-center">
            <div className="min-w-[150px] break-all">Host</div>
            <div>{database.postgresql?.host || ''}</div>
          </div>

          <div className="mb-1 flex w-full items-center">
            <div className="min-w-[150px]">Port</div>
            <div>{database.postgresql?.port || ''}</div>
          </div>

          <div className="mb-1 flex w-full items-center">
            <div className="min-w-[150px]">Username</div>
            <div>{database.postgresql?.username || ''}</div>
          </div>

          <div className="mb-1 flex w-full items-center">
            <div className="min-w-[150px]">Password</div>
            <div>{database.postgresql?.password ? '*********' : ''}</div>
          </div>

          <div className="mb-1 flex w-full items-center">
            <div className="min-w-[150px]">DB name</div>
            <div>{database.postgresql?.database || ''}</div>
          </div>

          <div className="mb-1 flex w-full items-center">
            <div className="min-w-[150px]">Use HTTPS</div>
            <div>{database.postgresql?.isHttps ? 'Yes' : 'No'}</div>
          </div>
        </>
      )}
    </div>
  );
};
