import type { Period } from '../../databases/model/Period';
import type { Interval } from '../../intervals';
import type { Storage } from '../../storages';
import type { BackupNotificationType } from './BackupNotificationType';

export interface BackupConfig {
  databaseId: string;

  isBackupsEnabled: boolean;
  storePeriod: Period;
  backupInterval?: Interval;
  storage?: Storage;
  sendNotificationsOn: BackupNotificationType[];
  cpuCount: number;
  isRetryIfFailed: boolean;
  maxFailedTriesCount: number;
}
