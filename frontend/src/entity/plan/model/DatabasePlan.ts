import type { Period } from '../../databases/model/Period';

export interface DatabasePlan {
  databaseId: string;
  maxBackupSizeMb: number;
  maxBackupsTotalSizeMb: number;
  maxStoragePeriod: Period;
}
