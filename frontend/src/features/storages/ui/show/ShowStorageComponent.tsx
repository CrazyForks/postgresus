import { type Storage, StorageType } from '../../../../entity/storages';
import { getStorageLogoFromType } from '../../../../entity/storages/models/getStorageLogoFromType';
import { getStorageNameFromType } from '../../../../entity/storages/models/getStorageNameFromType';
import { type UserProfile, UserRole } from '../../../../entity/users';
import { ShowAzureBlobStorageComponent } from './storages/ShowAzureBlobStorageComponent';
import { ShowFTPStorageComponent } from './storages/ShowFTPStorageComponent';
import { ShowGoogleDriveStorageComponent } from './storages/ShowGoogleDriveStorageComponent';
import { ShowNASStorageComponent } from './storages/ShowNASStorageComponent';
import { ShowRcloneStorageComponent } from './storages/ShowRcloneStorageComponent';
import { ShowS3StorageComponent } from './storages/ShowS3StorageComponent';
import { ShowSFTPStorageComponent } from './storages/ShowSFTPStorageComponent';

interface Props {
  storage?: Storage;
  user: UserProfile;
}

export function ShowStorageComponent({ storage, user }: Props) {
  if (!storage) return null;

  return (
    <div>
      <div className="mb-1 flex items-center">
        <div className="min-w-[110px]">Type</div>

        {getStorageNameFromType(storage.type)}

        <img
          src={getStorageLogoFromType(storage.type)}
          alt="storageIcon"
          className="ml-1 h-4 w-4"
        />
      </div>

      {storage.isSystem && user.role === UserRole.ADMIN && (
        <div className="mb-1 flex items-center">
          <div className="min-w-[110px]">System storage</div>
          <div>Yes</div>
        </div>
      )}

      <div>{storage?.type === StorageType.S3 && <ShowS3StorageComponent storage={storage} />}</div>

      <div>
        {storage?.type === StorageType.GOOGLE_DRIVE && (
          <ShowGoogleDriveStorageComponent storage={storage} />
        )}
      </div>

      <div>
        {storage?.type === StorageType.NAS && <ShowNASStorageComponent storage={storage} />}
      </div>

      <div>
        {storage?.type === StorageType.AZURE_BLOB && (
          <ShowAzureBlobStorageComponent storage={storage} />
        )}
      </div>

      <div>
        {storage?.type === StorageType.FTP && <ShowFTPStorageComponent storage={storage} />}
      </div>

      <div>
        {storage?.type === StorageType.SFTP && <ShowSFTPStorageComponent storage={storage} />}
      </div>

      <div>
        {storage?.type === StorageType.RCLONE && <ShowRcloneStorageComponent storage={storage} />}
      </div>
    </div>
  );
}
