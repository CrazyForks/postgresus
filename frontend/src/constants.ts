export function getApplicationServer() {
  const origin = window.location.origin;
  const url = new URL(origin);

  const isDevelopment = import.meta.env.MODE === 'development';

  if (isDevelopment) {
    return `${url.protocol}//${url.hostname}:4005`;
  } else {
    return `${url.protocol}//${url.hostname}:${url.port || (url.protocol === 'https:' ? '443' : '80')}`;
  }
}

export const GOOGLE_DRIVE_OAUTH_REDIRECT_URL = 'https://postgresus.com/storages/google-oauth';
