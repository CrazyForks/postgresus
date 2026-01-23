import { Modal } from 'antd';
import type { JSX } from 'react';
import { useEffect, useState } from 'react';

import { IS_CLOUD } from '../../../constants';

const STORAGE_KEY = 'databasus_playground_info_dismissed';

const TIMEOUT_SECONDS = 30;

export const PlaygroundWarningComponent = (): JSX.Element => {
  const [isVisible, setIsVisible] = useState(false);
  const [remainingSeconds, setRemainingSeconds] = useState(TIMEOUT_SECONDS);
  const [isButtonEnabled, setIsButtonEnabled] = useState(false);

  const handleClose = () => {
    try {
      localStorage.setItem(STORAGE_KEY, 'true');
    } catch (e) {
      console.warn('Failed to save playground modal state to localStorage:', e);
    }
    setIsVisible(false);
  };

  useEffect(() => {
    if (!IS_CLOUD) {
      return;
    }

    try {
      const isDismissed = localStorage.getItem(STORAGE_KEY) === 'true';
      if (!isDismissed) {
        setIsVisible(true);
      }
    } catch (e) {
      console.warn('Failed to read playground modal state from localStorage:', e);
      setIsVisible(true);
    }
  }, []);

  useEffect(() => {
    if (!isVisible) {
      return;
    }

    const interval = setInterval(() => {
      setRemainingSeconds((prev) => {
        if (prev <= 1) {
          setIsButtonEnabled(true);
          clearInterval(interval);
          return 0;
        }
        return prev - 1;
      });
    }, 1000);

    return () => clearInterval(interval);
  }, [isVisible]);

  return (
    <Modal
      title="Welcome to Databasus Playground"
      open={isVisible}
      onOk={handleClose}
      okText={
        <div className="min-w-[100px]">
          {isButtonEnabled ? 'Understood' : `${remainingSeconds}`}
        </div>
      }
      okButtonProps={{ disabled: !isButtonEnabled }}
      closable={false}
      cancelButtonProps={{ style: { display: 'none' } }}
      width={500}
      centered
      maskClosable={false}
    >
      <div className="space-y-6 py-4">
        <div>
          <h3 className="mb-2 text-lg font-semibold">What is Playground?</h3>
          <p className="text-gray-700 dark:text-gray-300">
            Playground is a dev environment where you can test small databases backup and see
            Databasus in action. Databasus dev team can test new features and see issues which hard
            to detect when using self hosted (without logs or reports)
          </p>
        </div>

        <div>
          <h3 className="mb-2 text-lg font-semibold">What is limit?</h3>
          <ul className="list-disc space-y-1 pl-5 text-gray-700 dark:text-gray-300">
            <li>Single backup size - 100 MB (~1.5 GB database)</li>
            <li>Store period - 7 days</li>
          </ul>
        </div>

        <div>
          <h3 className="mb-2 text-lg font-semibold">Is it secure?</h3>
          <p className="text-gray-700 dark:text-gray-300">
            Yes, it&apos;s regular Databasus installation, secured and maintained by Databasus team.
            More about security{' '}
            <a
              href="https://databasus.com/security"
              target="_blank"
              rel="noopener noreferrer"
              className="text-blue-600 hover:underline dark:text-blue-400"
            >
              you can read here
            </a>
          </p>
        </div>

        <div>
          <h3 className="mb-2 text-lg font-semibold">Can my data be currepted?</h3>
          <p className="text-gray-700 dark:text-gray-300">
            No, because playground use only read-only users and cannot affect your DB. Only issue
            you can face is instability: playground background workers frequently reloaded so backup
            can be slower or be restarted due to app restart
          </p>
        </div>

        <div>
          <h3 className="mb-2 text-lg font-semibold">What if I see an issue?</h3>
          <p className="text-gray-700 dark:text-gray-300">
            Create{' '}
            <a
              href="https://github.com/databasus/databasus/issues"
              target="_blank"
              rel="noopener noreferrer"
              className="text-blue-600 hover:underline dark:text-blue-400"
            >
              GitHub issue
            </a>{' '}
            or write{' '}
            <a
              href="https://t.me/databasus_community"
              target="_blank"
              rel="noopener noreferrer"
              className="text-blue-600 hover:underline dark:text-blue-400"
            >
              to the community
            </a>
          </p>
        </div>
      </div>
    </Modal>
  );
};
