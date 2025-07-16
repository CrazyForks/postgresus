import { Input } from 'antd';

import type { Notifier } from '../../../../../entity/notifiers';

interface Props {
  notifier: Notifier;
  setNotifier: (notifier: Notifier) => void;
  setIsUnsaved: (isUnsaved: boolean) => void;
}

export function EditDiscordNotifierComponent({ notifier, setNotifier, setIsUnsaved }: Props) {
  return (
    <>
      <div className="flex">
        <div className="min-w-[110px] max-w-[110px] pr-3">Channel webhook URL</div>

        <div className="w-[250px]">
          <Input
            value={notifier?.discordNotifier?.channelWebhookUrl || ''}
            onChange={(e) => {
              if (!notifier?.discordNotifier) return;
              setNotifier({
                ...notifier,
                discordNotifier: {
                  ...notifier.discordNotifier,
                  channelWebhookUrl: e.target.value.trim(),
                },
              });
              setIsUnsaved(true);
            }}
            size="small"
            className="w-full"
            placeholder="1234567890:ABCDEFGHIJKLMNOPQRSTUVWXYZ"
          />
        </div>
      </div>

      <div className="ml-[110px] max-w-[250px]">
        <div className="mt-1 text-xs text-gray-500">
          <strong>How to get Discord webhook URL:</strong>
          <br />
          <br />
          1. Create or select a Discord channel
          <br />
          2. Go to channel settings (gear icon)
          <br />
          3. Navigate to Integrations
          <br />
          4. Create a new webhook
          <br />
          5. Copy the webhook URL
          <br />
          <br />
          <em>Note: make sure make channel private if needed</em>
        </div>
      </div>
    </>
  );
}
