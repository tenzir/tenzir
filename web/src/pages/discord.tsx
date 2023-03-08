import React from 'react';

export default function Home(): JSX.Element {
  const url = 'https://discord.gg/xEyaRSMCPQ';
  React.useEffect(() => {
    window.location.replace(url)
  }, [])
  return <></>;
}
