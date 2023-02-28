import React from 'react';

export default function Home(): JSX.Element {
  const url = 'https://github.com/tenzir/vast/releases/latest/download/vast-linux-static.tar.gz';
  React.useEffect(() => {
    window.location.replace(url)
  }, [])
  return <></>;
}
