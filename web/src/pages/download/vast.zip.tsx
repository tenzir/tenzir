import React from 'react';

export default function Home(): JSX.Element {
  const url = 'https://github.com/tenzir/vast/archive/refs/heads/stable.zip';
  React.useEffect(() => {
    window.location.replace(url)
  }, [])
  return <></>;
}
