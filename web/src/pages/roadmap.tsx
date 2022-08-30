import React from 'react';

export default function Home(): JSX.Element {
  const url = 'https://github.com/orgs/tenzir/projects/8/views/1';
  React.useEffect(() => {
    window.location.replace(url)
  }, [])
  return <></>;
}
