import React from 'react';

export default function Home(): JSX.Element {
  const url = 'https://storage.googleapis.com/tenzir-public-data/vast-static-builds/vast-static-latest.tar.gz';
  React.useEffect(() => {
    window.location.replace(url)
  }, [])
  return <></>;
}
