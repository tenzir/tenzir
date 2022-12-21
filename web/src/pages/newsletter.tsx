import React from 'react';

export default function Home(): JSX.Element {
  const url = 'https://webforms.pipedrive.com/f/clRn2zcF1N5NGHAJ4Rzd3mVU6Xr55uL2Dm3z62Np2KUlq6vxaslf6xQ5Te3P1O1A6T';
  React.useEffect(() => {
    window.location.replace(url)
  }, [])
  return <></>;
}
