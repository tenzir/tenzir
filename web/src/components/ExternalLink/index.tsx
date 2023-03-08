import React from "react";

const ExternalLink = ({ url }) => {
  React.useEffect(() => {
    window.location.replace(url);
  }, []);
  return null;
};

export default ExternalLink;
