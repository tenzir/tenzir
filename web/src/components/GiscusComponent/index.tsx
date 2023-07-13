import React from 'react';
import Giscus from "@giscus/react";
import { useColorMode } from '@docusaurus/theme-common';

export default function GiscusComponent() {
  const { colorMode } = useColorMode();

  return (
    <Giscus
      repo="tenzir/tenzir"
      repoId="MDEwOlJlcG9zaXRvcnk5MzIzMTQ"
      category="Blog"
      categoryId="DIC_kwDOAA452s4CX0IU"
      mapping="url"
      term="Welcome to Tenzir GitHub Discussions!"
      strict="0"
      reactionsEnabled="1"
      emitMetadata="1"
      inputPosition="top"
      theme={colorMode}
      lang="en"
      loading="lazy"
      crossorigin="anonymous"
      async
    />
  );
}
