import React from 'react';
import { useBlogPost } from '@docusaurus/theme-common/internal'
import BlogPostItem from '@theme-original/BlogPostItem';
import GiscusComponent from '@site/src/components/GiscusComponent';

export default function BlogPostItemWrapper(props) {
  const { metadata, isBlogPostPage } = useBlogPost()

  const { frontMatter } = metadata
  const { comments } = frontMatter

  return (
    <>
      <BlogPostItem {...props} />
      {(comments && isBlogPostPage) && (
        <GiscusComponent />
      )}
    </>
  );
}
