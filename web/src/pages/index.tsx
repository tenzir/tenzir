import React from 'react';
import clsx from 'clsx';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import {useColorMode} from '@docusaurus/theme-common';
import styles from './index.module.css';
import HomepageFeatures from '@site/src/components/HomepageFeatures';

import VASTLight from '@site/static/img/vast-white.svg';
import VASTDark from '@site/static/img/vast-black.svg';
import Carousel from '../components/Carousel';

// NOTE: this is internal and may change, and the types might not be guaranteed
const blogpostsInternalArchive = require('../../.docusaurus/docusaurus-plugin-content-blog/default/blog-archive-80c.json');

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  const {colorMode} = useColorMode();
  return (
    <>
      <header>
        <div className={clsx('hero', styles.heroBanner)}>
          <div className="container">
            {colorMode === 'dark' ? (
              <VASTLight className={styles.vastLogo} title="VAST Logo" />
            ) : (
              <VASTDark className={styles.vastLogo} title="VAST Logo" />
            )}
            <p className="hero__subtitle">{siteConfig.tagline}</p>

            <div className={styles.buttons}>
              <Link
                className="button button--primary button--lg"
                to="/docs/about"
              >
                Get Started
              </Link>
            </div>
          </div>
        </div>

        <div className={styles.carousel}>
          <Carousel>
            {[
              <div className={styles.blogPost}>
                <div className="container">
                  <p>Sign up for our Newsletter</p>
                  <p className="hero__subtitle">
                    Stay in touch with what's new with VAST
                  </p>
                  <div>
                    <Link
                      className="button button--info button--md"
                      to="/newsletter"
                    >
                      Subscribe
                    </Link>
                  </div>
                </div>
              </div>,
            ].concat(
              blogpostsInternalArchive?.blogPosts
                ?.slice(0, 3)
                ?.map((post, idx) => (
                  <BlogPostSlide
                    key={idx}
                    title={post?.metadata?.title}
                    link={post?.metadata?.permalink}
                    isRelease={post?.metadata?.tags
                      ?.map((tag) => tag?.label)
                      ?.includes('release')}
                    imageLink={post?.metadata?.frontMatter?.image}
                    description={post?.metadata?.frontMatter?.description}
                  />
                ))
            )}
          </Carousel>
        </div>
      </header>
    </>
  );
}

type BlogPostType = {
  title: string;
  link: string;
  description?: string;
  imageLink?: string;
  isRelease?: boolean;
};

const BlogPostSlide = ({
  title,
  link,
  description,
  imageLink,
  isRelease = false,
}: BlogPostType) => {
  const {colorMode} = useColorMode();
  return (
    <div className={styles.blogPost}>
      <div className="container">
        <p>{isRelease ? 'Release' : 'Latest from our blog'}</p>{' '}
        <p className="hero__subtitle">{title}</p>
        {description && <p>{description}</p>}
        <div>
          <Link className="button button--info button--md" to={link}>
            {isRelease ? 'Read Announcement' : 'Read Post'}
          </Link>
        </div>
      </div>
      {imageLink && (
        <img
          src={imageLink}
          alt="Blogpost Figure"
          height="200px"
          className={colorMode === 'dark' ? styles.darkImage : ''}
        />
      )}
    </div>
  );
};

export default function Home(): JSX.Element {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={`Visibility Across Space and Time`}
      description="The network telemetry engine for data-driven security investigations."
    >
      <HomepageHeader />
      <main>
        <HomepageFeatures />
      </main>
    </Layout>
  );
}
