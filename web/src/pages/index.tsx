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

// There is no straightforward way to generate these links without
// overriding the blog plugin, so we hardcode them for now.
const latestNonReleaseBlogPost = {
  title: 'The New REST API',
  link: '/blog/the-new-rest-api',
};

const latestReleaseBlogPost = {
  title: 'VAST v2.4.1 Released',
  link: '/blog/vast-v2.4.1',
};

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
            <div className={styles.leftAligned}>
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
            </div>
            <BlogPostSlide
              title={latestNonReleaseBlogPost.title}
              link={latestNonReleaseBlogPost.link}
            />
            <BlogPostSlide
              title={latestReleaseBlogPost.title}
              link={latestReleaseBlogPost.link}
              isRelease={true}
            />
          </Carousel>
        </div>
      </header>
    </>
  );
}

const BlogPostSlide = ({title, link, isRelease = false}) => {
  return (
    <div className={styles.leftAligned}>
      <div className="container">
        <p>{isRelease ? 'Latest Release' : 'Latest from our blog'}</p>
        <p className="hero__subtitle">{title}</p>
        <div>
          <Link className="button button--info button--md" to={link}>
            {isRelease ? 'Read Announcement' : 'Read Post'}
          </Link>
        </div>
      </div>
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
