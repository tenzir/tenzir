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

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  const {colorMode} = useColorMode();
  return (
    <header className={clsx('hero shadow--lw', styles.heroBanner)}>
      <div className="container">
        {colorMode === "dark"
          ? <VASTLight className={styles.vastLogo} title="VAST Logo" />
          : <VASTDark className={styles.vastLogo} title="VAST Logo" />
        }
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div className={styles.buttons}>
          <Link
            className="button button--secondary button--lg"
            to="/docs/start">
            Get Started
          </Link>
        </div>
      </div>
    </header>
  );
}

export default function Home(): JSX.Element {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={`Hello from ${siteConfig.title}`}
      description="Description will go into a meta tag in <head />">
      <HomepageHeader />
      <main>
        <HomepageFeatures />
      </main>
    </Layout>
  );
}
