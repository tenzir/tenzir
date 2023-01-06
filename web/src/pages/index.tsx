import React, {useEffect, useState} from 'react';
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

import strip from 'strip-markdown';
import {remark} from 'remark';

// NOTE: this is internal and may change, and the types might not be guaranteed
const blogpostsInternalArchive = require('../../.docusaurus/docusaurus-plugin-content-blog/default/blog-archive-80c.json');

const extractTextBeforeTruncate = (str: string) => {
  const truncateIndex = str.indexOf('<!--truncate-->');
  return truncateIndex === -1 ? str : str.substring(0, truncateIndex);
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
            {[
              <CarouselCard
                title="Sign up for our Newsletter"
                link={'/newsletter'}
                label="Get the latest news and updates from the VAST team"
                buttonLabel="Subscribe"
              />,
            ].concat(
              blogpostsInternalArchive?.blogPosts
                ?.slice(0, 4)
                ?.map((post, idx) => {
                  return (
                    <CarouselCard
                      key={idx}
                      title={post?.metadata?.title}
                      link={post?.metadata?.permalink}
                      label={
                        post?.metadata?.tags
                          ?.map((tag) => tag?.label)
                          ?.includes('release')
                          ? 'Latest Release'
                          : 'Latest Blogpost'
                      }
                      buttonLabel={
                        post?.metadata?.tags
                          ?.map((tag) => tag?.label)
                          ?.includes('release')
                          ? 'Read Announcement'
                          : 'Read Post'
                      }
                      imageLink={post?.metadata?.frontMatter?.image}
                      description={
                        post?.metadata?.hasTruncateMarker
                          ? extractTextBeforeTruncate(post?.content)
                          : null
                      }
                    />
                  );
                })
            )}
          </Carousel>
        </div>
      </header>
    </>
  );
}

type CarouselCard = {
  title: string;
  link: string;
  label: string;
  buttonLabel: string;
  description?: string;
  imageLink?: string;
};

const CarouselCard = ({
  title,
  link,
  description,
  imageLink,
  label,
  buttonLabel,
}: CarouselCard) => {
  const maximumCarouselCardExcerptLength = 300;
  const [descriptionToShow, setDescriptionToShow] = useState('');
  useEffect(() => {
    remark()
      .use(strip)
      .process(description)
      .then((file) => {
        const truncated = truncateDesc(
          file.toString(),
          maximumCarouselCardExcerptLength
        );
        setDescriptionToShow(truncated);
      });
  }, [description]);

  return (
    <div className={styles.carouselCard}>
      <div className={clsx(styles.container, 'container')}>
        <div className={styles.top}>
          <div>
            {label}
            <p className="hero__subtitle">{title}</p>
            {description && description !== '' && (
              <div className={styles.blogDesc}>{descriptionToShow}</div>
            )}
          </div>
          <div>
            <Link className="button button--info button--md" to={link}>
              {buttonLabel}
            </Link>
          </div>
        </div>
      {imageLink && (
        <img
          src={imageLink}
          alt="Blogpost Figure"
          width="500px"
          className="svglite"
        />
      )}
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
