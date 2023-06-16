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
import {ReactSVG} from 'react-svg';

// NOTE: this is internal and may change, and the types might not be guaranteed
const blogpostsInternalArchive = require('../../.docusaurus/docusaurus-plugin-content-blog/default/blog-archive-80c.json');

const extractTextBeforeTruncate = (str: string) => {
  const truncateIndex = str.indexOf('<!--truncate-->');
  return truncateIndex === -1 ? str : str.substring(0, truncateIndex);
};

const latestReleaseBlogPost = blogpostsInternalArchive?.blogPosts?.find(
  (blogpost) =>
    blogpost?.metadata?.tags?.map((tag) => tag.label)?.includes('release')
);
const latestNonReleaseBlogPosts = blogpostsInternalArchive?.blogPosts
  ?.filter(
    (blogpost) =>
      !blogpost?.metadata?.tags?.map((tag) => tag.label)?.includes('release')
  )
  ?.slice(0, 3);

const truncateDesc = (str: string, n: number) => {
  return str.length > n ? str.substring(0, n) + '...' : str;
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
              latestReleaseBlogPost?.metadata?.title && (
                <CarouselCard
                  key="latest-release-carousel"
                  title={latestReleaseBlogPost?.metadata?.title}
                  link={latestReleaseBlogPost?.metadata?.permalink}
                  label="Latest Release"
                  buttonLabel="Read Announcement"
                  imageLink={latestReleaseBlogPost?.metadata?.frontMatter?.image}
                  description={
                    latestReleaseBlogPost?.metadata?.hasTruncateMarker
                      ? extractTextBeforeTruncate(latestReleaseBlogPost?.content)
                      : null
                  }
                />
              ),
            ].concat(
              latestNonReleaseBlogPosts?.map((post, idx) => {
                return (
                  <CarouselCard
                    key="blogpost-${idx}-carousel"
                    title={post?.metadata?.title}
                    link={post?.metadata?.permalink}
                    label="Blogpost"
                    buttonLabel="Read Post"
                    imageLink={post?.metadata?.frontMatter?.image}
                    description={
                      post?.metadata?.hasTruncateMarker
                        ? extractTextBeforeTruncate(post?.content)
                        : null
                    }
                  />
                );
              })
            ).concat(
              [
                <CarouselCard
                  key="newsletter-carousel"
                  title="Sign up for our Newsletter"
                  link={'/newsletter'}
                  label="Get the latest news and updates from the VAST team"
                  imageLink={"/img/newsletter.excalidraw.svg"}
                  buttonLabel="Subscribe"
                />
              ]
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
        {/* HACK: This is to safeguard against one incompatible (wrong?) image url in
        https://github.com/tenzir/vast/tree/6c17f01e630c71a55df7002d2276697dfcfaa463/web/blog/parquet-and-feather-writing-security-telemetry/index.qmd */}
        {imageLink && imageLink.startsWith('/') && (
          <ReactSVG
            src={imageLink}
            className={clsx("markdown-inline-svg", styles.svgWrapper)}
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
