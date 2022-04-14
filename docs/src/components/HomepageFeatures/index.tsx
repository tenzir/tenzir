import React from 'react';
import clsx from 'clsx';
import styles from './styles.module.css';

type FeatureItem = {
  title: string;
  Svg: React.ComponentType<React.ComponentProps<'svg'>>;
  description: JSX.Element;
};

const FeatureList: FeatureItem[] = [
  {
    title: 'High-Speed Telemetry Engine',
    Svg: require('@site/static/img/engine.svg').default,
    description: (
      <>
        VAST is fast: when your SIEM keels over or makes your costs explode,
        VAST ingests, stores, compacts, and rotates your high-volume telemetry
        at ease.
      </>
    ),
  },
  {
    title: 'Security Content Execution',
    Svg: require('@site/static/img/security-content.svg').default,
    description: (
      <>
        Easy-button detection: operationalize tactical security content
        automatically by synchronizing it with your threat intelligence
        platform, both live and retrospectively.
      </>
    ),
  },
  {
    title: 'Threat Hunting & Data Science',
    Svg: require('@site/static/img/explore.svg').default,
    description: (
      <>
        Pivot through contextualized events to what matters. Need richer
        analysis? Bring your own data science toolkit: VAST offers
        high-bandwidth data access via <a href="https://arrow.apache.org">Apache
        Arrow</a> and Parquet.
      </>
    ),
  },
];

function Feature({title, Svg, description}: FeatureItem) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center">
        <Svg className={styles.featureSvg} role="img" />
      </div>
      <div className="text--center padding-horiz--md">
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures(): JSX.Element {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
