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
    title: 'Telemetry Data Engine',
    Svg: require('@site/static/img/engine.svg').default,
    description: (
      <>
        Your SIEM keels over and costs explode? Deploy VAST close to the data
        source to transform, store, anonymize, query, and age your high-volume
        telemetry at the edge.
      </>
    ),
  },
  {
    title: 'Security Content Execution',
    Svg: require('@site/static/img/security-content.svg').default,
    description: (
      <>
        Easy-button detection: operationalize your security content by
        synchronizing it with your threat intelligence platform, unifying live
        and retro detection with one system.
      </>
    ),
  },
  {
    title: 'Threat Hunting & Data Science',
    Svg: require('@site/static/img/explore.svg').default,
    description: (
      <>
        Pivot to what matters and contextualize the gaps. Need richer analysis?
        Bring your own data science tooling: VAST offers high-bandwidth
        data access via <a href="https://arrow.apache.org">Apache Arrow</a> and
        Parquet.
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
