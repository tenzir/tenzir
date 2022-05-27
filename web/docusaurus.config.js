// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/vsDark');

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'VAST',
  tagline: 'Actionable insights at your fingertips.',
  url: 'https://vast.io',
  baseUrl: '/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'error',
  favicon: 'img/favicon.ico',
  organizationName: 'tenzir', // Usually your GitHub org/user name.
  projectName: 'vast', // Usually your repo name.
  trailingSlash: false, // GitHub Pages already adds a slash

  plugins: [
    'docusaurus-plugin-sass',
  ],

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl: 'https://github.com/tenzir/vast/tree/master/web',
          remarkPlugins: [require('mdx-mermaid')],
          // TODO: The last update author and time is always the person that
          // triggered the last deployment and the time of that deployment.
          // Ideally we'd show this information, but as-is it's unnecessary.
          showLastUpdateTime: false,
          showLastUpdateAuthor: false,
        },
        blog: {
          blogTitle: 'Blog',
          blogDescription: 'News from the VAST community',
          blogSidebarCount: 10,
          blogSidebarTitle: 'Blog Posts',
          postsPerPage: 20,
        },
        theme: {
          customCss: require.resolve('./src/css/custom.scss'),
        },
        sitemap: {
          changefreq: 'weekly',
          priority: 0.5,
          ignorePatterns: ['/tags/**'],
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      navbar: {
        title: 'VAST',
        logo: {
          alt: 'Visibility Across Space and Time',
          src: 'img/vast-logo.svg',
        },
        items: [
          {
            type: 'doc',
            docId: 'about-vast/README',
            position: 'left',
            label: 'Docs',
          },
          {
            to: 'blog',
            label: 'Blog',
            position: 'left'
          },
          {
            to: '/changelog',
            label: 'Changelog',
            position: 'left',
          },
          {
            href: 'http://slack.tenzir.com',
            'aria-label': 'Slack',
            className: 'header-slack-link',
            position: 'right',
          },
          {
            href: 'https://github.com/tenzir/vast',
            'aria-label': 'GitHub',
            className: 'header-github-link',
            position: 'right',
          },
        ],
      },
      announcementBar: {
        content:
        'This site is under construction. Help us by leaving <a target="_blank" rel="noopener noreferrer" href="https://github.com/tenzir/vast/discussions">feedback</a> and a <a target="_blank" rel="noopener noreferrer" href="https://github.com/tenzir/vast/stargazers">GitHub Star</a>.',
        backgroundColor: '#f1f2f2',
        isCloseable: false,
      },
      footer: {
        links: [
          {
            title: 'Docs',
            items: [
              {
                label: 'About VAST',
                to: '/docs/about-vast',
              },
              {
                label: 'Setup VAST',
                to: '/docs/setup-vast',
              },
              {
                label: 'Use VAST',
                to: '/docs/use-vast',
              },
              {
                label: 'Understand VAST',
                to: '/docs/understand-vast',
              },
              {
                label: 'Develop VAST',
                to: '/docs/develop-vast',
              },
            ],
          },
          {
            title: 'Community',
            items: [
              {
                label: 'Slack',
                href: 'http://slack.tenzir.com',
              },
              {
                label: 'GitHub',
                href: 'https://github.com/tenzir/vast',
              },
              {
                label: 'Twitter',
                href: 'https://twitter.com/tenzir_company',
              },
              {
                label: 'LinkedIn',
                href: 'https://www.linkedin.com/company/tenzir',
              },
            ],
          },
          {
            title: 'Tenzir',
            items: [
              {
                label: 'Blog',
                href: 'https://tenzir.com/blog',
              },
              {
                label: 'Request Demo',
                href: 'https://tenzir.com/request-demo',
              },
              {
                label: 'Contact Us',
                href: 'https://tenzir.com/contact-us',
              },
              {
                label: 'Website',
                href: 'https://tenzir.com/blog',
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} Tenzir GmbH.`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
    }),
};

module.exports = config;
