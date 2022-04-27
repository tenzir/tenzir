// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'VAST',
  tagline: 'Actionable insights at your fingertips.',
  url: 'https://tenzir.github.io',
  baseUrl: '/vast/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'error',
  favicon: 'img/favicon.ico',
  organizationName: 'tenzir', // Usually your GitHub org/user name.
  projectName: 'vast', // Usually your repo name.

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl: 'https://github.com/tenzir/vast/tree/master/docs',
          remarkPlugins: [require('mdx-mermaid')],
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
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
            docId: 'get-started/README',
            position: 'left',
            label: 'Docs',
          },
          {
            href: 'https://github.com/tenzir/vast',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        links: [
          {
            title: 'Docs',
            items: [
              {
                label: 'Get Started',
                to: '/docs/get-started',
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
