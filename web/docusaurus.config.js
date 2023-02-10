// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/vsDark');

async function createConfig() {
  return {
  title: 'VAST',
  tagline: 'Actionable insights at your fingertips.',
  url: 'https://vast.io',
  baseUrl: '/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'throw',
  favicon: 'img/favicon.ico',
  organizationName: 'tenzir', // Usually your GitHub org/user name.
  projectName: 'vast', // Usually your repo name.
  trailingSlash: false, // GitHub Pages already adds a slash

  markdown: {
    mermaid: true,
  },
  themes: ['@docusaurus/theme-mermaid'],

  plugins: [
    'docusaurus-plugin-sass',
    [
      '@docusaurus/plugin-client-redirects',
      {
        redirects: [
          {
            to: '/docs/about',
            from: '/docs/about-vast',
          },
          {
            to: '/docs/try',
            from: '/docs/try-vast',
          },
          {
            to: '/docs/setup',
            from: '/docs/setup-vast',
          },
          {
            to: '/docs/use',
            from: '/docs/use-vast',
          },
          {
            to: '/docs/understand',
            from: '/docs/understand-vast',
          },
          {
            to: '/docs/develop',
            from: '/docs/develop-vast',
          },
          {
            to: '/docs/contribute',
            from: '/docs/develop-vast/contributing',
          },
        ],
      },
    ],
  ],

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl: 'https://github.com/tenzir/vast/tree/master/web',
          // TODO: The last update author and time is always the person that
          // triggered the last deployment and the time of that deployment.
          // Ideally we'd show this information, but as-is it's unnecessary.
          showLastUpdateTime: false,
          showLastUpdateAuthor: false,
        },
        blog: {
          blogTitle: 'Blog',
          blogDescription: 'News from the VAST community',
          blogSidebarCount: 20,
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
    [
      'redocusaurus',
      {
        specs: [
          {
            spec: 'openapi/openapi.yaml',
            route: '/api/',
          },
        ],
        theme: {
          primaryColor: '#00a4f1',
        },
      },
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      algolia: {
        appId: 'BVB58VRBDH',
        apiKey: 'f7c2eb86ff85cd55d9634543ed1c60b2',
        indexName: 'vast',
      },
      navbar: {
        title: 'VAST',
        logo: {
          alt: 'Visibility Across Space and Time',
          src: 'img/vast-logo.svg',
        },
        items: [
          {
            type: 'doc',
            docId: 'about/README',
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
            to: '/roadmap',
            label: 'Roadmap',
            position: 'left',
          },
          // TODO: Uncomment once the API is available.
          //{
          //  to: '/api',
          //  label: 'API',
          //  position: 'left',
          //},
          {
            type: 'search',
            position: 'right',
          },
          {
            href: 'https://vast.io/discord',
            'aria-label': 'Discord',
            className: 'header-discord-link',
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
        'Leave us <a target="_blank" rel="noopener noreferrer" href="https://github.com/tenzir/vast/stargazers">a GitHub star</a> ⭐️',
        backgroundColor: '#f1f2f2',
        isCloseable: true,
      },
      footer: {
        links: [
          {
            title: 'Docs',
            items: [
              {
                label: 'About',
                to: '/docs/about',
              },
              {
                label: 'Setup',
                to: '/docs/setup',
              },
              {
                label: 'Use',
                to: '/docs/use',
              },
              {
                label: 'Understand',
                to: '/docs/understand',
              },
              {
                label: 'Contribute',
                to: '/docs/contribute',
              },
              {
                label: 'Develop',
                to: '/docs/develop',
              },
            ],
          },
          {
            title: 'Community',
            items: [
              {
                label: 'Discord',
                href: '/discord',
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
              {
                label: 'Newsletter',
                href: '/newsletter',
              },
            ],
          },
          {
            title: 'VAST',
            items: [
              {
                label: 'Blog',
                to: '/blog',
              },
              {
                label: 'Changelog',
                to: '/changelog',
              },
              {
                label: 'Roadmap',
                to: '/roadmap',
              },
              {
                label: 'API',
                to: '/api',
              },
              {
                label: 'Privacy Statement',
                to: '/privacy-statement',
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
        additionalLanguages: ['r'],
      },
    }),

  scripts: [{src: 'https://plausible.io/js/script.js', aysnc: true, defer: true, 'data-domain': 'vast.io'}],
};
}

module.exports = createConfig;
