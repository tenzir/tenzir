// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/vsDark');
const path = require('node:path');

async function createConfig() {
  /// BEGIN CUSTOM CODE ///

  // This is customized version of
  // https://github.com/alvinometric/remark-inline-svg/blob/e0c3b0ddb3f34a8766b7e39e05a1888a55347f7f/index.js
  // in order to co-operate with Docusaurus.

  // Problem: we store some images in /static/img folder and we want to reference
  // them in markdown as ![image something](/img/something.svg).
  // If we don't want to inline those images this works in docusaurus as docusaurus
  // preprocesses all /img/something.svg correctly). But remark-inline-svg plugin does
  // not support this - it tries to resolve relative to the markdown file.
  // We also need to support relative links like ![image something](something.svg).

  // Solution: we change the plugin's function to rewrite all /img/something.svg
  // to the correct full path which is <ROOT>/static/img/something.svg
  // NOTE: `static` is a default folder for static assets in docusaurus.
  // We provide the option `staticDir` to override this.
  // And in case of relative links we just prepend the path to the markdown file.

  // This is not perfect but it works.

  const inlineSVGTransform = (await import('remark-inline-svg')).transform;
  const visit = (await import('unist-util-visit')).default;

  const inlineSVG = (options = {}) => {
    const suffix = options.suffix || '.inline.svg';
    const staticDir = options.staticDir || 'static';

    return async (tree, file) => {
      const svgs = [];

      visit(tree, 'image', (node) => {
        const {url} = node;

        if (url.endsWith(suffix)) {
          // if url is /img/something.svg, then we need to use our custom logic
          if (url.startsWith('/img/')) {
            const fullPath = path.join(process.cwd(), staticDir, url);
            node.url = fullPath;
          }
          // otherwise we just use the original logic
          else {
            const markdownFileDir = path.dirname(file.history[0]);
            node.url = path.resolve('./', markdownFileDir, url);
          }

          svgs.push(node);
        }
      });

      if (svgs.length > 0) {
        const promises = svgs.map(async (node) => {
          return await inlineSVGTransform(node, options);
        });

        await Promise.all(promises);
      }

      return tree;
    };
  };

  /// END CUSTOM CODE ///
  return {
    title: 'VAST',
    tagline: 'The open-source pipeline and storage engine for security.',
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
            beforeDefaultRemarkPlugins: [[inlineSVG, {suffix: '.svg'}]],
          },
          blog: {
            blogTitle: 'Blog',
            blogDescription: 'News from the VAST community',
            blogSidebarCount: 20,
            blogSidebarTitle: 'Blog Posts',
            postsPerPage: 20,
            beforeDefaultRemarkPlugins: [[inlineSVG, {suffix: '.svg'}]],
          },
          pages: {
            beforeDefaultRemarkPlugins: [[inlineSVG, {suffix: '.svg'}]],
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
              position: 'left',
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
            {
              type: 'docsVersionDropdown',
              position: 'right',
              dropdownActiveClassDisabled: true,
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
                  label: 'VAST and Tenzir',
                  to: '/vast-and-tenzir',
                },
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
                  href: 'https://tenzir.com',
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

    // This replaces Docusaurus' default babel loader with esbuild
    // See more at https://github.com/facebook/docusaurus/issues/4765#issuecomment-841135926
    webpack: {
      jsLoader: (isServer) => ({
        loader: require.resolve('esbuild-loader'),
        options: {
          loader: 'tsx',
          format: isServer ? 'cjs' : undefined,
          target: isServer ? 'node12' : 'es2017',
        },
      }),
    },

    scripts: [
      {
        src: 'https://plausible.io/js/script.js',
        aysnc: true,
        defer: true,
        'data-domain': 'vast.io',
      },
    ],
  };
}

module.exports = createConfig;
