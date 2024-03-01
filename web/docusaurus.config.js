// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require("prism-react-renderer/themes/github");
const darkCodeTheme = require("prism-react-renderer/themes/vsDark");

async function createConfig() {
  return {
    title: "Tenzir",
    tagline: "Easy data pipelines for security teams.",
    url: "https://docs.tenzir.com",
    baseUrl: "/",
    onBrokenLinks: "throw",
    onBrokenMarkdownLinks: "throw",
    favicon: "img/tenzir-logo-black.svg",
    organizationName: "tenzir", // Usually your GitHub org/user name.
    projectName: "tenzir", // Usually your repo name.
    trailingSlash: false, // GitHub Pages already adds a slash

    markdown: {
      mermaid: true,
    },
    themes: ["@docusaurus/theme-mermaid"],

    plugins: [
      "docusaurus-plugin-sass",
      [
        "@docusaurus/plugin-client-redirects",
        {
          redirects: [
            {
              to: "/why-tenzir",
              from: "/docs/about-vast",
            },
            {
              to: "/get-started",
              from: "/docs/try-vast",
            },
            {
              to: "/setup-guides",
              from: "/docs/setup-vast",
            },
            {
              to: "/user-guides",
              from: "/docs/use-vast",
            },
            {
              to: "/developer-guides",
              from: "/docs/develop-vast",
            },
            {
              to: "/contribute",
              from: "/docs/develop-vast/contributing",
            },
            {
              to: "/",
              from: "/docs",
            },
          ],
        },
      ],
    ],

    presets: [
      [
        "classic",
        /** @type {import('@docusaurus/preset-classic').Options} */
        ({
          docs: {
            path: "docs", // The directory in the filesystem.
            routeBasePath: "/", // Serve the docs at the site's root.
            sidebarPath: require.resolve("./sidebars.js"),
            editUrl: "https://github.com/tenzir/tenzir/tree/main/web",
            // TODO: The last update author and time is always the person that
            // triggered the last deployment and the time of that deployment.
            // Ideally we'd show this information, but as-is it's unnecessary.
            showLastUpdateTime: false,
            showLastUpdateAuthor: false,
          },
          blog: {
            blogTitle: "Blog",
            blogDescription: "News from the Tenzir community",
            blogSidebarCount: 20,
            blogSidebarTitle: "Blog Posts",
            postsPerPage: 20,
          },
          pages: {},
          theme: {
            customCss: require.resolve("./src/css/custom.scss"),
          },
          sitemap: {
            changefreq: "weekly",
            priority: 0.5,
            ignorePatterns: ["/tags/**"],
          },
        }),
      ],
      [
        "redocusaurus",
        {
          specs: [
            {
              spec: "openapi/openapi.yaml",
              route: "/api/",
            },
          ],
          theme: {
            primaryColor: "#00a4f1",
          },
        },
      ],
    ],

    themeConfig:
      /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
      ({
        algolia: {
          appId: "BVB58VRBDH",
          apiKey: "f7c2eb86ff85cd55d9634543ed1c60b2",
          indexName: "vast",
        },
        colorMode: {
          disableSwitch: false,
          respectPrefersColorScheme: true,
        },
        navbar: {
          logo: {
            alt: "Tenzir",
            href: "https://tenzir.com",
            src: "img/tenzir-black.svg",
            srcDark: "img/tenzir-white.svg",
            width: 120,
          },
          items: [
            {
              type: "doc",
              docId: "get-started",
              position: "left",
              label: "Docs",
            },
            {
              to: "blog",
              label: "Blog",
              position: "left",
            },
            {
              to: "/changelog",
              label: "Changelog",
              position: "left",
            },
            {
              to: "/roadmap",
              label: "Roadmap",
              position: "left",
            },
            {
              to: "/discord",
              "aria-label": "Discord",
              className: "header-discord-link",
              position: "right",
            },
            {
              href: "https://github.com/tenzir/tenzir",
              "aria-label": "GitHub",
              className: "header-github-link",
              position: "right",
            },
            {
              type: "docsVersionDropdown",
              position: "right",
              dropdownActiveClassDisabled: true,
            },
            {
              type: "search",
              position: "right",
            },
          ],
        },
        //announcementBar: {
        //  content:
        //  'Tenzir is now <em>live</em>. Read our <a href="/blog/introducing-tenzir-security-data-pipelines">announcement blog</a> and be among the first to experience it at <a href="https://app.tenzir.com">app.tenzir.com</a>.',
        //  backgroundColor: '#f1f2f2',
        //  isCloseable: true,
        //},
        footer: {
          links: [
            {
              title: "Resources",
              items: [
                {
                  label: "Docs",
                  to: "/",
                },
                {
                  label: "API",
                  to: "/api",
                },
                {
                  label: "Blog",
                  to: "/blog",
                },
                {
                  label: "Changelog",
                  to: "/changelog",
                },
                {
                  label: "Roadmap",
                  to: "/roadmap",
                },
              ],
            },
            {
              title: "Community",
              items: [
                {
                  label: "Discord",
                  href: "/discord",
                },
                {
                  label: "GitHub",
                  href: "https://github.com/tenzir/tenzir",
                },
                {
                  label: "Twitter",
                  href: "https://twitter.com/tenzir_company",
                },
                {
                  label: "LinkedIn",
                  href: "https://www.linkedin.com/company/tenzir",
                },
              ],
            },
            {
              title: "Tenzir",
              items: [
                {
                  label: "Website",
                  href: "https://tenzir.com",
                },
                {
                  label: "Privacy Statement",
                  to: "/privacy-statement",
                },
              ],
            },
          ],
          copyright: `Copyright © ${new Date().getFullYear()} Tenzir GmbH.`,
        },
        prism: {
          theme: lightCodeTheme,
          darkTheme: darkCodeTheme,
          additionalLanguages: ["r"],
        },
      }),

    // This replaces Docusaurus' default babel loader with esbuild
    // See more at https://github.com/facebook/docusaurus/issues/4765#issuecomment-841135926
    webpack: {
      jsLoader: (isServer) => ({
        loader: require.resolve("esbuild-loader"),
        options: {
          loader: "tsx",
          format: isServer ? "cjs" : undefined,
          target: isServer ? "node12" : "es2017",
        },
      }),
    },

    scripts: [
      {
        src: "https://plausible.io/js/script.js",
        aysnc: true,
        defer: true,
        "data-domain": "docs.tenzir.com",
      },
    ],
  };
}

module.exports = createConfig;
