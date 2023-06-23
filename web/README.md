# Tenzir Website

This directory contains code and content of the official Tenzir documentation
site at <https://docs.tenzir.com> We use [Docusaurus](https://docusaurus.io/) as
driving framework and an [enhanced flavor of
Markdown](https://docusaurus.io/docs/markdown-features) for primary content.

## Structure

Key files and directories of the `/web` directory include:

- [blog](/blog): articles and accompanying files.
- [docs](/docs): documentation content
- [presets](/presets): partial MDX components for inclusion elsewhere
- [src](/src): React components in Typescript, CSS, and pages
- [static](/static): images and other content directly copied into the root

## Adding and Editing Content

Please consult the section on [writing
documentation](https://docs.tenzir.com/next/contribute/documentation) for
instructions on how to change existing or add new content.

## Build and View Locally

Use [yarn](https://yarnpkg.com/) to build and view the site locally:

```bash
# Setup environment.
yarn
# Guaranteed to work, but a bit more to type than just 'yarn':
yarn install --frozen-lockfile
```

After installing dependencies, build the site via Docusaurus and serve at
http://localhost:3000:

```bash
yarn start
```

The `start` command starts a local development server and opens up a browser
window. Most changes are reflected live without having to restart the server.

## Deploy the Site

To package up a local for deployment, use the `build` command:

```
yarn build
```

The resulting `build` directory can be served using any static hosting service.

For the main site, we use a GitHub Actions workflow to deploy with every push to
main.
