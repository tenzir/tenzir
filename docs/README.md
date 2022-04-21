# VAST Docs

This directory the [official VAST documentation][site], along with the
[Docusaurus](https://docusaurus.io/) scaffold for local building, viewing, and
deploying.

### Build

Use [yarn](https://yarnpkg.com/) to build and view the site locally:

```
yarn
yarn start
```

The `start` command starts a local development server and opens up a browser
window. Most changes are reflected live without having to restart the server.

### Deployment

To package up a local for deployment, use the `build` command:

```
yarn build
```

The resulting `build` directory can be served using any static hosting service.

We use a [github actions workflow][workflow] to deploy this site with every push
to master.

[site]: https://docs.tenzir.com/vast
[workflow]: https://github.com/tenzir/vast/blob/master/.github/workflows/docs.yaml
