# GitHub

This page documents workflows concerning developer-facing GitHub infrastructure.

## Synchronize issue labels

To ensure that [issue and pull request
labels](https://github.com/tenzir/tenzir/labels) are consistent within and
across several of our repositories, we use [GitHub Label
Sync](https://github.com/Financial-Times/github-label-sync).

To synchronize the labels, run:

```bash
github-label-sync --access-token TOKEN --labels labels.yml REPO
```

`TOKEN` is a personal GitHub Access Token and `REPO` the GitHub repository,
e.g., `tenzir/tenzir`. The labels configuration
[`labels.yml`](https://github.com/tenzir/tenzir/blob/main/.github/labels.yml) is
part of this repository and has the following contents:

import CodeBlock from '@theme/CodeBlock';
import Configuration from '!!raw-loader!@site/../.github/labels.yml';

<CodeBlock language="yaml">{Configuration}</CodeBlock>
