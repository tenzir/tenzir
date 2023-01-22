---
sidebar_position: 5
---

# Documentation

The VAST documentation resides inside [our main GitHub
repository](https://github.com/tenzir/vast) in
[`/web/docs`](https://github.com/tenzir/vast/tree/master/web/docs).
We use [Docusaurus](https://docusaurus.io/) as website framework.

## Build and view locally

To view the entire site (including the documentation) locally,
change to the [`/web`](https://github.com/tenzir/vast/tree/master/web/)
directory and invoke [`yarn`](https://yarnpkg.com/), or to be on the safe side,
`yarn install --frozen-lockfile` to avoid pollution from global dependencies.
Then build and serve the site via:

```bash
yarn start
```

Browse to <http://localhost:3000/> to view the site. Docusaurus should spawn
your default browser automatically upon invoking `yarn start`.

## Write content

Docusaurus uses an [enhanced flavor of
Markdown](https://docusaurus.io/docs/markdown-features) that allows for
embedding richer content elements, such as:

- [Callouts/admonitions](https://docusaurus.io/docs/markdown-features/admonitions)
  for block notes, infos, and warnings
- [React JSX components](https://docusaurus.io/docs/markdown-features/react) via
  [MDX](https://mdxjs.com/), specifically via the built-in UI component library
  [Infima](https://infima.dev/)
- [Math equations](https://docusaurus.io/docs/markdown-features/math-equations)
  via [KaTeX](https://katex.org/)

We encourage making judicious use of these extras for an optimal reading
experience.

## Edit diagrams

We use [Excalidraw](https://excalidraw.com) as primary tool to create sketches
of architectural diagrams. It is open source and has a neat collaboration
feature: the ability to *embed the source code* of the sketch into the exported
PNG or SVG images.

This means the editing workflow looks as follows:

1. Open <https://excalidraw.com> and click *Upload* in the top left
2. Select the SVG you would like to edit
3. Make your edits in Excalidraw
4. Uncheck the box "Background" to ensure a transparent background.
5. Re-export the drawing in size **2x** and **check the box "Embed scene"**

The last part is crucial: If you don't check "Embed scene" it will no longer be
able to recover the original diagram source.

## Cater to dark mode

Our setup makes it easy to render different images whether light or dark mode is
toggled. We use the same CSS that GitHub supports, i.e., `#gh-dark-mode-only`
and `#gh-light-mode-only`.

Here's an example to include one image that exists in two variants:

```markdown
![Image Description](/path/to/dark.png#gh-dark-mode-only)
![Image Description](/path/to/light.png#gh-light-mode-only)
```

SVGs do not need this double-inclusion. We automatically perform the following
Excalidraw-style CSS filter for all inlined SVGs:

```css
[data-theme='dark'] .markdown-inline-svg {
  filter: invert(100%) hue-rotate(180deg);
}
```

This means that it suffices to include SVGs as follows:

```markdown
![Image Description](/path/to/light-version.svg)
```

## Scale images

We're making use of the alt text in Markdown images in combination with some
CSS3 features to specify a maximum width:

```markdown
![alt #width500](/path/to/img)
```

The suffix `#width500` gets picked up by the following CSS:

```css
img[alt$="#width500"]{
  max-width: 500px;
  display: block;
}
```

We currently support the following classes:

- `#width300`
- `#width400`
- `#width500`
- `#width600`

This should hopefully cover the majority of use cases.
