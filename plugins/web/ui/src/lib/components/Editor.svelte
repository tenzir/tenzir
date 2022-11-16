<script>
  import 'bytemd/dist/index.css';
  import { Editor, Viewer } from 'bytemd';
  import gfm from '@bytemd/plugin-gfm';

  let value;
  export let placeholder = 'Write here...';
  const plugins = [
    gfm()
    // Add more plugins here
  ];

  function handleChange(e) {
    value = e.detail.value;
  }
</script>

<template>
  <Editor {value} {placeholder} {plugins} on:change={handleChange} />
</template>
