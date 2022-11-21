<script lang="ts">
  import 'bytemd/dist/index.css';
  import { Editor } from 'bytemd';
  import gfm from '@bytemd/plugin-gfm';

  let value: string;
  export let placeholder = 'Write here...';
  const plugins = [
    gfm()
    // Add more plugins here
  ];

  function handleChange(e: CustomEvent) {
    value = e.detail.value;
  }
</script>

<template>
  <Editor {value} {placeholder} {plugins} on:change={handleChange} />
</template>
