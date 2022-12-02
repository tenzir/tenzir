<script lang="ts">
  import 'bytemd/dist/index.css';
  import { Editor, type BytemdPlugin } from 'bytemd';

  export let value: string;
  export let placeholder = 'Write here...';

  export let plugins: Array<BytemdPlugin>;

  function handleChange(e: CustomEvent) {
    value = e.detail.value;
  }
</script>

<template>
  <Editor {value} {placeholder} {plugins} on:change={handleChange} />
</template>
