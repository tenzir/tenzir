<script lang="ts">
  import 'bytemd/dist/index.css';
  import { Viewer, type BytemdPlugin } from 'bytemd';

  export let value: string;
  export let plugins: Array<BytemdPlugin>;
</script>

<template>
  <Viewer {value} {plugins} />
</template>
