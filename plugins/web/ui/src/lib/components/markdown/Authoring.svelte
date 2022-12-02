<script lang="ts">
  import Button from '../Button.svelte';
  import Editor from './Editor.svelte';
  import Viewer from './Viewer.svelte';

  let showEditor = true;
  let showViewer = false;

  let value: string;
  const handleSaveOrEdit = () => {
    showEditor = !showEditor;
    showViewer = !showViewer;
  };

  import gfm from '@bytemd/plugin-gfm';
  import highlight from '@bytemd/plugin-highlight';
  import mermaid from '@bytemd/plugin-mermaid';

  const plugins = [highlight(), gfm(), mermaid()];
</script>

<div>
  {#if showEditor}
    <Editor bind:value {plugins} />
    <Button onClick={handleSaveOrEdit}>Save</Button>
  {/if}
  {#if showViewer}
    <Viewer bind:value {plugins} />

    <Button onClick={handleSaveOrEdit}>Edit</Button>
  {/if}
</div>
