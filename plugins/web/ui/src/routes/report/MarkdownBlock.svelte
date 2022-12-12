<script lang="ts">
  import Button from '$lib/components/Button.svelte';
  import Editor from '$lib/components/markdown/Editor.svelte';
  import Viewer from '$lib/components/markdown/Viewer.svelte';
  import gfm from '@bytemd/plugin-gfm';
  import highlight from '@bytemd/plugin-highlight';
  import mermaid from '@bytemd/plugin-mermaid';

  let showEditor = true;

  export let parameters = {
    title: 'TODO make me editable',
    content: ''
  };

  const handleSaveOrEdit = () => {
    showEditor = !showEditor;
  };

  const plugins = [highlight(), gfm(), mermaid()];
</script>

<div>
  <span>{parameters.title}</span>

  {#if showEditor}
    <Editor bind:value={parameters.content} {plugins} />
    <Button onClick={handleSaveOrEdit}>Save</Button>
  {/if}
  {#if !showEditor}
    <Viewer bind:value={parameters.content} {plugins} />
    <Button onClick={handleSaveOrEdit}>Edit</Button>
  {/if}
</div>
