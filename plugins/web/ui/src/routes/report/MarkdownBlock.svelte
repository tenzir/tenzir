<script lang="ts">
  import Button from '$lib/components/Button.svelte';
  import Editor from '$lib/components/markdown/Editor.svelte';
  import Viewer from '$lib/components/markdown/Viewer.svelte';
  import gfm from '@bytemd/plugin-gfm';
  import highlight from '@bytemd/plugin-highlight';
  import mermaid from '@bytemd/plugin-mermaid';
  import BlockHeader from './BlockHeader.svelte';


  export let parameters = {
    title: '',
    content: '',
    isEditing: false
  };

  const handleSaveOrEdit = () => {
    parameters.isEditing= !parameters.isEditing;
  };

  const plugins = [highlight(), gfm(), mermaid()];
</script>

<div>
  {#if parameters.isEditing}
    <Editor bind:value={parameters.content} {plugins} />
    <div class="py-2">
      <Button onClick={handleSaveOrEdit}>Save</Button>
    </div>
  {/if}
  {#if !parameters.isEditing}
    <BlockHeader
      bind:title={parameters.title}
      onEdit={() => handleSaveOrEdit()}
      onDelete={() => {
        console.log('delete is not implemented yet');
      }}
    />
    <Viewer bind:value={parameters.content} {plugins} />
  {/if}
</div>
