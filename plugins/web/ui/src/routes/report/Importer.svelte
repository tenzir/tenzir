<script lang="ts">
  export let fileString: string | undefined = undefined;

  import { report } from './stores';
  import FilePond from 'svelte-filepond';
  import Button from '$lib/components/Button.svelte';

  // a reference to the component, used to call FilePond methods
  let pond;

  // the name to use for the internal file input
  let name = 'filepond';

  const handleAddFile = async (err, fileItem) => {
    // console.log('A file has been added', fileItem);
    const text = await pond.getFiles()[0].file.text();
    fileString = text;
  };

  const submitToStore = () => {
    if (fileString) {
      report.set(JSON.parse(fileString));
    }
  };
</script>

<div class="w-1/3 m-auto py-10">
  <FilePond
    bind:this={pond}
    {name}
    allowMultiple={false}
    onaddfile={handleAddFile}
    credits={false}
  />
  {#if fileString}
    <Button onClick={submitToStore}>Import</Button>
  {/if}
</div>

<style global>
  @import 'filepond/dist/filepond.css';
</style>
