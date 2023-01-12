<script lang="ts">
  import FilePondPluginFileValidateType from 'filepond-plugin-file-validate-type';

  import { report } from './stores';
  import type { FilePond as FilePondType } from 'filepond';
  import FilePond, { registerPlugin } from 'svelte-filepond';
  import Button from '$lib/components/Button.svelte';

  // a reference to the component, used to call FilePond methods
  let pond: FilePondType;
  // the name to use for the internal file input
  let name = 'filepond';

  registerPlugin(FilePondPluginFileValidateType);

  export let fileString: string | undefined = undefined;

  const handleAddFile = async () => {
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
    labelIdle="Drag'n'Drop or <u>Browse</u> to import a Report"
    acceptedFileTypes={['application/json']}
  />
  {#if fileString}
    <Button onClick={submitToStore}>Import</Button>
  {/if}
</div>

<style global>
  @import 'filepond/dist/filepond.css';
</style>
