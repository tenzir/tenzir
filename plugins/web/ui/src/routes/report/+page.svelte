<script lang="ts">
  import Button from '$lib/components/Button.svelte';

  import ReportEditor from './ReportEditor.svelte';
  import Importer from './Importer.svelte';
  import { report } from './stores';
  let showEditor = false;
</script>

<div>
  {#if $report.blocks.length !== 0 || showEditor}
    <ReportEditor />
  {:else}
    <div class="flex flex-col">
      <Importer />
      <div class="m-auto">
        <Button
          onClick={() => {
            showEditor = true;
          }}
        >
          Create New Report
        </Button>
      </div>
    </div>
  {/if}
</div>
