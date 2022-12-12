<script lang="ts">
  import Menu from '$lib/components/Menu.svelte';
  import { saveAs } from 'file-saver';
  import MarkdownBlock from './MarkdownBlock.svelte';
  import QueryBlock from './QueryBlock.svelte';
  import { report } from './stores';

  const handlePdfExport = () => {
    window.print();
  };

  const handleJsonExport = () => {
    // Create a blob of the data
    let fileName = 'vast-report.json'; // TODO use the title of the report
    var fileToSave = new Blob([JSON.stringify($report)], {
      type: 'application/json'
    });
    saveAs(fileToSave, fileName);
  };

  const addBlock = (blockType: 'query' | 'markdown') => {
    if (blockType === 'query') {
      report.update((oldReport) => {
        return {
          ...oldReport,
          blocks: [
            ...oldReport.blocks,
            {
              category: 'query',
              params: {
                query: '',
                title: ''
              }
            }
          ]
        };
      });
    } else {
      report.update((oldReport) => {
        return {
          ...oldReport,
          blocks: [
            ...oldReport.blocks,
            {
              category: 'markdown',
              params: {
                title: '',
                content: ''
              }
            }
          ]
        };
      });
    }
  };
  $: console.log($report);
</script>

<svelte:head>
  <title>VAST Reports</title>
  <meta name="description" content="Reporting page for VAST" />
</svelte:head>

<div class="reporting-page p-2 m-2 text-sm text-left text-gray-600">
  <div class="flex justify-between my-4 mr-4">
    <div class="text-xl font-bold">Reports</div>
    <Menu
      description="Export"
      items={[
        { text: 'As JSON', onClick: handleJsonExport },
        { text: 'As PDF', onClick: handlePdfExport }
      ]}
    />
  </div>
  <div>
    {#each $report.blocks as block}
      {#if block.category == 'query'}
        <div class="m-10">
          <QueryBlock parameters={block.params} />
        </div>
      {:else}
        <div class="my-10">
          <MarkdownBlock parameters={block.params} />
        </div>
      {/if}
    {/each}
  </div>
  <Menu
    description="Add Block"
    items={[
      { text: 'Query', onClick: () => addBlock('query') },
      { text: 'Markdown', onClick: () => addBlock('markdown') }
    ]}
  />
</div>

<style>
  /* Don't display buttons and other interactive elements when exported */
  @media print {
    /* https://github.com/sveltejs/svelte/issues/5804#issuecomment-746921375 */
    .reporting-page :global(button) {
      display: none;
    }
    :global(#nav-sidebar, .bytemd-toolbar, .bytemd-editor, .bytemd-status) {
      display: none;
    }

    :global(.bytemd-split, .bytemd, .bytemd-preview) {
      border: none !important;
    }
  }
</style>
