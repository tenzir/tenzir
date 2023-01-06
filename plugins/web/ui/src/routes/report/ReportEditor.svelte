<script lang="ts">
  import InlineInput from '$lib/components/InlineInput.svelte';
  import Menu from '$lib/components/Menu.svelte';
  import pkg from 'file-saver';
  const { saveAs } = pkg;
  import MarkdownBlock from './MarkdownBlock.svelte';
  import QueryBlock from './QueryBlock.svelte';
  import { report } from './stores';

  const handlePdfExport = () => {
    window.print();
  };

  const handleJsonExport = () => {
    // Create a blob of the data
    let fileName = `${$report.title}.json`;
    var fileToSave = new Blob([JSON.stringify($report, undefined, 2)], {
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
                title: '',
                isEditing: true,
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
                content: '',
                isEditing: true,
              }
            }
          ]
        };
      });
    }
  };
</script>

<svelte:head>
  <title>VAST Reports</title>
  <meta name="description" content="Reporting page for VAST" />
</svelte:head>

<div class="reporting-page p-2 m-2 text-sm text-left text-gray-600">
  <div class="justify-center">
    <div class="w-5/6 m-auto grid grid-cols-1 divide-y divide-slate-300">
      <div class="flex justify-between my-4">
        <InlineInput
          bind:value={$report.title}
          placeholder="Untitled Report"
          labelClasses="text-xl font-bold"
          inputClasses="bg-gray-50 text-lg"
        />
        <Menu
          description="Export"
          items={[
            { text: 'As JSON', onClick: handleJsonExport },
            { text: 'As PDF', onClick: handlePdfExport }
          ]}
        />
      </div>
      {#each $report.blocks as block}
        <div class="py-10">
          {#if block.category == 'query'}
            <QueryBlock parameters={block.params} />
          {:else}
            <MarkdownBlock parameters={block.params} />
          {/if}
        </div>
      {/each}
      <div class="py-3">
        <Menu
          description="Add Block"
          items={[
            { text: 'Query', onClick: () => addBlock('query') },
            { text: 'Markdown', onClick: () => addBlock('markdown') }
          ]}
        />
      </div>
    </div>
  </div>
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
