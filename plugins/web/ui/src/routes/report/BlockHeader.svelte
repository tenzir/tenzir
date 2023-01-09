<script lang="ts">
  import IconButton from '$lib/components/IconButton.svelte';
  import InlineInput from '$lib/components/InlineInput.svelte';

  export let title: string;
  export let onEdit: () => void;
  export let onDelete: () => void;

  let hasSectionTitle = false;

  const toglleSectionTitle = () => {
    hasSectionTitle = !hasSectionTitle;
    title = ''; // reset title
  };
</script>

<div>
  <div class="flex justify-between">
    {#if hasSectionTitle}
      <InlineInput placeholder="Untitled section" bind:value={title}
        labelClasses="text-l hover:bg-slate-200 px-2 py-1 rounded"

      />
    {:else}
      <!-- Empty element so that justify-between works as expected -->
      <div />
    {/if}
    <div class="flex">
      <IconButton onClick={toglleSectionTitle} iconifyString="i-material-symbols:title" />
      <IconButton onClick={onEdit} iconifyString="i-material-symbols:edit-outline-rounded" />
      <IconButton onClick={onDelete} iconifyString="i-ic:baseline-delete-forever" />
    </div>
  </div>
</div>
