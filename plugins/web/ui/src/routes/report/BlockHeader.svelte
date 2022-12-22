<script lang="ts">
  import IconButton from '$lib/components/IconButton.svelte';
  import InlineInput from '$lib/components/InlineInput.svelte';

  export let title: string;
  export let onEdit: () => void;
  export let onDelete: () => void;
</script>

<div>
  <div class="flex justify-between">
    <InlineInput placeholder="Untitled Section" bind:value={title} />
    <div class="flex">
      <IconButton onClick={onEdit} iconifyString="i-material-symbols:edit-outline-rounded" />
      <IconButton onClick={onDelete} iconifyString="i-ic:baseline-delete-forever" />
    </div>
  </div>
</div>
