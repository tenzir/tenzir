<script lang="ts">
  import IconButton from '$lib/components/IconButton.svelte';
  import InlineInput from '$lib/components/InlineInput.svelte';

  export let title: string;
  export let onClick: () => void;
  // export let onDelete: () => void;
</script>

<div>
  <div class="flex justify-between">
    <InlineInput placeholder="Untitled Section" bind:value={title} />
    <div class="flex">
      <IconButton {onClick} iconName="material-symbols:edit-outline-rounded" />

      <IconButton
        onClick={() => {
          console.log('Delete clicked');
        }}
        iconName="ic:baseline-delete-forever"
      />
    </div>
  </div>
</div>
