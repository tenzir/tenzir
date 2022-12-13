<script lang="ts">
  import InlineInput from 'svelte-inline-input';
  export let placeholder: string;
  export let value: string;
</script>

<div>
  <InlineInput bind:value bind:placeholder />
</div>
