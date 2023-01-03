<script lang="ts">
  import InlineInput from 'svelte-inline-input';
  export let placeholder: string;
  export let value: string;
  export let inputClasses = '';
  export let labelClasses = '';
</script>

<div>
  <InlineInput bind:value bind:placeholder {inputClasses} {labelClasses} />
</div>
