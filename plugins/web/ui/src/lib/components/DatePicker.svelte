<script lang="ts">
  import { DateInput } from 'date-picker-svelte';
  export let date: null | Date = null; // the placeholder only works when it is null

  export let placeholder = 'yyyy-MM-dd';
</script>

<DateInput bind:value={date} format="yyyy-MM-dd" {placeholder} />
