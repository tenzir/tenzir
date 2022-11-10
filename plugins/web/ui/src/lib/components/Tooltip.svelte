<script>
  import { Popover, PopoverButton, PopoverPanel } from '@rgossiaux/svelte-headlessui';

  let popoverOpen = false;
</script>

<Popover style="position: relative;">
  <PopoverButton
    on:mouseenter={() => {
      popoverOpen = true;
    }}
    on:mouseleave={() => {
      popoverOpen = false;
    }}
    class="border rounded bg-white hover:bg-gray-100 align-middle h-8"
  >
    <slot name="main" />
  </PopoverButton>
  {#if popoverOpen}
    <PopoverPanel style="position: absolute; z-index: 10;" static>
      <slot name="popover" />
    </PopoverPanel>
  {/if}
</Popover>
