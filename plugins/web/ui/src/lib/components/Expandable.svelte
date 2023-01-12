<script lang="ts">
  import { Disclosure, DisclosureButton, DisclosurePanel } from '@rgossiaux/svelte-headlessui';
  export let summary: string;
</script>

<Disclosure let:open>
  <DisclosureButton class="bg-gray-200 border-0 rounded">
    <div class="inline-flex items-center">
      <div
        class="i-fluent:triangle-right-16-filled mr-2"
        style={open ? 'transform: rotate(90deg);' : ''}
      />
      {summary}
    </div>
  </DisclosureButton>

  <DisclosurePanel>
    <slot />
  </DisclosurePanel>
</Disclosure>
