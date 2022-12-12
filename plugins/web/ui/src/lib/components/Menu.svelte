<script lang="ts">
  import { Menu, MenuButton, MenuItems, MenuItem, Transition } from '@rgossiaux/svelte-headlessui';

  function classNames(...classes: (string | false | null | undefined)[]) {
    return classes.filter(Boolean).join(' ');
  }
  function resolveClass({ active, disabled }: { active: boolean; disabled: boolean }) {
    return classNames(
      'flex justify-between px-4 py-2 text-sm leading-5 text-left',
      active ? 'bg-gray-100 text-gray-900' : 'text-gray-700',
      disabled && 'cursor-not-allowed opacity-50'
    );
  }

  interface Item {
    text: string;
    onClick: () => void;
  }
  export let items: Item[];
  export let description: string;
</script>

<div class="flex">
  <div class="relative inline-block text-left">
    <Menu>
      <span class="rounded-md shadow-sm">
        <MenuButton
          class="inline-flex justify-center w-full px-4 py-2 text-sm font-medium leading-5 text-gray-700 transition duration-150 ease-in-out bg-white border border-gray-300 rounded-md hover:text-gray-500 hover:bg-gray-100 focus:outline-none focus:border-blue-300 focus:shadow-outline-blue active:bg-gray-50 active:text-gray-800"
        >
          <span>{description}</span>
          <svg class="w-5 h-5 ml-2 -mr-1" viewBox="0 0 20 20" fill="currentColor">
            <path
              fill-rule="evenodd"
              d="M5.293 7.293a1 1 0 011.414 0L10 10.586l3.293-3.293a1 1 0 111.414 1.414l-4 4a1 1 0 01-1.414 0l-4-4a1 1 0 010-1.414z"
              clip-rule="evenodd"
            />
          </svg>
        </MenuButton>
      </span>

      <Transition
        enter="transition duration-100 ease-out"
        enterFrom="transform scale-95 opacity-0"
        enterTo="transform scale-100 opacity-100"
        leave="transition duration-100 ease-out"
        leaveFrom="transform scale-100 opacity-100"
        leaveTo="transform scale-95 opacity-0"
      >
        <MenuItems
          class="absolute mt-2 origin-top-right bg-white border border-gray-200 divide-y divide-gray-100 rounded-md shadow-lg outline-none"
        >
          <div class="py-1">
            {#each items as item}
              <MenuItem class={resolveClass} on:click={item.onClick}>
                {item.text}
              </MenuItem>
            {/each}
          </div></MenuItems
        >
      </Transition>
    </Menu>
  </div>
</div>
