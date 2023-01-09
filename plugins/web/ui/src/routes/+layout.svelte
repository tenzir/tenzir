<script lang="ts">
  import 'uno.css';
  import './styles.css';
  import { QueryClient, QueryClientProvider } from '@sveltestack/svelte-query';
  import { goto } from '$app/navigation';
  const client = new QueryClient();
</script>

<div class="flex">
  <div class="sticky top-0 h-screen" id="nav-sidebar">
    <nav class="flex flex-col items-center h-100% w-12 bg-gray-200 dark:bg-gray-800 z-50">
      <!-- /vast.svg is processed from static/vast.svg during build-->
      <img class="mt-4 mb-6" src="/vast.svg" alt="VAST logo" />
      <button
        on:click={() => {
          goto('/status');
        }}
        class="mb-2 flex bg-gray-300 border-none border-gray-300 rounded hover:border border-gray-400 hover:cursor-pointer w-34px h-34px"
      >
        <div class="i-pajamas-status-health m-auto" />
      </button>

      <button
        on:click={() => {
          goto('/report');
        }}
        class="mb-2 flex bg-gray-300 border-none border-gray-300 rounded hover:border border-gray-400 hover:cursor-pointer w-34px h-34px"
      >
        <div class="i-bxs:report m-auto" />
      </button>
    </nav>
  </div>

  <div id="detail" class="flex flex-col overflow-y-auto w-full">
    <QueryClientProvider {client}>
      <slot />
    </QueryClientProvider>
  </div>
</div>
