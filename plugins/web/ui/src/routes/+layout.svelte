<script lang="ts">
  import 'uno.css';
  import './styles.css';
  import { QueryClient, QueryClientProvider } from '@sveltestack/svelte-query';
  const client = new QueryClient();
</script>

<div class="flex">
  <div class="sticky top-0 h-screen" id="nav-sidebar">
    <nav class="flex flex-col items-center h-100% w-12 bg-gray-200 dark:bg-gray-800 z-50">
      <!-- /vast.svg is processed from static/vast.svg during build-->
      <img class="my-4" src="/vast.svg" alt="" />
      <div class="rounded bg-gray-300 hover:border border-gray-400 my-1">
        <a href="/status">
          <button class="i-pajamas-status-health m-2 hover:border hover:rounded w-16px" />
        </a>
      </div>
      <div class="rounded bg-gray-300 hover:border border-gray-400 my-1">
        <a href="/report">
          <button class="i-bxs:report m-2 hover:border hover:rounded w-16px" />
        </a>
      </div>
    </nav>
  </div>

  <div id="detail" class="flex flex-col overflow-y-auto w-full">
    <QueryClientProvider {client}>
      <slot />
    </QueryClientProvider>
  </div>
</div>
