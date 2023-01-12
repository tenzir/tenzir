<script lang="ts">
  export let onClick: () => void;
  export let iconifyString: string; // in the form of i-<collection>-<icon>

  export let classNames =
    'justify-center p-2 text-sm font-medium leading-5 text-gray-700 transition duration-150 ease-in-out bg-white border-0 border-gray-300 rounded-md hover:text-gray-500 hover:bg-gray-100 focus:outline-none focus:border-blue-300 focus:shadow-outline-blue active:bg-gray-50 active:text-gray-800';
</script>

<div>
  <button on:click={onClick} class={classNames}>
    <div class={iconifyString} />
  </button>
</div>
