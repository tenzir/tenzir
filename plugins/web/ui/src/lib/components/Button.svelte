<script lang="ts">
  export let onClick: () => void;
  export let classNames = 'flex items-center mx-1';
</script>

<div>
  <button on:click={onClick} class={classNames}>
    <slot />
  </button>
</div>
