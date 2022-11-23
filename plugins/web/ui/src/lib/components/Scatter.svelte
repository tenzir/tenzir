<!-- { filename: './components/Scatter.svelte' } -->
<script>
  // Import the getContext function from svelte
  import { getContext } from 'svelte';

  // Access the context using the 'LayerCake' keyword
  // Grab some helpful functions
  const { data, x, xScale, y, yScale } = getContext('LayerCake');

  export let fill = '#000';
  export let r = 5;
</script>

<g>
  {#each $data as d}
    <circle cx='{ $xScale($x(d)) }' cy='{ $yScale($y(d)) }' {fill} {r} />
  {/each}
</g>
