<!--
  @component
  Generates an SVG multi-series line chart. It expects your data to be an array of objects, each with a `values` key that is an array of data objects.
 -->
<script>
  import { getContext } from 'svelte';

  const { data, xGet, yGet, zGet, xScale, yScale } = getContext('LayerCake');

  $: path = (values) => {
    console.log({ values })
    return (
      'M' +
      values
        .map((d) => {
          return $xGet(d) + ',' + $yGet(d);
        })
        .join('L')
    );
  };
</script>

<g class="line-group">
  {#each $data as group}
    <path class="path-line" d={path(group.values)} stroke={$zGet(group)} />
  {/each}
</g>
<g class="scatter-group">
  {#each $data as group}
    {#each group.values as d}
      <circle
        cx={$xGet(d) + ($xScale.bandwidth ? $xScale.bandwidth() / 2 : 0)}
        cy={$yGet(d) + ($yScale.bandwidth ? $yScale.bandwidth() / 2 : 0)}
        r={4}
        fill={$zGet(group)}
      />
    {/each}
  {/each}
</g>

<style>
  .path-line {
    fill: none;
    stroke-linejoin: round;
    stroke-linecap: round;
    stroke-width: 3px;
  }
</style>
