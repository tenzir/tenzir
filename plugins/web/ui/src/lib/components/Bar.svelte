<!--
	@component
	Generates an SVG bar chart.
 -->
<script>
	import { getContext } from 'svelte';

	const { data, xGet, yGet, xScale, yScale } = getContext('LayerCake');

	/** @type {String} [fill='#00bbff'] – The shape's fill color. This is technically optional because it comes with a default value but you'll likely want to replace it with your own color. */
	export let fill = '#00bbff';

	export let callback = null;

	let highlight = -1;

	let color = function(i) {
		if (i == highlight)
			return '#ff00bb';
		else
			return fill;
	}
</script>

<g class="bar-group">
	{#each $data as d, i}
		<rect
		    on:click="{() => {callback(d['layout']); highlight = i;} }"
			class='group-rect'
			data-id="{i}"
			x="{$xScale.range()[0]}"
			y="{$yGet(d)}"
			height={$yScale.bandwidth()}
			width="{$xGet(d)}"
			fill={fill}
		></rect>
	{/each}
</g>
