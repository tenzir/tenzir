<script>
  // @ts-nocheck

  import { Html, LayerCake, Svg } from 'layercake';
  import { scaleOrdinal, scaleLog } from 'd3-scale';
  import { timeParse, timeFormat } from 'd3-time-format';
  import { format, precisionFixed } from 'd3-format';

  import MultiLine from '../_components/MultiLine.svelte';
  import AxisX from '../_components/AxisX.svelte';
  import AxisY from '../_components/AxisY.svelte';
  import Key from '../_components/Key.html.svelte';

  /* --------------------------------------------
   * Set what is our x key to separate it from the other series
   */
  const xKey = 'day';
  const yKey = 'value';
  const zKey = 'type';
  export let data;

  const seriesNames = Object.keys(data[0]).filter((d) => d !== xKey);
  const seriesColors = ['#11e4b8', '#ff00cc'];

  const parseDate = timeParse('%Y-%m-%d');

  /* --------------------------------------------
   * Create a "long" format that is a grouped series of data points
   * Layer Cake uses this data structure and the key names
   * set in xKey, yKey and zKey to map your data into each scale.
   */
  const dataLong = seriesNames.map((key) => {
    return {
      [zKey]: key,
      values: data.map((d) => {
        d[xKey] = typeof d[xKey] === 'string' ? parseDate(d[xKey]) : d[xKey]; // Conditional required for sapper
        return {
          [yKey]: +d[key],
          [xKey]: d[xKey],
          [zKey]: key
        };
      })
    };
  });

  // nice looking d3 log scale with automatic
  // domain and range
  const yScale = scaleLog().base(10).nice();

  /* --------------------------------------------
   * Make a flat array of the `values` of our nested series
   * we can pluck the field set from `yKey` from each item
   * in the array to measure the full extents
   */
  const flatten = (data) =>
    data.reduce((memo, group) => {
      return memo.concat(group.values);
    }, []);

  const formatTickX = timeFormat('%b. %e');
  const formatTickY = (d) => format(`.${precisionFixed(d)}s`)(d);
</script>

<div class="w-600px h-400px border border-slate-300 rounded p-20">
  <LayerCake
    padding={{ top: 7, right: 10, bottom: 20, left: 25 }}
    x={xKey}
    y={yKey}
    z={zKey}
    {yScale}
    zScale={scaleOrdinal()}
    zRange={seriesColors}
    flatData={flatten(dataLong)}
    data={dataLong}
  >
    <Svg>
      <AxisX
        gridlines={false}
        ticks={data.map((d) => d[xKey]).sort((a, b) => a - b)}
        formatTick={formatTickX}
        snapTicks={true}
        tickMarks={true}
        yTick={50}
      />

      <AxisY formatTick={formatTickY} xTick={-35} ticks={2} />
      <MultiLine />
    </Svg>
    <Html>
      <Key shape="circle" />
    </Html>
  </LayerCake>
</div>
