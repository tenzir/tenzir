<script>
  import { LayerCake, Svg } from 'layercake';
  import { scaleOrdinal } from 'd3-scale';
  import { timeParse, timeFormat } from 'd3-time-format';
  import { format, precisionFixed } from 'd3-format';

  import MultiLine from './_components/MultiLine.svelte';
  import AxisX from './_components/AxisX.svelte';
  import AxisY from './_components/AxisY.svelte';
  import { data } from './data';

  /* --------------------------------------------
   * Set what is our x key to separate it from the other series
   */
  const xKey = 'month';
  const yKey = 'value';
  const zKey = 'type';

  const seriesNames = Object.keys(data[0]).filter((d) => d !== xKey);
  const seriesColors = ['#ffe4b8', '#ffb3c0', '#ff7ac7', '#ff00cc'];

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

<div class="w-800px h-600px my-10 mx-auto">
  <LayerCake
    padding={{ top: 7, right: 10, bottom: 20, left: 25 }}
    x={xKey}
    y={yKey}
    z={zKey}
    yDomain={[0, null]}
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
      />
      <AxisY ticks={4} formatTick={formatTickY} />
      <MultiLine />
    </Svg>
  </LayerCake>
</div>
