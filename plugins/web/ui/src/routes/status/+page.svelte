<script lang="ts">
  import { scaleBand } from 'd3-scale';

  import { useQuery } from '@sveltestack/svelte-query';

  import Table from '$lib/components/Table.svelte';
  import Expandable from '$lib/components/Expandable.svelte';

  import { formatBytes } from '$lib/util/formatBytes';
  import Tooltip from '$lib/components/Tooltip.svelte';

  import { LayerCake, Svg } from 'layercake';
  import Scatter from '$lib/components/Scatter.svelte';

  import Bar from '$lib/components/Bar.svelte';
  import DetailView from '$lib/components/DetailView.svelte';
  import AxisX from '$lib/components/AxisX.svelte';
  import AxisY from '$lib/components/AxisY.svelte';

  const xKey = 'count';
  const yKey = 'layout';

  let detailedSchema = "";

  const dtFormat = new Intl.DateTimeFormat('en-US', {
    timeStyle: 'medium',
    timeZone: 'UTC'
  });

  const getStatus = async () => {
    try {
      // if we have a VAST_API_ENDPOINT .env variable available
      // use that, otherwise construct from the current base url
      // this assumes that the /api/v0 endpoint is available
      // under the current BASE_URL of the frontend deployment
      const API_BASE =
        import.meta.env.VITE_VAST_API_ENDPOINT ?? `${import.meta.env.BASE_URL}api/v0`;
      const url = `${API_BASE}/status?verbosity=debug`;

      const response = await fetch(url);

      const data = await response.json();

      return { ...data };
    } catch (error) {
      console.error(`Error in getStatus function : ${error}`);
    }
  };

  let pluginColumns = [
    { header: 'Plugin', accessor: 'name' },
    { header: 'Version', accessor: 'version' }
  ];

  interface Events {
    [index: string]: { count: number; percentage: number };
  }

  const getPluginRows = (plugins) =>
    Object.keys(plugins).map((key) => ({ name: key, version: plugins[key] }));

  let eventColumns = [
    { header: 'Layout', accessor: 'layout' },
    { header: 'Count', accessor: 'count' },
    { header: 'Percentage', accessor: 'percentage' }
  ];

  const getEventsRows = (events: Events) => {
    return Object.keys(events).map((key) => ({
      layout: key,
      count: events[key].count,
      percentage: new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 }).format(
        events[key].percentage
      )
    }));
  };

  const getHistogram = (catalogStatus, schema) => {
    let result = [];
    let schemas = catalogStatus["schemas"];
    let bucket_width = "5 days";
    for (let i in schemas) {
      let obj = schemas[i];
      if (obj["name"] != schema)
        continue;
      let partitions = obj["partitions"];
      for (let j in partitions) {
        let partition = partitions[j];
        let age = Date.now() - Date.parse(partition["import-time"]["min"]);
        let days = Math.floor(age / 1000 / 60 / 60 / 24);
        result.push({"x": days, "y": partition["num-events"]});
      }
    }
    return result;
  };

  const queryResult = useQuery('status', getStatus, { refetchInterval: 5000 });
</script>

<style>
  /*
    The wrapper div needs to have an explicit width and height in CSS.
    It can also be a flexbox child or CSS grid element.
    The point being it needs dimensions since the <LayerCake> element will
    expand to fill it.
  */
  .chart-container {
    width: 100%;
    height: 300px;
  }
</style>

<svelte:head>
  <title>VAST Status</title>
  <meta name="description" content="Status page for VAST" />
</svelte:head>

<div class="p-2 m-2 text-sm text-left text-gray-600">
  <div class="flex justify-between my-4 mr-4">
    <div class="text-xl font-bold">Status</div>
    <Tooltip>
      <div class="" slot="main">
        {$queryResult.data ? '🟢 Running' : '🔴 Not Running'}
      </div>
      <div class="panel-contents" slot="popover">
        Last checked {(Date.now() - $queryResult.dataUpdatedAt) / 1000} seconds ago.
      </div>
    </Tooltip>
  </div>

  {#if $queryResult.isSuccess}
    <div class="my-4">
      <div>
        Last updated: {dtFormat.format(new Date($queryResult.dataUpdatedAt))}
      </div>
      <div>
        DB Path: {$queryResult.data?.system['database-path']}
      </div>
      <div>
        Memory Usage: {formatBytes($queryResult.data?.system['current-memory-usage'])}
      </div>
      <div>
        VAST version: {$queryResult.data?.version.VAST}
      </div>
    </div>

    {#if $queryResult.data?.version.plugins}
      <Expandable summary="Plugins">
        <div class="py-6 text-left md:w-1/5">
          <Table
            tableRows={$queryResult?.data && getPluginRows($queryResult.data?.version.plugins)}
            columnDetails={pluginColumns}
          />
        </div>
      </Expandable>
    {/if}

    <div class="py-6 text-left md:w-1/4">
      {#if $queryResult.data?.index.statistics.layouts}
        <Table
          tableRows={getEventsRows($queryResult.data?.index.statistics.layouts)}
          columnDetails={eventColumns}
        />
      {:else}
        Database is empty.
      {/if}
    </div>
    {#if $queryResult.data?.index.statistics.layouts}
    <h2>Event Distribution</h2>
    <div class="chart-container">
      <LayerCake
        padding={{ top: 0, bottom: 100, left: 120 }}
        x={xKey}
        y={yKey}
        yScale={scaleBand().paddingInner([0.55])}
        xDomain={[0, null]}
        data={Object.values(getEventsRows($queryResult.data?.index.statistics.layouts))}
      >
        <Svg>
          <AxisY
            gridlines={false}
          />
          <Bar
            callback="{(schema) => detailedSchema = (detailedSchema != schema) ? schema : '' }"
          />
        </Svg>
      </LayerCake>
    </div>
    {/if}
  {/if}

  {#if detailedSchema != ""}
    <div class="chart-container">
      <LayerCake
        padding={{ top: 0, bottom: 100, left: 120 }}
        x='x'
        y='y'
        yScale={scaleBand().paddingInner([0.55])}
        xDomain={[0, null]}
        data={getHistogram($queryResult.data?.catalog, detailedSchema)}
      >
        <Svg>
          <Bar
            horizontal={false}
          />
          <AxisX
            gridlines={false}
            tickMarks={false}
          />
        </Svg>
      </LayerCake>
    </div>
  {/if}
</div>
