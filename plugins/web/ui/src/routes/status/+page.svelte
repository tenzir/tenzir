<script lang="ts">
	import { useQuery } from '@sveltestack/svelte-query';

	import Table from '$lib/components/Table.svelte';
	import Expandable from '$lib/components/Expandable.svelte';

	import { formatBytes } from '$lib/util/formatBytes';
	import Tooltip from '$lib/components/Tooltip.svelte';

	const dtFormat = new Intl.DateTimeFormat('en-US', {
		timeStyle: 'medium',
		timeZone: 'UTC'
	});

	const getStatus = async () => {
		try {
			const API_BASE = 'http://localhost:42001/api/v0';
			const url = `${API_BASE}/status?verbosity=detailed`;
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

	const getPluginRows = (plugins) =>
		Object.keys(plugins).map((key) => ({ name: key, version: plugins[key] }));

	let eventColumns = [
		{ header: 'Layout', accessor: 'layout' },
		{ header: 'Count', accessor: 'count' },
		{ header: 'Percentage', accessor: 'percentage' }
	];

	const getEventsRows = (events) =>
		Object.keys(events).map((key) => ({
			layout: key,
			count: events[key].count,
			percentage: new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 }).format(
				parseFloat(events[key].percentage)
			)
		}));

	const queryResult = useQuery('status', getStatus, { refetchInterval: 5000 });
</script>

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
				Last chacked {(Date.now() - $queryResult.dataUpdatedAt) / 1000} seconds ago.
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

		<Expandable summary="Plugins">
			<div class="py-6 text-left md:w-1/5">
				<Table
					tableRows={$queryResult?.data && getPluginRows($queryResult.data?.version.plugins)}
					columnDetails={pluginColumns}
				/>
			</div>
		</Expandable>

		<div class="py-6 text-left md:w-1/4">
			<Table
				tableRows={$queryResult?.data && getEventsRows($queryResult.data?.index.statistics.layouts)}
				columnDetails={eventColumns}
			/>
		</div>
	{/if}
</div>
