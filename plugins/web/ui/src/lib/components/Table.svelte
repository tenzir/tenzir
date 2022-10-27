<script lang="ts">
	import { Render, Subscribe, createTable } from 'svelte-headless-table';
	import { readable } from 'svelte/store';
	interface TableRow {
		[key: string]: string | number;
	}

	type ColumnInfo = { header: string; accessor: string };

	export let tableRows: TableRow[];
	export let columnDetails: ColumnInfo[];

	const table = createTable(readable(tableRows));

	const columns = table.createColumns(
		columnDetails.map((detail) =>
			table.column({ header: detail.header, accessor: detail.accessor })
		)
	);

	const { headerRows, rows, tableAttrs, tableBodyAttrs } = table.createViewModel(columns);
</script>

<div class="bg-gray-100 rounded p-2">
	<table {...$tableAttrs} class="table-auto text-sm text-left text-gray-500 w-full">
		<thead>
			{#each $headerRows as headerRow (headerRow.id)}
				<Subscribe rowAttrs={headerRow.attrs()} let:rowAttrs>
					<tr {...rowAttrs}>
						{#each headerRow.cells as cell (cell.id)}
							<Subscribe attrs={cell.attrs()} let:attrs>
								<th {...attrs}>
									<Render of={cell.render()} />
								</th>
							</Subscribe>
						{/each}
					</tr>
				</Subscribe>
			{/each}
		</thead>
		<tbody {...$tableBodyAttrs}>
			{#each $rows as row (row.id)}
				<Subscribe rowAttrs={row.attrs()} let:rowAttrs>
					<tr {...rowAttrs}>
						{#each row.cells as cell (cell.id)}
							<Subscribe attrs={cell.attrs()} let:attrs>
								<td {...attrs}>
									<Render of={cell.render()} />
								</td>
							</Subscribe>
						{/each}
					</tr>
				</Subscribe>
			{/each}
		</tbody>
	</table>
</div>
