<script lang="ts">
  import { Render, Subscribe, createTable } from 'svelte-headless-table';
  import { addSortBy } from 'svelte-headless-table/plugins';
  import { writable } from 'svelte/store';

  type TableRow = {
    [key: string]: string | number;
  };

  type ColumnInfo = { header: string; accessor: string };

  export let tableRows: TableRow[];
  export let columnDetails: ColumnInfo[];

  const table = createTable(writable(tableRows), {
    sort: addSortBy()
  });

  const columns = table.createColumns(
    columnDetails.map((detail) =>
      table.column({ header: detail.header, accessor: detail.accessor })
    )
  );

  const { headerRows, rows, tableAttrs, tableBodyAttrs } = table.createViewModel(columns);
  export let showHeaderBorder = false;
  export let showCellBorder = false;
</script>

<div class="bg-gray-100 rounded p-2 overflow-auto">
  <table {...$tableAttrs} class={`table-auto text-sm text-left text-gray-500 border-collapse m-2`}>
    <thead>
      {#each $headerRows as headerRow (headerRow.id)}
        <Subscribe rowAttrs={headerRow.attrs()} let:rowAttrs>
          <tr {...rowAttrs}>
            {#each headerRow.cells as cell (cell.id)}
              <Subscribe attrs={cell.attrs()} let:attrs props={cell.props()} let:props>
                <th
                  {...attrs}
                  on:click={props.sort.toggle}
                  class={showHeaderBorder ? 'border border-slate-300' : ''}
                >
                  <div class="flex items-center">
                    <Render of={cell.render()} />
                    {#if props.sort.order === 'asc'}
                      <div class="i-ic:outline-keyboard-arrow-down pl-2" />
                    {:else if props.sort.order === 'desc'}
                      <div class="i-ic:outline-keyboard-arrow-up pl-2" />
                    {/if}
                  </div>
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
                <td {...attrs} class={showCellBorder ? 'border border-slate-300' : ''}>
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

<style>
  th,
  td {
    padding: 8px;
  }
</style>
