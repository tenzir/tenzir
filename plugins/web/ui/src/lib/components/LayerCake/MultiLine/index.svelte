<script>
  // @ts-nocheck

  import { getQuery } from '$lib/util/api';
  import DatePicker from '$lib/components/DatePicker.svelte';
  import Button from '$lib/components/Button.svelte';
  import { groupZeekConnectionsByDay } from '$lib/util/misc';

  import Bytes from './Bytes.svelte';

  let startDate = new Date('2012-01-01');
  let stopDate = new Date('2022-01-01');

  let data;
  const handleRun = async () => {
    const queryResult = await getQuery(
      `#type == "zeek.conn" && id.orig_h in 192.168.0.0/16 && :timestamp >= ${startDate
        .toISOString()
        .substring(0, 10)} && :timestamp <= ${stopDate.toISOString().substring(0, 10)}`,
      1000000
    ); // TODO manage limit

    const groupedEvents = groupZeekConnectionsByDay(queryResult?.events);
    data = groupedEvents.map((d) => ({
      day: d.day,
      outbound: d.resp_bytes,
      inbound: d.orig_bytes
    }));
  };
</script>

<div>
  <div class="flex m-2">
    <div class="m-2">
      <DatePicker bind:date={startDate} placeholder="start date(yyyy-MM-dd)" />
    </div>
    <div class="m-2">
      <DatePicker bind:date={stopDate} placeholder="stop date(yyyy-MM-dd)" />
    </div>
    <div class="m-2">
      <Button onClick={handleRun}>Run</Button>
    </div>
  </div>
  {#if data}
    <Bytes {data} />
  {/if}
</div>
