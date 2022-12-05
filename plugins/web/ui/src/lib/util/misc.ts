type ConnMinimal = {
  ts: string; // TODO strong type it
  orig_bytes: number;
  resp_bytes: number;
};

export const groupZeekConnectionsByDay = (conns: ConnMinimal[]) => {
  return conns.reduce(
    (result: Array<{ day: string; orig_bytes: number; resp_bytes: number }>, conn: ConnMinimal) => {
      const day = conn.ts.split('T')[0];
      const existing = result.find((r) => r?.day === day);
      // ignore if null
      if (conn.orig_bytes && conn.resp_bytes) {
        if (existing) {
          existing.orig_bytes = existing.orig_bytes + conn.orig_bytes || 0;
          existing.resp_bytes = existing.resp_bytes + conn.resp_bytes || 0;
        } else {
          result.push({
            day,
            orig_bytes: conn.orig_bytes || 0,
            resp_bytes: conn.resp_bytes || 0
          });
        }
      }
      return result;
    },
    []
  );
};
