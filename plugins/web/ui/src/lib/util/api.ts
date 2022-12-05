export const getQuery = async (expression: string, limit = 100) => {
  try {
    const API_BASE = import.meta.env.VITE_VAST_API_ENDPOINT ?? `${import.meta.env.BASE_URL}api/v0`;

    // const url = `${API_BASE}/export?expression=${encodeURIComponent('#type == "zeek.conn" && id.orig_h in 192.168.0.0/16')}`;
    const url = `${API_BASE}/export?expression=${encodeURIComponent(
      `${expression}`
    )}&limit=${limit}`;

    const response = await fetch(url);

    const data = await response.json();

    return { ...data };
  } catch (error) {
    console.error(`Error in getQuery function : ${error}`);
  }
};
