export async function postWithReconciliationRetry(url, onRetry = () => new Promise((resolve) => setTimeout(resolve, 1000))) {
  let response;
  do {
    response = await fetch(url, { method: "POST" });
    if (response.status === 503) {
      await response.text();
      await onRetry();
    }
  } while (response.status === 503);
  return response;
}
