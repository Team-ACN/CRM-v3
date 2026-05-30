export async function register() {
  // Fix broken localStorage shim in some Node.js environments
  if (
    typeof globalThis.localStorage !== 'undefined' &&
    typeof (globalThis as unknown as { localStorage: Storage }).localStorage.getItem !== 'function'
  ) {
    delete (globalThis as unknown as Record<string, unknown>).localStorage
  }
}
